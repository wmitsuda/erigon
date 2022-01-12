package commands

import (
	"bytes"
	"encoding/binary"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

// Bootstrap a function able to locate a series of byte chunks containing
// related block numbers, starting from a specific block number (greater or equal than).
type ChunkLocator func(block uint64) (chunkProvider ChunkProvider, ok bool, err error)

// Allows to iterate over a set of byte chunks.
//
// If err is not nil, it indicates an error and the other returned values should be
// ignored.
//
// If err is nil and ok is true, the returned chunk should contain the raw chunk data.
//
// If err is nil and ok is false, it indicates that there is no more data. Subsequent calls
// to the same function should return (nil, false, nil).
type ChunkProvider func() (chunk []byte, ok bool, err error)

type BlockProvider func() (nextBlock uint64, hasMore bool, err error)

// This ChunkLocator searches over a cursor with a key format of [common.Address, block uint64],
// where block is the first block number contained in the chunk value.
//
// It positions the cursor on the chunk that contains the first block >= minBlock.
func NewForwardChunkLocator(cursor kv.Cursor, addr common.Address, minBlock uint64) ChunkLocator {
	return func(block uint64) (ChunkProvider, bool, error) {
		search := make([]byte, common.AddressLength+8)
		copy(search[:common.AddressLength], addr.Bytes())
		binary.BigEndian.PutUint64(search[common.AddressLength:], minBlock)

		k, _, err := cursor.Seek(search)
		if err != nil {
			return nil, false, err
		}

		// Exact match?
		if bytes.Equal(k, search) {
			return newForwardChunkProvider(cursor, addr, minBlock), true, nil
		}

		// It maybe the previous chunk
		kp, _, err := cursor.Prev()
		if err != nil {
			return nil, false, err
		}
		if !bytes.HasPrefix(kp, addr.Bytes()) {
			// It is in the current chunk
			_, _, err = cursor.Next()
			if err != nil {
				return nil, false, err
			}
			return newForwardChunkProvider(cursor, addr, minBlock), true, nil
		}

		// It is in the previous chunk
		return newForwardChunkProvider(cursor, addr, minBlock), true, nil
	}
}

// This ChunkProvider is built by NewForwardChunkLocator and advances the cursor forward until
// there is no more chunks for the desired addr.
func newForwardChunkProvider(cursor kv.Cursor, addr common.Address, minBlock uint64) ChunkProvider {
	first := true
	var err error
	eof := false
	return func() ([]byte, bool, error) {
		if err != nil {
			return nil, false, err
		}
		if eof {
			return nil, false, nil
		}

		var k, v []byte
		if first {
			first = false
			k, v, err = cursor.Current()
		} else {
			k, v, err = cursor.Next()
		}

		if err != nil {
			eof = true
			return nil, false, err
		}
		if !bytes.HasPrefix(k, addr.Bytes()) {
			eof = true
			return nil, false, nil
		}
		return v, true, nil
	}
}

// Given a ChunkLocator, moves forward over the chunks and inside each chunk, moves
// forward over the block numbers.
func NewForwardBlockProvider(chunkLocator ChunkLocator, block uint64) BlockProvider {
	var iter roaring64.IntPeekable64
	var chunkProvider ChunkProvider

	return func() (uint64, bool, error) {
		if chunkProvider == nil {
			var ok bool
			var err error
			chunkProvider, ok, err = chunkLocator(block)
			if err != nil {
				return 0, false, err
			}
			if !ok {
				return 0, false, nil
			}
			if chunkProvider == nil {
				return 0, false, nil
			}
		}

		if iter == nil {
			chunk, ok, err := chunkProvider()
			if err != nil {
				return 0, false, err
			}
			if !ok {
				return 0, false, nil
			}

			bm := roaring64.NewBitmap()
			if _, err := bm.ReadFrom(bytes.NewReader(chunk)); err != nil {
				return 0, false, err
			}
			iter = bm.Iterator()

			// It can happen that on the first chunk we'll get a chunk that contains
			// the first block >= minBlock in the middle of the chunk/bitmap, so we
			// skip all previous blocks before it.
			iter.AdvanceIfNeeded(block)
		}

		nextBlock := iter.Next()
		hasNext := iter.HasNext()
		if !hasNext {
			// Check if there is another chunk to get blocks from
			chunk, ok, err := chunkProvider()
			if err != nil {
				return 0, false, err
			}
			if ok {
				hasNext = true

				bm := roaring64.NewBitmap()
				if _, err := bm.ReadFrom(bytes.NewReader(chunk)); err != nil {
					return 0, false, err
				}
				iter = bm.Iterator()
			}
		}

		return nextBlock, hasNext, nil
	}
}
