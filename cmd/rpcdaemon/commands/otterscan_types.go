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
			return NewForwardChunkProvider(cursor, addr, minBlock), true, nil
		}

		// It maybe the previous chunk
		kp, _, err := cursor.Prev()
		if err != nil {
			return nil, false, err
		}
		if !bytes.Equal(kp[:common.AddressLength], addr.Bytes()) {
			// It is in the current chunk
			_, _, err = cursor.Next()
			if err != nil {
				return nil, false, err
			}
			return NewForwardChunkProvider(cursor, addr, minBlock), true, nil
		}

		// It is in the previous chunk
		return NewForwardChunkProvider(cursor, addr, minBlock), true, nil
	}
}

func NewForwardChunkProvider(cursor kv.Cursor, addr common.Address, minBlock uint64) ChunkProvider {
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
		if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
			eof = true
			return nil, false, nil
		}
		return v, true, nil
	}
}

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
		}

		nextBlock := iter.Next()
		hasNext := iter.HasNext()
		for {
			if nextBlock >= block || !hasNext {
				break
			}
			nextBlock = iter.Next()
			hasNext = iter.HasNext()
		}

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
