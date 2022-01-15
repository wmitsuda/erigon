package commands

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

// This ChunkLocator searches over a cursor with a key format of [common.Address, block uint64],
// where block is the first block number contained in the chunk value.
//
// It positions the cursor on the chunk that contains the last block <= maxBlock.
func newBackwardChunkLocator(cursor kv.Cursor, addr common.Address) ChunkLocator {
	return func(block uint64) (ChunkProvider, bool, error) {
		searchKey := callIndexKey(addr, block)
		k, _, err := cursor.Seek(searchKey)
		if err != nil {
			return nil, false, err
		}

		// If the addr prefix is different it means there is not even the last
		// chunk (0xffff...), so this address has no call index
		if !bytes.HasPrefix(k, addr.Bytes()) {
			return nil, false, nil
		}

		// Exact match?
		if bytes.Equal(k, searchKey) {
			return newBackwardChunkProvider(cursor, addr, block), true, nil
		}

		// If we reached the last addr's chunk (0xffff...), it may contain desired blocks
		lastAddrKey := callIndexKey(addr, MaxBlockNum)
		if bytes.Equal(k, lastAddrKey) {
			return newBackwardChunkProvider(cursor, addr, block), true, nil
		}

		// It maybe the previous chunk; position it over the previous, but let the prefix to be
		// checked in the ChunkProvider (peek + prefix check)
		_, _, err = cursor.Prev()
		if err != nil {
			return nil, false, err
		}
		return newBackwardChunkProvider(cursor, addr, block), true, nil
	}
}

// This ChunkProvider is built by NewBackwardChunkLocator and advances the cursor backwards until
// there is no more chunks for the desired addr.
func newBackwardChunkProvider(cursor kv.Cursor, addr common.Address, minBlock uint64) ChunkProvider {
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
			k, v, err = cursor.Prev()
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

// Given a ChunkLocator, moves back over the chunks and inside each chunk, moves
// backwards over the block numbers.
func NewBackwardBlockProvider(chunkLocator ChunkLocator, block uint64) BlockProvider {
	// block == 0 means no max
	if block == 0 {
		block = MaxBlockNum
	}
	var iter roaring64.IntIterable64
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

			// It can happen that on the first chunk we'll get a chunk that contains
			// the last block <= maxBlock in the middle of the chunk/bitmap, so we
			// remove all blocks after it (since there is no AdvanceIfNeeded() in
			// IntIterable64)
			if block != MaxBlockNum {
				bm.RemoveRange(block+1, MaxBlockNum)
			}
			iter = bm.ReverseIterator()
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
				iter = bm.ReverseIterator()
			}
		}

		return nextBlock, hasNext, nil
	}
}

func NewCallCursorBackwardBlockProvider(cursor kv.Cursor, addr common.Address, maxBlock uint64) BlockProvider {
	chunkLocator := newBackwardChunkLocator(cursor, addr)
	return NewBackwardBlockProvider(chunkLocator, maxBlock)
}
