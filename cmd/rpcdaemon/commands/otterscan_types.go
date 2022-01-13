package commands

import (
	"encoding/binary"

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

func callIndexKey(addr common.Address, block uint64) []byte {
	key := make([]byte, common.AddressLength+8)
	copy(key[:common.AddressLength], addr.Bytes())
	binary.BigEndian.PutUint64(key[common.AddressLength:], block)
	return key
}

const MaxBlockNum = ^uint64(0)
