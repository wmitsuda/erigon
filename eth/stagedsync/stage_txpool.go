package stagedsync

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func spawnTxPool(s *StageState, db ethdb.GetterPutter, pool *core.TxPool, poolStart func() error, quitCh <-chan struct{}) error {
	to, err := s.ExecutionAt(db)
	if err != nil {
		return err
	}
	logPrefix := s.state.LogPrefix()
	if to < s.BlockNumber {
		return fmt.Errorf("%s: to (%d) < from (%d)", logPrefix, to, s.BlockNumber)
	}
	if pool != nil && !pool.IsStarted() {
		log.Info(fmt.Sprintf("[%s] Starting tx pool after sync", logPrefix), "from", s.BlockNumber, "to", to)
		headHash, err := rawdb.ReadCanonicalHash(db, to)
		if err != nil {
			return err
		}
		headHeader := rawdb.ReadHeader(db, headHash, to)
		if err := pool.Start(headHeader.GasLimit, to); err != nil {
			return fmt.Errorf("%s: start pool phase 1: %w", logPrefix, err)
		}
		if err := poolStart(); err != nil {
			return fmt.Errorf("%s: start pool phase 2: %w", logPrefix, err)
		}
	}
	if pool != nil && pool.IsStarted() && s.BlockNumber > 0 {
		if err := incrementalTxPoolUpdate(logPrefix, s.BlockNumber, to, pool, db, quitCh); err != nil {
			return err
		}
		pending, queued := pool.Stats()
		log.Info(fmt.Sprintf("[%s] Transaction stats", logPrefix), "pending", pending, "queued", queued)
	}
	return s.DoneAndUpdate(db, to)
}

func incrementalTxPoolUpdate(logPrefix string, from, to uint64, pool *core.TxPool, db ethdb.Getter, quitCh <-chan struct{}) error {
	headHash, err := rawdb.ReadCanonicalHash(db, to)
	if err != nil {
		return err
	}

	headHeader := rawdb.ReadHeader(db, headHash, to)
	pool.ResetHead(headHeader.GasLimit, to)
	canonical := make([]common.Hash, to-from)

	log.Info(fmt.Sprintf("[%s] Read canonical hashes", logPrefix), "hashes", len(canonical))
	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber > to {
			return false, nil
		}

		bodyRlp, err := rawdb.DecompressBlockBody(v)
		if err != nil {
			return false, err
		}

		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(bodyRlp), body); err != nil {
			return false, fmt.Errorf("%s: invalid block body RLP: %w", logPrefix, err)
		}
		for _, tx := range body.Transactions {
			pool.RemoveTx(tx.Hash(), true /* outofbound */)
		}
		return true, nil
	}); err != nil {
		log.Error(fmt.Sprintf("[%s] walking over the block bodies", logPrefix), "error", err)
		return err
	}
	return nil
}

func unwindTxPool(u *UnwindState, s *StageState, db ethdb.GetterPutter, pool *core.TxPool, quitCh <-chan struct{}) error {
	if u.UnwindPoint >= s.BlockNumber {
		s.Done()
		return nil
	}
	logPrefix := s.state.LogPrefix()
	if pool != nil && pool.IsStarted() {
		if err := unwindTxPoolUpdate(logPrefix, u.UnwindPoint, s.BlockNumber, pool, db, quitCh); err != nil {
			return err
		}
		pending, queued := pool.Stats()
		log.Info(fmt.Sprintf("[%s] Transaction stats", logPrefix), "pending", pending, "queued", queued)
	}
	if err := u.Done(db); err != nil {
		return fmt.Errorf("%s: reset: %w", logPrefix, err)
	}
	return nil
}

func unwindTxPoolUpdate(logPrefix string, from, to uint64, pool *core.TxPool, db ethdb.Getter, quitCh <-chan struct{}) error {
	headHash, err := rawdb.ReadCanonicalHash(db, from)
	if err != nil {
		return err
	}
	headHeader := rawdb.ReadHeader(db, headHash, from)
	pool.ResetHead(headHeader.GasLimit, from)

	log.Info(fmt.Sprintf("[%s] Read canonical hashes", logPrefix))
	senders := make([][]common.Address, to-from+1)
	if err := db.Walk(dbutils.Senders, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber > to {
			return false, nil
		}

		sendersArray := make([]common.Address, len(v)/common.AddressLength)
		for i := 0; i < len(sendersArray); i++ {
			copy(sendersArray[i][:], v[i*common.AddressLength:])
		}
		senders[blockNumber-from-1] = sendersArray
		return true, nil
	}); err != nil {
		log.Error(fmt.Sprintf("[%s] TxPoolUpdate: walking over sender", logPrefix), "error", err)
		return err
	}
	var txsToInject []*types.Transaction
	if err := db.Walk(dbutils.BlockBodyPrefix, dbutils.EncodeBlockNumber(from+1), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(quitCh); err != nil {
			return false, err
		}

		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber > to {
			return false, nil
		}

		bodyRlp, err := rawdb.DecompressBlockBody(v)
		if err != nil {
			return false, err
		}

		body := new(types.Body)
		if err := rlp.Decode(bytes.NewReader(bodyRlp), body); err != nil {
			return false, fmt.Errorf("%s: invalid block body RLP: %w", logPrefix, err)
		}
		body.SendersToTxs(senders[blockNumber-from-1])
		txsToInject = append(txsToInject, body.Transactions...)
		return true, nil
	}); err != nil {
		log.Error(fmt.Sprintf("[%s]: walking over the block bodies", logPrefix), "error", err)
		return err
	}
	//nolint:errcheck
	log.Info(fmt.Sprintf("[%s] Injecting txs into the pool", logPrefix), "number", len(txsToInject))
	pool.AddRemotesSync(txsToInject)
	log.Info(fmt.Sprintf("[%s] Injection complete", logPrefix))
	return nil
}
