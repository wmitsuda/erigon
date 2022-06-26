package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type OtsApprovalsIndexCfg struct {
	db          kv.RwDB
	chainConfig *params.ChainConfig
	blockReader services.FullBlockReader
	isEnabled   bool
}

func StageOtsApprovalsIndexCfg(db kv.RwDB, chainConfig *params.ChainConfig, blockReader services.FullBlockReader, isEnabled bool) OtsApprovalsIndexCfg {
	return OtsApprovalsIndexCfg{
		db:          db,
		chainConfig: chainConfig,
		blockReader: blockReader,
		isEnabled:   isEnabled,
	}
}

func SpawnStageOtsApprovalsIndex(cfg OtsApprovalsIndexCfg, s *StageState, tx kv.RwTx, ctx context.Context) error {
	if !cfg.isEnabled {
		return nil
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	bodiesProgress, err := stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return fmt.Errorf("getting bodies progress: %w", err)
	}

	// start/end block are inclusive
	startBlock := s.BlockNumber + 1
	endBlock := bodiesProgress
	if startBlock > endBlock {
		return nil
	}

	// Log timer
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	// minerIdx, err := tx.RwCursor(kv.OtsMinerIndex)
	// if err != nil {
	// 	return err
	// }
	// defer minerIdx.Close()

	stopped := false
	currentBlock := startBlock
	for ; currentBlock <= endBlock && !stopped; currentBlock++ {
		_, err := cfg.blockReader.HeaderByNumber(ctx, tx, currentBlock)
		if err != nil {
			return err
		}

		// 	chunkKey, m, err := searchLastChunk(s, currentBlock, minerIdx, header.Coinbase)
		// 	if err != nil {
		// 		return err
		// 	}

		// 	if err := addBlock2Chunk(m, currentBlock, minerIdx, header.Coinbase, s, chunkKey); err != nil {
		// 		return err
		// 	}

		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Indexing approvals", s.LogPrefix()),
				"block", currentBlock)
		default:
		}

	}

	if currentBlock > endBlock {
		currentBlock--
	}
	if err := s.Update(tx, currentBlock); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// func searchLastChunk(s *StageState, currentBlock uint64, minerIdx kv.Cursor, addr common.Address) ([]byte, *roaring64.Bitmap, error) {
// 	chunkKey := dbutils.MinerIdxKey(addr)
// 	k, v, err := minerIdx.SeekExact(chunkKey)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	m := roaring64.New()
// 	if k != nil {
// 		reader := bytes.NewReader(v)
// 		_, err := m.ReadFrom(reader)
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 		// log.Info(fmt.Sprintf("[%s] Existing miner", s.LogPrefix()), "blockNumber", currentBlock, "chunkKey", hexutil.Bytes(chunkKey), "min", m.Minimum(), "max", m.Maximum(), "count", m.GetCardinality(), "size", m.GetSerializedSizeInBytes())
// 	} else {
// 		// log.Info(fmt.Sprintf("[%s] New miner", s.LogPrefix()), "blockNumber", currentBlock, "chunkKey", hexutil.Bytes(chunkKey))
// 	}

// 	return chunkKey, m, nil
// }

const MaxChunkSize = 4096

// func addBlock2Chunk(m *roaring64.Bitmap, currentBlockNumber uint64, minerIdx kv.RwCursor, addr common.Address, s *StageState, chunkKey []byte) error {
// 	m.Add(currentBlockNumber)
// 	if m.GetSerializedSizeInBytes() > MaxChunkSize {
// 		prev := bitmapdb.CutLeft64(m, MaxChunkSize)

// 		// log.Info(fmt.Sprintf("[%s] Breaking up", s.LogPrefix()), "blockNumber", currentBlockNumber)
// 		buf := bytes.NewBuffer(nil)
// 		if _, err := prev.WriteTo(buf); err != nil {
// 			return err
// 		}
// 		prevChunkKey := dbutils.MinerIdxPrevKey(addr, prev.Maximum())
// 		if err := minerIdx.Put(prevChunkKey, buf.Bytes()); err != nil {
// 			return err
// 		}
// 		// log.Info(fmt.Sprintf("[%s] Prev chunk", s.LogPrefix()), "blockNumber", currentBlockNumber, "chunkKey", hexutil.Bytes(prevChunkKey), "min", lft.Minimum(), "max", lft.Maximum(), "count", lft.GetCardinality(), "size", lft.GetSerializedSizeInBytes())
// 	}

// 	buf := bytes.NewBuffer(nil)
// 	if _, err := m.WriteTo(buf); err != nil {
// 		return err
// 	}
// 	if err := minerIdx.Put(chunkKey, buf.Bytes()); err != nil {
// 		return err
// 	}

// 	return nil
// }

func UnwindOtsApprovalsIndex(u *UnwindState, cfg OtsApprovalsIndexCfg, tx kv.RwTx, ctx context.Context) error {
	if !cfg.isEnabled {
		return nil
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := u.Done(tx); err != nil {
		return fmt.Errorf(" reset: %w", err)
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to write db commit: %w", err)
		}
	}
	return nil
}
