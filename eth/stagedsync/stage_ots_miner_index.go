package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type OtsMinerIndexCfg struct {
	db          kv.RwDB
	chainConfig *params.ChainConfig
	blockReader services.FullBlockReader
}

func StageOtsMinerIndexCfg(db kv.RwDB, chainConfig *params.ChainConfig, blockReader services.FullBlockReader) OtsMinerIndexCfg {
	return OtsMinerIndexCfg{
		db:          db,
		chainConfig: chainConfig,
		blockReader: blockReader,
	}
}

func SpawnStageOtsMinerIndex(cfg OtsMinerIndexCfg, s *StageState, tx kv.RwTx, ctx context.Context) error {
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

	minerIdx, err := tx.RwCursor(kv.OtsMinerIndex)
	if err != nil {
		return err
	}
	defer minerIdx.Close()

	stopped := false
	currentBlock := startBlock
	for ; currentBlock <= endBlock && !stopped; currentBlock++ {
		header, err := cfg.blockReader.HeaderByNumber(ctx, tx, currentBlock)
		if err != nil {
			return err
		}

		chunkKey, m, err := searchLastChunk(minerIdx, header.Coinbase)
		if err != nil {
			return err
		}

		if err := addBlock2Chunk(m, currentBlock, minerIdx, s, chunkKey); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Indexing miner addresses", s.LogPrefix()),
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

func searchLastChunk(minerIdx kv.Cursor, addr common.Address) ([]byte, *roaring64.Bitmap, error) {
	chunkKey := dbutils.MinerIdxKey(addr)
	k, v, err := minerIdx.SeekExact(chunkKey)
	if err != nil {
		return nil, nil, err
	}

	m := roaring64.New()
	if k != nil {
		reader := bytes.NewReader(v)
		_, err := m.ReadFrom(reader)
		if err != nil {
			return nil, nil, err
		}
	}

	return chunkKey, m, nil
}

func addBlock2Chunk(m *roaring64.Bitmap, currentBlockNumber uint64, minerIdx kv.RwCursor, s *StageState, chunkKey []byte) error {
	m.Add(currentBlockNumber)
	m.RunOptimize()

	// log.Info(fmt.Sprintf("[%s] Miner indexed", s.LogPrefix()), "blockNum", currentBlockNumber, "chunk", hexutil.Bytes(chunkKey), "count", m.GetCardinality())

	buf := bytes.NewBuffer(nil)
	if _, err := m.WriteTo(buf); err != nil {
		return err
	}
	if err := minerIdx.Put(chunkKey, buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func UnwindOtsMinerIndex(u *UnwindState, cfg OtsMinerIndexCfg, tx kv.RwTx, ctx context.Context) error {
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

func PruneOtsMinerIndex(p *PruneState, cfg OtsMinerIndexCfg, tx kv.RwTx, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
