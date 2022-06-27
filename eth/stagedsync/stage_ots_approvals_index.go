package stagedsync

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
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

	f, err := os.OpenFile("approvals.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	stopped := false
	currentBlock := startBlock
	blockCount := 0
	txCount := 0
	approvalHash := common.HexToHash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
	for ; currentBlock <= endBlock && !stopped; /*&& currentBlock < 1023000;*/ currentBlock++ {
		_, err := cfg.blockReader.HeaderByNumber(ctx, tx, currentBlock)
		if err != nil {
			return err
		}

		// log.Info(fmt.Sprintf("[%s] Approvals index", s.LogPrefix()), "blockNumber", header.Number)
		receipts := rawdb.ReadRawReceipts(tx, currentBlock)
		if receipts == nil {
			// Ignore on purpose, it could be a pruned receipt, which would constitute an
			// error, but also an empty block, which should be the case
			// // log.Warn(fmt.Sprintf("[%s] No receipt for block", s.LogPrefix()), "block", currentBlock)
			continue
		}
		// log.Info(fmt.Sprintf("[%s] Found receipt for block", s.LogPrefix()), "block", currentBlock)
		found := false
		// block:
		for _, r := range receipts {
			for _, l := range r.Logs {
				// topics: [approvalHash, owner, spender]
				if len(l.Topics) != 3 {
					continue
				}
				if l.Topics[0] != approvalHash {
					continue
				}

				// log.Info(fmt.Sprintf("[%s] Found approval", s.LogPrefix()), "block", currentBlock, "token", l.Address, "owner", l.Topics[1], "spender", l.Topics[2])
				f.WriteString(fmt.Sprintf("%v;%v;%v\n", l.Address, l.Topics[1], l.Topics[2]))
				found = true
				txCount++
				// break block
			}
		}
		if found {
			blockCount++
			// cfg.blockReader.BodyWithTransactions(ctx, tx)
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

	log.Info(fmt.Sprintf("[%s] Indexed approvals", s.LogPrefix()), "blockCount", blockCount, "txCount", txCount)
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
