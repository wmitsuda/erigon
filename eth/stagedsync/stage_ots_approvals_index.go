package stagedsync

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type OtsApprovalsIndexCfg struct {
	db          kv.RwDB
	chainConfig *params.ChainConfig
	blockReader services.FullBlockReader
	tmpdir      string
	isEnabled   bool
}

// const buffLimit = 256 * datasize.MB

func StageOtsApprovalsIndexCfg(db kv.RwDB, chainConfig *params.ChainConfig, blockReader services.FullBlockReader, tmpdir string, isEnabled bool) OtsApprovalsIndexCfg {
	return OtsApprovalsIndexCfg{
		db:          db,
		chainConfig: chainConfig,
		blockReader: blockReader,
		tmpdir:      tmpdir,
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

	// Setup approvals table
	approvalsIdx, err := tx.RwCursorDupSort(kv.OtsApprovalsIndex)
	if err != nil {
		return err
	}
	defer approvalsIdx.Close()

	stopped := false
	currentBlock := startBlock
	approvalHash := common.HexToHash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
	for ; currentBlock <= endBlock && !stopped; /*&& currentBlock < 5000000*/ currentBlock++ {
		receipts := rawdb.ReadRawReceipts(tx, currentBlock)
		if receipts == nil {
			// Ignore on purpose, it could be a pruned receipt, which would constitute an
			// error, but also an empty block, which should be the case
			continue
		}

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

				// Locate existing approvals for token
				ownerAddr := common.BytesToAddress(l.Topics[1].Bytes())
				tokenAddr := l.Address
				spenderAddr := common.BytesToAddress(l.Topics[2].Bytes())

				key := dbutils.ApprovalsIdxKey(ownerAddr)
				k, v, err := approvalsIdx.SeekBothExact(key, tokenAddr[:])
				if err != nil {
					return err
				}
				if !bytes.Equal(k, key) || !bytes.HasPrefix(v, tokenAddr[:]) {
					spender := rawdb.NewSpender(spenderAddr)
					spender.Blocks = append(spender.Blocks, currentBlock)
					spenders := rawdb.Spenders{}
					spenders.Spenders = append(spenders.Spenders, *spender)

					buf, err := rlp.EncodeToBytes(&spenders)
					if err != nil {
						return err
					}

					buf2 := make([]byte, common.AddressLength+len(buf))
					copy(buf2, tokenAddr[:])
					copy(buf2[common.AddressLength:], buf)
					approvalsIdx.AppendDup(key, buf2)
					// log.Info(fmt.Sprintf("[%s] New spender", s.LogPrefix()), "k", hexutil.Encode(key2[:]), "size", len(cache))
				} else {
					existingSpenders := rawdb.Spenders{}
					err := rlp.DecodeBytes(v[common.AddressLength:], &existingSpenders)
					if err != nil {
						return err
					}

					// Merge existing spenders from DB
					var spenderFound *rawdb.Spender
					for _, ps := range existingSpenders.Spenders {
						if ps.Spender == spenderAddr {
							spenderFound = &ps
							break
						}
					}
					if spenderFound == nil {
						existingSpenders.Spenders = append(existingSpenders.Spenders, rawdb.Spender{Spender: spenderAddr, Blocks: []uint64{currentBlock}})
					} else {
						spenderFound.Blocks = append(spenderFound.Blocks, currentBlock)
					}

					// Update or save spenders
					err = approvalsIdx.DeleteCurrent()
					if err != nil {
						return err
					}

					buf, err := rlp.EncodeToBytes(&existingSpenders)
					if err != nil {
						return err
					}

					buf2 := make([]byte, common.AddressLength+len(buf))
					copy(buf2, tokenAddr[:])
					copy(buf2[common.AddressLength:], buf)
					approvalsIdx.AppendDup(key, buf2)
				}
			}
		}

		select {
		case <-ctx.Done():
			stopped = true
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Indexing approvals", s.LogPrefix()),
				"block", currentBlock)
		default:
		}
	}

	// loadFunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
	// 	prev, err := table.Get(k)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	existingSpenders := rawdb.Spenders{}
	// 	if prev != nil {
	// 		err := rlp.DecodeBytes(prev, &existingSpenders)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}

	// 	newSpenders := rawdb.Spenders{}
	// 	err = rlp.DecodeBytes(v, &newSpenders)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	// Merge existing spenders from DB
	// 	for _, s := range newSpenders.Spenders {
	// 		var spenderFound *rawdb.Spender
	// 		for _, ps := range existingSpenders.Spenders {
	// 			if ps.Spender == s.Spender {
	// 				spenderFound = &ps
	// 				break
	// 			}
	// 		}
	// 		if spenderFound == nil {
	// 			existingSpenders.Spenders = append(existingSpenders.Spenders, s)
	// 		} else {
	// 			spenderFound.Blocks = append(spenderFound.Blocks, s.Blocks...)
	// 		}
	// 	}

	// 	// Update or save spenders
	// 	buf, err := rlp.EncodeToBytes(existingSpenders)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if err := next(k, k, buf); err != nil {
	// 		return err
	// 	}

	// 	return nil
	// }
	// if err := collector.Load(tx, kv.OtsApprovalsIndex, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
	// 	return err
	// }

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

// func flushCache(cache map[string]*rawdb.Spenders, logPrefix string, currentBlock uint64, collector *etl.Collector) error {
// 	log.Info(fmt.Sprintf("[%s] Flushing spenders", logPrefix), "block", currentBlock)
// 	for k, v := range cache {
// 		buf, err := rlp.EncodeToBytes(v)
// 		if err != nil {
// 			return err
// 		}

// 		buf2 := make([]byte, common.AddressLength+len(buf))
// 		copy(buf2, []byte(k)[common.AddressLength:])
// 		copy(buf2[common.AddressLength:], buf)

// 		if err := collector.Collect([]byte(k)[:common.AddressLength], buf2); err != nil {
// 			return err
// 		}
// 		// log.Info(fmt.Sprintf("[%s] Collected", s.LogPrefix()), "k", hexutil.Encode(k[:]), "v", hexutil.Encode(buf))
// 	}
// 	return nil
// }

func UnwindOtsApprovalsIndex(u *UnwindState, cfg OtsApprovalsIndexCfg, tx kv.RwTx, ctx context.Context) error {
	if !cfg.isEnabled {
		return nil
	}

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
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
