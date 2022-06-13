package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
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
	currentBlockNumber := s.BlockNumber + 1
	bodiesProgress, err := stages.GetStageProgress(tx, stages.Bodies)
	if err != nil {
		return fmt.Errorf("getting bodies progress: %w", err)
	}
	if currentBlockNumber > bodiesProgress {
		return nil
	}

	if err := s.Update(tx, bodiesProgress); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
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
