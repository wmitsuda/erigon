package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
)

func (api *OtterscanAPIImpl) IsMiner(ctx context.Context, addr common.Address) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	minerIdx, err := tx.Cursor(kv.OtsMinerIndex)
	if err != nil {
		return false, err
	}
	k, _, err := minerIdx.SeekExact(dbutils.MinerIdxKey(addr))
	if err != nil {
		return false, err
	}
	return k != nil, nil
}
