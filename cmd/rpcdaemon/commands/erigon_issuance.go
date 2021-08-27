package commands

import (
	"bytes"
	"context"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
)

// BlockReward returns the block reward for this block
// func (api *ErigonImpl) BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
//	tx, err := api.db.Begin(ctx, ethdb.RO)
//	if err != nil {
//		return Issuance{}, err
//	}
//	defer tx.Rollback()
//
//	return api.rewardCalc(tx, blockNr, "block") // nolint goconst
//}

// UncleReward returns the uncle reward for this block
// func (api *ErigonImpl) UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
//	tx, err := api.db.Begin(ctx, ethdb.RO)
//	if err != nil {
//		return Issuance{}, err
//	}
//	defer tx.Rollback()
//
//	return api.rewardCalc(tx, blockNr, "uncle") // nolint goconst
//}

// Issuance implements erigon_issuance. Returns the total issuance (block reward plus uncle reward) for the given block.
func (api *ErigonImpl) Issuance(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return Issuance{}, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return Issuance{}, err
	}
	if chainConfig.Ethash == nil {
		// Clique for example has no issuance
		return Issuance{}, nil
	}

	block, err := api.getBlockByRPCNumber(tx, blockNr)
	if err != nil {
		return Issuance{}, err
	}
	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	issuance := minerReward
	for _, r := range uncleRewards {
		p := r // avoids warning?
		issuance.Add(&issuance, &p)
	}

	totalIssued, totalBurnt, err := api.getIssuance(tx, block)
	if err != nil {
		return Issuance{}, err
	}

	var ret Issuance
	ret.BlockReward = hexutil.EncodeBig(minerReward.ToBig())
	ret.Issuance = hexutil.EncodeBig(issuance.ToBig())
	issuance.Sub(&issuance, &minerReward)
	ret.UncleReward = hexutil.EncodeBig(issuance.ToBig())
	ret.TotalIssued = hexutil.EncodeBig(totalIssued.ToBig())
	ret.TotalBurnt = hexutil.EncodeBig(totalBurnt.ToBig())
	return ret, nil
}

func (api *ErigonImpl) getBlockByRPCNumber(tx kv.Tx, blockNr rpc.BlockNumber) (*types.Block, error) {
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}
	return rawdb.ReadBlockByNumber(tx, blockNum)
}

func (api *ErigonImpl) getIssuance(tx kv.Tx, block *types.Block) (*uint256.Int, *uint256.Int, error) {
	totalIssued := uint256.NewInt(0)
	totalBurnt := uint256.NewInt(0)

	key := dbutils.IssuanceKey(block.NumberU64())
	v, err := tx.GetOne(kv.Issuance, key)
	if err != nil {
		return nil, nil, err
	}

	if v != nil {
		totals := new(stagedsync.Issuance)
		reader := bytes.NewBuffer(v)
		if err := rlp.Decode(reader, totals); err != nil {
			return nil, nil, err
		}

		totalIssued.Set(totals.TotalIssued)
		totalBurnt.Set(totals.TotalBurnt)
	}

	return totalIssued, totalBurnt, nil
}

// Issuance structure to return information about issuance
type Issuance struct {
	BlockReward string `json:"blockReward,omitempty"`
	UncleReward string `json:"uncleReward,omitempty"`
	Issuance    string `json:"issuance,omitempty"`
	TotalIssued string `json:"totalIssued,omitempty"`
	TotalBurnt  string `json:"totalBurnt,omitempty"`
}
