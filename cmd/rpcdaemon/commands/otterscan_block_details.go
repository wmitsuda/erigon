package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/rpc"
)

func (api *OtterscanAPIImpl) GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b, senders, err := api.getBlockWithSenders(number, tx)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, b, number, false)
	if err != nil {
		return nil, err
	}
	getIssuanceRes, err := api.delegateIssuance(tx, b, chainConfig)
	if err != nil {
		return nil, err
	}
	feesRes, err := api.delegateBlockFees(ctx, tx, b, senders, chainConfig)
	if err != nil {
		return nil, err
	}

	response := map[string]interface{}{}
	response["block"] = getBlockRes
	response["issuance"] = getIssuanceRes
	response["totalFees"] = hexutil.Uint64(feesRes)
	return response, nil
}

func (api *OtterscanAPIImpl) GetBlockDetailsByHash(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	b, senders, err := rawdb.ReadBlockByHashWithSenders(tx, hash)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getBlockRes, err := api.delegateGetBlockByNumber(tx, b, rpc.BlockNumber(b.Number().Int64()), false)
	if err != nil {
		return nil, err
	}
	getIssuanceRes, err := api.delegateIssuance(tx, b, chainConfig)
	if err != nil {
		return nil, err
	}
	feesRes, err := api.delegateBlockFees(ctx, tx, b, senders, chainConfig)
	if err != nil {
		return nil, err
	}

	response := map[string]interface{}{}
	response["block"] = getBlockRes
	response["issuance"] = getIssuanceRes
	response["totalFees"] = hexutil.Uint64(feesRes)
	return response, nil
}
