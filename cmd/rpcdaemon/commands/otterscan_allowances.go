package commands

import (
	"bytes"
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/rlp"
)

type Allowance struct {
	Owner   common.Address `json:"owner"`
	Token   common.Address `json:"token"`
	Spender common.Address `json:"spender"`
	Blocks  []uint64       `json:"blocks"`
}

type Approval struct {
	Token   common.Address `json:"token"`
	Spender common.Address `json:"spender"`
}

func (api *OtterscanAPIImpl) GetAllAllowances(ctx context.Context) ([]*Allowance, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	approvalsIdx, err := tx.Cursor(kv.OtsApprovalsIndex)
	if err != nil {
		return nil, err
	}
	defer approvalsIdx.Close()

	allowances := make([]*Allowance, 1)
	for k, v, err := approvalsIdx.First(); k != nil; k, v, err = approvalsIdx.Next() {
		if err != nil {
			return nil, err
		}
		// log.Info("ots_getAllAllowances", "k", hexutil.Encode(k))

		sp := rawdb.Spenders{}
		if err := rlp.DecodeBytes(v, &sp); err != nil {
			return nil, err
		}
		for _, s := range sp.Spenders {
			allowances = append(allowances, &Allowance{
				Owner:   common.BytesToAddress(k[:common.AddressLength]),
				Token:   common.BytesToAddress(k[common.AddressLength:]),
				Spender: s.Spender,
				Blocks:  s.Blocks,
			})
		}
	}

	return allowances, nil
}

func (api *OtterscanAPIImpl) GetAllowances(ctx context.Context, owner common.Address) ([]*Allowance, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	approvalsIdx, err := tx.Cursor(kv.OtsApprovalsIndex)
	if err != nil {
		return nil, err
	}
	defer approvalsIdx.Close()

	allowances := make([]*Allowance, 1)
	for k, v, err := approvalsIdx.Seek(dbutils.ApprovalsIdxKey(owner, common.HexToAddress("0x0000000000000000000000000000000000000000"))); k != nil; k, v, err = approvalsIdx.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, owner.Bytes()) {
			break
		}
		// log.Info("ots_getAllowances", "k", hexutil.Encode(k), "v", hexutil.Encode(v))

		sp := rawdb.Spenders{}
		if err := rlp.DecodeBytes(v, &sp); err != nil {
			return nil, err
		}
		for _, s := range sp.Spenders {
			allowances = append(allowances, &Allowance{
				Owner:   common.BytesToAddress(k[:common.AddressLength]),
				Token:   common.BytesToAddress(k[common.AddressLength:]),
				Spender: s.Spender,
				Blocks:  s.Blocks,
			})
		}
	}

	return allowances, nil
}

func (api *OtterscanAPIImpl) GetApprovals(ctx context.Context, owner common.Address) ([]*Approval, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	approvalsIdx, err := tx.Cursor(kv.OtsApprovalsIndex)
	if err != nil {
		return nil, err
	}
	defer approvalsIdx.Close()

	approvals := make([]*Approval, 1)
	for k, v, err := approvalsIdx.Seek(dbutils.ApprovalsIdxKey(owner, common.HexToAddress("0x0000000000000000000000000000000000000000"))); k != nil; k, v, err = approvalsIdx.Next() {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(k, owner.Bytes()) {
			break
		}

		token := common.BytesToAddress(k[common.AddressLength:])

		sp := rawdb.Spenders{}
		if err := rlp.DecodeBytes(v, &sp); err != nil {
			return nil, err
		}
		for _, s := range sp.Spenders {
			approvals = append(approvals, &Approval{
				Token:   token,
				Spender: s.Spender,
			})
		}
	}

	return approvals, nil
}
