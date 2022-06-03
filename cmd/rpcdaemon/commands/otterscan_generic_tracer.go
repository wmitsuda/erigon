package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type GenericTracer interface {
	vm.Tracer
	SetTransaction(tx types.Transaction)
	Found() bool
}

func (api *OtterscanAPIImpl) genericTracer(dbtx kv.Tx, ctx context.Context, blockNum uint64, chainConfig *params.ChainConfig, tracer GenericTracer) error {
	// Retrieve the transaction and assemble its EVM context
	blockHash, err := rawdb.ReadCanonicalHash(dbtx, blockNum)
	if err != nil {
		return err
	}

	block, _, err := rawdb.ReadBlockWithSenders(dbtx, blockHash, blockNum)
	if err != nil {
		return err
	}

	reader := state.NewPlainState(dbtx, blockNum)
	stateCache := shards.NewStateCache(32, 0 /* no limit */)
	cachedReader := state.NewCachedReader(reader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)

	ibs := state.New(cachedReader)
	signer := types.MakeSigner(chainConfig, blockNum)

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(dbtx, hash, number)
	}
	engine := ethash.NewFaker()
	checkTEVM := ethdb.GetHasTEVM(dbtx)

	// blockReceipts := rawdb.ReadReceipts(dbtx, block, senders)
	header := block.Header()
	rules := chainConfig.Rules(block.NumberU64())
	for idx, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), idx)

		msg, _ := tx.AsMessage(*signer, header.BaseFee, rules)

		BlockContext := core.NewEVMBlockContext(header, getHeader, engine, nil, checkTEVM)
		TxContext := core.NewEVMTxContext(msg)

		vmenv := vm.NewEVM(BlockContext, TxContext, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return err
		}
		_ = ibs.FinalizeTx(vmenv.ChainConfig().Rules(block.NumberU64()), cachedWriter)

		if tracer.Found() {
			tracer.SetTransaction(tx)
			return nil
		}
	}

	return nil
}
