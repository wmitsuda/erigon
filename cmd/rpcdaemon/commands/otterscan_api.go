package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	otterscan "github.com/ledgerwatch/erigon/otterscan/transactions"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/log/v3"
)

// API_LEVEL Must be incremented every time new additions are made
const API_LEVEL = 6

type SearchResult struct {
	BlockNumber uint64
}

type BlockSearchResult struct {
	hash common.Hash
}

type TransactionsWithReceipts struct {
	Txs       []*RPCTransaction        `json:"txs"`
	Receipts  []map[string]interface{} `json:"receipts"`
	FirstPage bool                     `json:"firstPage"`
	LastPage  bool                     `json:"lastPage"`
}

type OtterscanAPI interface {
	GetApiLevel() uint8
	GetInternalOperations(ctx context.Context, hash common.Hash) ([]*otterscan.InternalOperation, error)
	SearchTransactionsBefore(ctx context.Context, addr common.Address, blockNum uint64, minPageSize uint16) (*TransactionsWithReceipts, error)
	SearchTransactionsAfter(ctx context.Context, addr common.Address, blockNum uint64, minPageSize uint16) (*TransactionsWithReceipts, error)
	GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error)
	GetBlockTransactions(ctx context.Context, number rpc.BlockNumber, pageNumber uint8, pageSize uint8) (map[string]interface{}, error)
	HasCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error)
	TraceTransaction(ctx context.Context, hash common.Hash) ([]*otterscan.TraceEntry, error)
	GetTransactionError(ctx context.Context, hash common.Hash) (hexutil.Bytes, error)
	GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (common.Hash, error)
}

type OtterscanAPIImpl struct {
	*BaseAPI
	db kv.RoDB
}

func NewOtterscanAPI(base *BaseAPI, db kv.RoDB) *OtterscanAPIImpl {
	return &OtterscanAPIImpl{
		BaseAPI: base,
		db:      db,
	}
}

func (api *OtterscanAPIImpl) GetApiLevel() uint8 {
	return API_LEVEL
}

func (api *OtterscanAPIImpl) GetInternalOperations(ctx context.Context, hash common.Hash) ([]*otterscan.InternalOperation, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	txn, blockHash, _, txIndex, err := rawdb.ReadTransaction(tx, hash)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	block, err := rawdb.ReadBlockByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	checkTEVM := ethdb.GetHasTEVM(tx)
	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, checkTEVM, ethash.NewFaker(), tx, blockHash, txIndex)
	if err != nil {
		return nil, err
	}

	tracer := otterscan.NewOperationsTracer(ctx)
	vmenv := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})

	if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(msg.Gas()), true, false /* gasBailout */); err != nil {
		return nil, fmt.Errorf("tracing failed: %v", err)
	}

	return tracer.Results, nil
}

func (api *OtterscanAPIImpl) SearchTransactionsBefore(ctx context.Context, addr common.Address, blockNum uint64, minPageSize uint16) (*TransactionsWithReceipts, error) {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	fromCursor, err := dbtx.Cursor(kv.CallFromIndex)
	if err != nil {
		return nil, err
	}
	defer fromCursor.Close()
	toCursor, err := dbtx.Cursor(kv.CallToIndex)
	if err != nil {
		return nil, err
	}
	defer toCursor.Close()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	// Initialize search cursors at the first shard >= desired block number
	resultCount := uint16(0)
	fromIter := newSearchBackIterator(fromCursor, addr, blockNum)
	toIter := newSearchBackIterator(toCursor, addr, blockNum)

	txs := make([]*RPCTransaction, 0)
	receipts := make([]map[string]interface{}, 0)

	multiIter, err := newMultiIterator(false, fromIter, toIter)
	if err != nil {
		return nil, err
	}
	eof := false
	for {
		if resultCount >= minPageSize || eof {
			break
		}

		var wg sync.WaitGroup
		results := make([]*TransactionsWithReceipts, 100, 100)
		tot := 0
		for i := 0; i < int(minPageSize-resultCount); i++ {
			var blockNum uint64
			blockNum, eof, err = multiIter()
			if err != nil {
				return nil, err
			}
			if eof {
				break
			}

			wg.Add(1)
			tot++
			go api.traceOneBlock(ctx, &wg, addr, chainConfig, i, blockNum, results)
		}
		wg.Wait()

		for i := 0; i < tot; i++ {
			r := results[i]
			if r == nil {
				return nil, errors.New("XXXX")
			}

			resultCount += uint16(len(r.Txs))
			for i := len(r.Txs) - 1; i >= 0; i-- {
				txs = append(txs, r.Txs[i])
			}
			for i := len(r.Receipts) - 1; i >= 0; i-- {
				receipts = append(receipts, r.Receipts[i])
			}

			if resultCount >= minPageSize {
				break
			}
		}
	}

	return &TransactionsWithReceipts{txs, receipts, blockNum == 0, eof}, nil
}

func newSearchBackIterator(cursor kv.Cursor, addr common.Address, maxBlock uint64) func() (uint64, bool, error) {
	search := make([]byte, common.AddressLength+8)
	copy(search[:common.AddressLength], addr.Bytes())
	if maxBlock == 0 {
		binary.BigEndian.PutUint64(search[common.AddressLength:], ^uint64(0))
	} else {
		binary.BigEndian.PutUint64(search[common.AddressLength:], maxBlock)
	}

	first := true
	var iter roaring64.IntIterable64

	return func() (uint64, bool, error) {
		if first {
			first = false
			k, v, err := cursor.Seek(search)
			if err != nil {
				return 0, true, err
			}
			if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
				return 0, true, nil
			}

			bitmap := roaring64.New()
			if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
				return 0, true, err
			}
			iter = bitmap.ReverseIterator()
		}

		var blockNum uint64
		for {
			if !iter.HasNext() {
				// Try and check previous shard
				k, v, err := cursor.Prev()
				if err != nil {
					return 0, true, err
				}
				if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
					return 0, true, nil
				}

				bitmap := roaring64.New()
				if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
					return 0, true, err
				}
				iter = bitmap.ReverseIterator()
			}
			blockNum = iter.Next()

			if maxBlock == 0 || blockNum < maxBlock {
				break
			}
		}
		return blockNum, false, nil
	}
}

func (api *OtterscanAPIImpl) SearchTransactionsAfter(ctx context.Context, addr common.Address, blockNum uint64, minPageSize uint16) (*TransactionsWithReceipts, error) {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	fromCursor, err := dbtx.Cursor(kv.CallFromIndex)
	if err != nil {
		return nil, err
	}
	defer fromCursor.Close()
	toCursor, err := dbtx.Cursor(kv.CallToIndex)
	if err != nil {
		return nil, err
	}
	defer toCursor.Close()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	// Initialize search cursors at the first shard >= desired block number
	resultCount := uint16(0)
	fromIter := newSearchForwardIterator(fromCursor, addr, blockNum)
	toIter := newSearchForwardIterator(toCursor, addr, blockNum)

	txs := make([]*RPCTransaction, 0)
	receipts := make([]map[string]interface{}, 0)

	multiIter, err := newMultiIterator(true, fromIter, toIter)
	if err != nil {
		return nil, err
	}
	eof := false
	for {
		if resultCount >= minPageSize || eof {
			break
		}

		var wg sync.WaitGroup
		results := make([]*TransactionsWithReceipts, 100, 100)
		tot := 0
		for i := 0; i < int(minPageSize-resultCount); i++ {
			var blockNum uint64
			blockNum, eof, err = multiIter()
			if err != nil {
				return nil, err
			}
			if eof {
				break
			}

			wg.Add(1)
			tot++
			go api.traceOneBlock(ctx, &wg, addr, chainConfig, i, blockNum, results)
		}
		wg.Wait()

		for i := 0; i < tot; i++ {
			r := results[i]
			if r == nil {
				return nil, errors.New("XXXX")
			}

			resultCount += uint16(len(r.Txs))
			for _, v := range r.Txs {
				txs = append([]*RPCTransaction{v}, txs...)
			}
			for _, v := range r.Receipts {
				receipts = append([]map[string]interface{}{v}, receipts...)
			}

			if resultCount > minPageSize {
				break
			}
		}
	}

	return &TransactionsWithReceipts{txs, receipts, eof, blockNum == 0}, nil
}

func newSearchForwardIterator(cursor kv.Cursor, addr common.Address, minBlock uint64) func() (uint64, bool, error) {
	search := make([]byte, common.AddressLength+8)
	copy(search[:common.AddressLength], addr.Bytes())
	if minBlock == 0 {
		binary.BigEndian.PutUint64(search[common.AddressLength:], uint64(0))
	} else {
		binary.BigEndian.PutUint64(search[common.AddressLength:], minBlock)
	}

	first := true
	var iter roaring64.IntIterable64

	return func() (uint64, bool, error) {
		if first {
			first = false
			k, v, err := cursor.Seek(search)
			if err != nil {
				return 0, true, err
			}
			if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
				return 0, true, nil
			}

			bitmap := roaring64.New()
			if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
				return 0, true, err
			}
			iter = bitmap.Iterator()
		}

		var blockNum uint64
		for {
			if !iter.HasNext() {
				// Try and check next shard
				k, v, err := cursor.Next()
				if err != nil {
					return 0, true, err
				}
				if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
					return 0, true, nil
				}

				bitmap := roaring64.New()
				if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
					return 0, true, err
				}
				iter = bitmap.Iterator()
			}
			blockNum = iter.Next()

			if minBlock == 0 || blockNum > minBlock {
				break
			}
		}
		return blockNum, false, nil
	}
}

func newMultiIterator(smaller bool, fromIter func() (uint64, bool, error), toIter func() (uint64, bool, error)) (func() (uint64, bool, error), error) {
	nextFrom, fromEnd, err := fromIter()
	if err != nil {
		return nil, err
	}
	nextTo, toEnd, err := toIter()
	if err != nil {
		return nil, err
	}

	return func() (uint64, bool, error) {
		if fromEnd && toEnd {
			return 0, true, nil
		}

		var blockNum uint64
		if !fromEnd {
			blockNum = nextFrom
		}
		if !toEnd {
			if smaller {
				if nextTo < blockNum {
					blockNum = nextTo
				}
			} else {
				if nextTo > blockNum {
					blockNum = nextTo
				}
			}
		}

		// Pull next; it may be that from AND to contains the same blockNum
		if !fromEnd && blockNum == nextFrom {
			nextFrom, fromEnd, err = fromIter()
			if err != nil {
				return 0, false, err
			}
		}
		if !toEnd && blockNum == nextTo {
			nextTo, toEnd, err = toIter()
			if err != nil {
				return 0, false, err
			}
		}
		return blockNum, false, nil
	}, nil
}

func (api *OtterscanAPIImpl) traceOneBlock(ctx context.Context, wg *sync.WaitGroup, addr common.Address, chainConfig *params.ChainConfig, idx int, bNum uint64, results []*TransactionsWithReceipts) {
	defer wg.Done()

	// Trace block for Txs
	newdbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		log.Error("ERR", "err", err)
		// TODO: signal error
		results[idx] = nil
	}
	defer newdbtx.Rollback()

	_, result, err := api.traceBlock(newdbtx, ctx, bNum, addr, chainConfig)
	if err != nil {
		// TODO: signal error
		log.Error("ERR", "err", err)
		results[idx] = nil
		//return nil, err
	}
	results[idx] = result
}

func (api *OtterscanAPIImpl) traceBlock(dbtx kv.Tx, ctx context.Context, blockNum uint64, searchAddr common.Address, chainConfig *params.ChainConfig) (bool, *TransactionsWithReceipts, error) {
	rpcTxs := make([]*RPCTransaction, 0)
	receipts := make([]map[string]interface{}, 0)

	// Retrieve the transaction and assemble its EVM context
	blockHash, err := rawdb.ReadCanonicalHash(dbtx, blockNum)
	if err != nil {
		return false, nil, err
	}

	block, senders, err := rawdb.ReadBlockWithSenders(dbtx, blockHash, blockNum)
	if err != nil {
		return false, nil, err
	}

	reader := state.NewPlainState(dbtx, blockNum-1)
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

	blockReceipts := rawdb.ReadReceipts(dbtx, block, senders)
	header := block.Header()
	found := false
	for idx, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), idx)

		msg, _ := tx.AsMessage(*signer, header.BaseFee)

		tracer := otterscan.NewTouchTracer(searchAddr)
		BlockContext := core.NewEVMBlockContext(header, getHeader, engine, nil, checkTEVM)
		TxContext := core.NewEVMTxContext(msg)

		vmenv := vm.NewEVM(BlockContext, TxContext, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return false, nil, err
		}
		_ = ibs.FinalizeTx(vmenv.ChainConfig().Rules(block.NumberU64()), cachedWriter)

		if tracer.Found {
			rpcTx := newRPCTransaction(tx, block.Hash(), blockNum, uint64(idx), block.BaseFee())
			mReceipt := marshalReceipt(blockReceipts[idx], tx, chainConfig, block)
			mReceipt["timestamp"] = block.Time()
			rpcTxs = append(rpcTxs, rpcTx)
			receipts = append(receipts, mReceipt)
			found = true
		}
	}

	return found, &TransactionsWithReceipts{rpcTxs, receipts, false, false}, nil
}

func (api *OtterscanAPIImpl) delegateGetBlockByNumber(tx kv.Tx, b *types.Block, number rpc.BlockNumber, inclTx bool) (map[string]interface{}, error) {
	td, err := rawdb.ReadTd(tx, b.Hash(), b.NumberU64())
	if err != nil {
		return nil, err
	}
	response, err := ethapi.RPCMarshalBlock(b, inclTx, inclTx)
	response["totalDifficulty"] = (*hexutil.Big)(td)
	response["transactionCount"] = b.Transactions().Len()

	if err == nil && number == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	// Explicitly drop unwanted fields
	response["logsBloom"] = nil
	return response, err
}

func (api *OtterscanAPIImpl) delegateIssuance(tx kv.Tx, block *types.Block, chainConfig *params.ChainConfig) (Issuance, error) {
	if chainConfig.Ethash == nil {
		// Clique for example has no issuance
		return Issuance{}, nil
	}

	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	issuance := minerReward
	for _, r := range uncleRewards {
		p := r // avoids warning?
		issuance.Add(&issuance, &p)
	}

	var ret Issuance
	ret.BlockReward = hexutil.EncodeBig(minerReward.ToBig())
	ret.Issuance = hexutil.EncodeBig(issuance.ToBig())
	issuance.Sub(&issuance, &minerReward)
	ret.UncleReward = hexutil.EncodeBig(issuance.ToBig())
	return ret, nil
}

func (api *OtterscanAPIImpl) delegateBlockFees(ctx context.Context, tx kv.Tx, block *types.Block, senders []common.Address, chainConfig *params.ChainConfig) (uint64, error) {
	receipts, err := getReceipts(ctx, tx, chainConfig, block, senders)
	if err != nil {
		return 0, fmt.Errorf("getReceipts error: %v", err)
	}

	fees := uint64(0)
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		effectiveGasPrice := uint64(0)
		if !chainConfig.IsLondon(block.NumberU64()) {
			effectiveGasPrice = txn.GetPrice().Uint64()
		} else {
			baseFee, _ := uint256.FromBig(block.BaseFee())
			gasPrice := new(big.Int).Add(block.BaseFee(), txn.GetEffectiveGasTip(baseFee).ToBig())
			effectiveGasPrice = gasPrice.Uint64()
		}
		fees += effectiveGasPrice * receipt.GasUsed
	}

	return fees, nil
}

func (api *OtterscanAPIImpl) getBlockWithSenders(number rpc.BlockNumber, tx kv.Tx) (*types.Block, []common.Address, error) {
	if number == rpc.PendingBlockNumber {
		return api.pendingBlock(), nil, nil
	}

	n, err := getBlockNumber(number, tx)
	if err != nil {
		return nil, nil, err
	}

	block, senders, err := rawdb.ReadBlockByNumberWithSenders(tx, n)
	return block, senders, err
}

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

func (api *OtterscanAPIImpl) GetBlockTransactions(ctx context.Context, number rpc.BlockNumber, pageNumber uint8, pageSize uint8) (map[string]interface{}, error) {
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

	getBlockRes, err := api.delegateGetBlockByNumber(tx, b, number, true)
	if err != nil {
		return nil, err
	}

	// Receipts
	receipts, err := getReceipts(ctx, tx, chainConfig, b, senders)
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %v", err)
	}
	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := b.Transactions()[receipt.TransactionIndex]
		marshalledRcpt := marshalReceipt(receipt, txn, chainConfig, b)
		marshalledRcpt["logs"] = nil
		marshalledRcpt["logsBloom"] = nil
		result = append(result, marshalledRcpt)
	}

	// Pruned block attrs
	prunedBlock := map[string]interface{}{}
	for _, k := range []string{"timestamp", "miner", "baseFeePerGas"} {
		prunedBlock[k] = getBlockRes[k]
	}

	// Crop tx input to 4bytes
	var txs = getBlockRes["transactions"].([]interface{})
	for _, rawTx := range txs {
		rpcTx := rawTx.(*ethapi.RPCTransaction)
		if len(rpcTx.Input) >= 4 {
			rpcTx.Input = rpcTx.Input[:4]
		}
	}

	// Crop page
	pageEnd := b.Transactions().Len() - int(pageNumber)*int(pageSize)
	pageStart := pageEnd - int(pageSize)
	if pageEnd < 0 {
		pageEnd = 0
	}
	if pageStart < 0 {
		pageStart = 0
	}

	response := map[string]interface{}{}
	getBlockRes["transactions"] = getBlockRes["transactions"].([]interface{})[pageStart:pageEnd]
	response["fullblock"] = getBlockRes
	response["receipts"] = result[pageStart:pageEnd]
	return response, nil
}

func (api *OtterscanAPIImpl) HasCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return false, fmt.Errorf("hasCode cannot open tx: %w", err)
	}
	defer tx.Rollback()

	blockNumber, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		return false, err
	}

	reader := adapter.NewStateReader(tx, blockNumber)
	acc, err := reader.ReadAccountData(address)
	if acc == nil || err != nil {
		return false, err
	}
	return !acc.IsEmptyCodeHash(), nil
}

func (api *OtterscanAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash) ([]*otterscan.TraceEntry, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	txn, blockHash, _, txIndex, err := rawdb.ReadTransaction(tx, hash)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	block, err := rawdb.ReadBlockByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	checkTEVM := ethdb.GetHasTEVM(tx)
	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, checkTEVM, ethash.NewFaker(), tx, blockHash, txIndex)
	if err != nil {
		return nil, err
	}

	tracer := otterscan.NewTransactionTracer(ctx)
	vmenv := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})

	if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(msg.Gas()), true, false /* gasBailout */); err != nil {
		return nil, fmt.Errorf("tracing failed: %v", err)
	}

	return tracer.Results, nil
}

func (api *OtterscanAPIImpl) GetTransactionError(ctx context.Context, hash common.Hash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	txn, blockHash, _, txIndex, err := rawdb.ReadTransaction(tx, hash)
	if err != nil {
		return nil, err
	}
	if txn == nil {
		return nil, fmt.Errorf("transaction %#x not found", hash)
	}
	block, err := rawdb.ReadBlockByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	checkTEVM := ethdb.GetHasTEVM(tx)
	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, checkTEVM, ethash.NewFaker(), tx, blockHash, txIndex)
	if err != nil {
		return nil, err
	}

	vmenv := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

	result, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(msg.Gas()), true, false /* gasBailout */)
	if err != nil {
		return nil, fmt.Errorf("tracing failed: %v", err)
	}

	return result.Revert(), nil
}

func (api *OtterscanAPIImpl) GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (common.Hash, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("getTransactionByAddressAndNonce cannot open tx: %w", err)
	}
	defer tx.Rollback()

	fromCursor, err := tx.Cursor(kv.CallFromIndex)
	if err != nil {
		return common.Hash{}, err
	}
	defer fromCursor.Close()

	// Locate which shard contains the tx with the desired nonce
	search := make([]byte, common.AddressLength+8)
	copy(search[:common.AddressLength], addr.Bytes())
	binary.BigEndian.PutUint64(search[common.AddressLength:], uint64(0))

	k, v, err := fromCursor.Seek(search)
	if err != nil {
		return common.Hash{}, err
	}
	if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
		return common.Hash{}, nil
	}

	bitmap := roaring64.New()
	if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
		return common.Hash{}, err
	}
	// log.Info("SHARD", "cardinality", bitmap.GetCardinality())

	c := 0
	for {
		maxBlock := bitmap.Maximum()
		maxBlockAfterNonce, err := GetNonceAfterBlock(tx, addr, maxBlock)
		if err != nil {
			return common.Hash{}, nil
		}

		// Tx with desired nonce is in this shard
		if maxBlockAfterNonce >= nonce {
			break
		}

		// Try and check next shard
		k, v, err := fromCursor.Next()
		if err != nil {
			return common.Hash{}, err
		}
		if !bytes.Equal(k[:common.AddressLength], addr.Bytes()) {
			return common.Hash{}, nil
		}

		if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
			return common.Hash{}, err
		}
		// log.Info("SHARD", "cardinality", bitmap.GetCardinality())
	}

	// Locate which block inside the shard contains the desired nonce
	blocks := bitmap.ToArray()
	blockIndex := sort.Search(len(blocks), func(i int) bool {
		c++
		// log.Info("BLOCKNUM", "c", c, "b", blocks[i])

		// TODO: handle error
		afterNonce, err := GetNonceAfterBlock(tx, addr, blocks[i])
		if err != nil {
			return false
		}
		return afterNonce >= nonce
	})

	// Not found
	if blockIndex == len(blocks) {
		return common.Hash{}, nil
	}
	blockNum := blocks[blockIndex]
	// log.Info("STATEREADER", "b", blockNum, "nonce", afterNonce)

	// Inspect the block; find tx corresponding to desired nonce
	found, hash, err := FindNonce(tx, addr, nonce, blockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if found {
		return hash, nil
	}

	return common.Hash{}, nil
}

func GetNonceAfterBlock(tx kv.Tx, addr common.Address, blockNum uint64) (uint64, error) {
	reader := adapter.NewStateReader(tx, blockNum)
	acc, err := reader.ReadAccountData(addr)
	if acc == nil || err != nil {
		return 0, err
	}
	return acc.Nonce, nil
}

func FindNonce(tx kv.Tx, addr common.Address, nonce uint64, blockNum uint64) (bool, common.Hash, error) {
	hash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		return false, common.Hash{}, err
	}
	senders, err := rawdb.ReadSenders(tx, hash, blockNum)
	if err != nil {
		return false, common.Hash{}, err
	}
	body := rawdb.ReadBodyWithTransactions(tx, hash, blockNum)

	for i, s := range senders {
		if s == addr {
			trans := body.Transactions[i]
			if trans.GetNonce() == nonce {
				log.Info("Found EXACT", "b", blockNum, "nonce", trans.GetNonce(), "tx", trans.Hash())
				return true, trans.Hash(), nil
			}
		}
	}

	return false, common.Hash{}, nil
}
