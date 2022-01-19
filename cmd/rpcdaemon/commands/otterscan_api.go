package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	otterscan "github.com/ledgerwatch/erigon/otterscan/transactions"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

// API_LEVEL Must be incremented every time new additions are made
const API_LEVEL = 6

type TransactionsWithReceipts struct {
	Txs       []*RPCTransaction        `json:"txs"`
	Receipts  []map[string]interface{} `json:"receipts"`
	FirstPage bool                     `json:"firstPage"`
	LastPage  bool                     `json:"lastPage"`
}

type OtterscanAPI interface {
	GetApiLevel() uint8
	GetInternalOperations(ctx context.Context, hash common.Hash) ([]*otterscan.InternalOperation, error)
	SearchTransactionsBefore(ctx context.Context, addr common.Address, blockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error)
	SearchTransactionsAfter(ctx context.Context, addr common.Address, blockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error)
	GetBlockDetails(ctx context.Context, number rpc.BlockNumber) (map[string]interface{}, error)
	GetBlockTransactions(ctx context.Context, number rpc.BlockNumber, pageNumber uint8, pageSize uint8) (map[string]interface{}, error)
	HasCode(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (bool, error)
	TraceTransaction(ctx context.Context, hash common.Hash) ([]*otterscan.TraceEntry, error)
	GetTransactionError(ctx context.Context, hash common.Hash) (hexutil.Bytes, error)
	GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (*common.Hash, error)
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

// Search transactions that touch a certain address.
//
// It searches back a certain block (excluding); the results are sorted descending.
//
// The pageSize indicates how many txs may be returned. If there are less txs than pageSize,
// they are just returned. But it may return a little more than pageSize if there are more txs
// than the necessary to fill pageSize in the last found block, i.e., let's say you want pageSize == 25,
// you already found 24 txs, the next block contains 4 matches, then this function will return 28 txs.
func (api *OtterscanAPIImpl) SearchTransactionsBefore(ctx context.Context, addr common.Address, blockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error) {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	callFromCursor, err := dbtx.Cursor(kv.CallFromIndex)
	if err != nil {
		return nil, err
	}
	defer callFromCursor.Close()

	callToCursor, err := dbtx.Cursor(kv.CallToIndex)
	if err != nil {
		return nil, err
	}
	defer callToCursor.Close()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	isFirstPage := false
	if blockNum == 0 {
		isFirstPage = true
	} else {
		// Internal search code considers blockNum [including], so adjust the value
		blockNum--
	}

	// Initialize search cursors at the first shard >= desired block number
	callFromProvider := NewCallCursorBackwardBlockProvider(callFromCursor, addr, blockNum)
	callToProvider := NewCallCursorBackwardBlockProvider(callToCursor, addr, blockNum)
	callFromToProvider := newCallFromToBlockProvider(false, callFromProvider, callToProvider)

	txs := make([]*RPCTransaction, 0, pageSize)
	receipts := make([]map[string]interface{}, 0, pageSize)

	resultCount := uint16(0)
	hasMore := true
	for {
		if resultCount >= pageSize || !hasMore {
			break
		}

		var results []*TransactionsWithReceipts
		results, hasMore, err = api.traceBlocks(ctx, addr, chainConfig, pageSize, resultCount, callFromToProvider)
		if err != nil {
			return nil, err
		}

		for _, r := range results {
			if r == nil {
				return nil, errors.New("internal error during search tracing")
			}

			for i := len(r.Txs) - 1; i >= 0; i-- {
				txs = append(txs, r.Txs[i])
			}
			for i := len(r.Receipts) - 1; i >= 0; i-- {
				receipts = append(receipts, r.Receipts[i])
			}

			resultCount += uint16(len(r.Txs))
			if resultCount >= pageSize {
				break
			}
		}
	}

	return &TransactionsWithReceipts{txs, receipts, isFirstPage, !hasMore}, nil
}

// Search transactions that touch a certain address.
//
// It searches forward a certain block (excluding); the results are sorted descending.
//
// The pageSize indicates how many txs may be returned. If there are less txs than pageSize,
// they are just returned. But it may return a little more than pageSize if there are more txs
// than the necessary to fill pageSize in the last found block, i.e., let's say you want pageSize == 25,
// you already found 24 txs, the next block contains 4 matches, then this function will return 28 txs.
func (api *OtterscanAPIImpl) SearchTransactionsAfter(ctx context.Context, addr common.Address, blockNum uint64, pageSize uint16) (*TransactionsWithReceipts, error) {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	callFromCursor, err := dbtx.Cursor(kv.CallFromIndex)
	if err != nil {
		return nil, err
	}
	defer callFromCursor.Close()

	callToCursor, err := dbtx.Cursor(kv.CallToIndex)
	if err != nil {
		return nil, err
	}
	defer callToCursor.Close()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return nil, err
	}

	isLastPage := false
	if blockNum == 0 {
		isLastPage = true
	} else {
		// Internal search code considers blockNum [including], so adjust the value
		blockNum++
	}

	// Initialize search cursors at the first shard >= desired block number
	callFromProvider := NewCallCursorForwardBlockProvider(callFromCursor, addr, blockNum)
	callToProvider := NewCallCursorForwardBlockProvider(callToCursor, addr, blockNum)
	callFromToProvider := newCallFromToBlockProvider(true, callFromProvider, callToProvider)

	txs := make([]*RPCTransaction, 0, pageSize)
	receipts := make([]map[string]interface{}, 0, pageSize)

	resultCount := uint16(0)
	hasMore := true
	for {
		if resultCount >= pageSize || !hasMore {
			break
		}

		var results []*TransactionsWithReceipts
		results, hasMore, err = api.traceBlocks(ctx, addr, chainConfig, pageSize, resultCount, callFromToProvider)
		if err != nil {
			return nil, err
		}

		for _, r := range results {
			if r == nil {
				return nil, errors.New("internal error during search tracing")
			}

			txs = append(txs, r.Txs...)
			receipts = append(receipts, r.Receipts...)

			resultCount += uint16(len(r.Txs))
			if resultCount >= pageSize {
				break
			}
		}
	}

	// Reverse results
	lentxs := len(txs)
	for i := 0; i < lentxs/2; i++ {
		txs[i], txs[lentxs-1-i] = txs[lentxs-1-i], txs[i]
		receipts[i], receipts[lentxs-1-i] = receipts[lentxs-1-i], receipts[i]
	}
	return &TransactionsWithReceipts{txs, receipts, !hasMore, isLastPage}, nil
}

func (api *OtterscanAPIImpl) traceBlocks(ctx context.Context, addr common.Address, chainConfig *params.ChainConfig, pageSize, resultCount uint16, callFromToProvider BlockProvider) ([]*TransactionsWithReceipts, bool, error) {
	var wg sync.WaitGroup

	// Estimate the common case of user address having at most 1 interaction/block and
	// trace N := remaining page matches as number of blocks to trace concurrently.
	// TODO: this is not optimimal for big contract addresses; implement some better heuristics.
	estBlocksToTrace := pageSize - resultCount
	results := make([]*TransactionsWithReceipts, estBlocksToTrace)
	totalBlocksTraced := 0
	hasMore := true

	for i := 0; i < int(estBlocksToTrace); i++ {
		var nextBlock uint64
		var err error
		nextBlock, hasMore, err = callFromToProvider()
		if err != nil {
			return nil, false, err
		}
		// TODO: nextBlock == 0 seems redundant with hasMore == false
		if !hasMore && nextBlock == 0 {
			break
		}

		wg.Add(1)
		totalBlocksTraced++
		go api.searchTraceBlock(ctx, &wg, addr, chainConfig, i, nextBlock, results)
	}
	wg.Wait()

	return results[:totalBlocksTraced], hasMore, nil
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

func (api *OtterscanAPIImpl) GetTransactionBySenderAndNonce(ctx context.Context, addr common.Address, nonce uint64) (*common.Hash, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	accHistoryC, err := tx.Cursor(kv.AccountsHistory)
	if err != nil {
		return nil, err
	}
	defer accHistoryC.Close()

	accChangesC, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return nil, err
	}
	defer accChangesC.Close()

	// Locate the chunk where the nonce happens
	acs := changeset.Mapper[kv.AccountChangeSet]
	k, v, err := accHistoryC.Seek(acs.IndexChunkKey(addr.Bytes(), 0))
	if err != nil {
		return nil, err
	}

	bitmap := roaring64.New()
	maxBlPrevChunk := uint64(0)
	var acc accounts.Account

	for {
		if k == nil || !bytes.HasPrefix(k, addr.Bytes()) {
			// Check plain state
			data, err := tx.GetOne(kv.PlainState, addr.Bytes())
			if err != nil {
				return nil, err
			}
			if err := acc.DecodeForStorage(data); err != nil {
				return nil, err
			}

			// Nonce changed in plain state, so it means the last block of last chunk
			// contains the actual nonce change
			if acc.Nonce > nonce {
				break
			}

			// Not found; asked for nonce still not used
			return nil, nil
		}

		// Inspect block changeset
		if _, err := bitmap.ReadFrom(bytes.NewReader(v)); err != nil {
			return nil, err
		}
		maxBl := bitmap.Maximum()
		data, err := acs.Find(accChangesC, maxBl, addr.Bytes())
		if err != nil {
			return nil, err
		}
		if err := acc.DecodeForStorage(data); err != nil {
			return nil, err
		}

		// Desired nonce was found in this chunk
		if acc.Nonce > nonce {
			break
		}

		maxBlPrevChunk = maxBl
		k, v, err = accHistoryC.Next()
		if err != nil {
			return nil, err
		}
	}

	// Locate the exact block inside chunk when the nonce changed
	blocks := bitmap.ToArray()
	var errSearch error = nil
	idx := sort.Search(len(blocks), func(i int) bool {
		if errSearch != nil {
			return false
		}

		// Locate the block changeset
		data, err := acs.Find(accChangesC, blocks[i], addr.Bytes())
		if err != nil {
			errSearch = err
			return false
		}

		if err := acc.DecodeForStorage(data); err != nil {
			errSearch = err
			return false
		}

		// Since the state contains the nonce BEFORE the block changes, we look for
		// the block when the nonce changed to be > the desired once, which means the
		// previous history block contains the actual change; it may contain multiple
		// nonce changes.
		return acc.Nonce > nonce
	})
	if errSearch != nil {
		return nil, errSearch
	}

	// Since the changeset contains the state BEFORE the change, we inspect
	// the block before the one we found; if it is the first block inside the chunk,
	// we use the last block from prev chunk
	nonceBlock := maxBlPrevChunk
	if idx > 0 {
		nonceBlock = blocks[idx-1]
	}
	found, txHash, err := findNonce(tx, addr, nonce, nonceBlock)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	return &txHash, nil
}

func findNonce(tx kv.Tx, addr common.Address, nonce uint64, blockNum uint64) (bool, common.Hash, error) {
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
		if s != addr {
			continue
		}

		t := body.Transactions[i]
		if t.GetNonce() == nonce {
			return true, t.Hash(), nil
		}
	}

	return false, common.Hash{}, nil
}
