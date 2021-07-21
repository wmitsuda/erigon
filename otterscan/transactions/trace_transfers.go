package otterscan

import (
	"context"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
)

type TransferType int

const (
	TRANSFER      TransferType = 0
	SELF_DESTRUCT TransferType = 1
	CREATE        TransferType = 2
	CREATE2       TransferType = 3
)

type TransactionTransfer struct {
	Type  TransferType   `json:"type"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
}

type TransferTracer struct {
	ctx     context.Context
	Results []*TransactionTransfer
}

func NewTransferTracer(ctx context.Context) *TransferTracer {
	return &TransferTracer{
		ctx:     ctx,
		Results: make([]*TransactionTransfer, 0),
	}
}

func (l *TransferTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, codeHash common.Hash) error {
	if depth == 0 {
		return nil
	}

	if calltype == vm.CALLT && value.Uint64() != 0 {
		l.Results = append(l.Results, &TransactionTransfer{TRANSFER, from, to, (*hexutil.Big)(value)})
		return nil
	}
	if calltype == vm.CREATET {
		l.Results = append(l.Results, &TransactionTransfer{CREATE, from, to, (*hexutil.Big)(value)})
	}
	if calltype == vm.CREATE2T {
		l.Results = append(l.Results, &TransactionTransfer{CREATE2, from, to, (*hexutil.Big)(value)})
	}

	return nil
}

func (l *TransferTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *TransferTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *TransferTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}

func (l *TransferTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	l.Results = append(l.Results, &TransactionTransfer{SELF_DESTRUCT, from, to, (*hexutil.Big)(value)})
}

func (l *TransferTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *TransferTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
