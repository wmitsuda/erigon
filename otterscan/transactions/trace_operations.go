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

type OperationType int

const (
	TRANSFER      OperationType = 0
	SELF_DESTRUCT OperationType = 1
	CREATE        OperationType = 2
	CREATE2       OperationType = 3
)

type InternalOperation struct {
	Type  OperationType  `json:"type"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
}

type OperationsTracer struct {
	ctx     context.Context
	Results []*InternalOperation
}

func NewOperationsTracer(ctx context.Context) *OperationsTracer {
	return &OperationsTracer{
		ctx:     ctx,
		Results: make([]*InternalOperation, 0),
	}
}

func (l *OperationsTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) error {
	if depth == 0 {
		return nil
	}

	if calltype == vm.CALLT && value.Uint64() != 0 {
		l.Results = append(l.Results, &InternalOperation{TRANSFER, from, to, (*hexutil.Big)(value)})
		return nil
	}
	if calltype == vm.CREATET {
		l.Results = append(l.Results, &InternalOperation{CREATE, from, to, (*hexutil.Big)(value)})
	}
	if calltype == vm.CREATE2T {
		l.Results = append(l.Results, &InternalOperation{CREATE2, from, to, (*hexutil.Big)(value)})
	}

	return nil
}

func (l *OperationsTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *OperationsTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *OperationsTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) error {
	return nil
}

func (l *OperationsTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	l.Results = append(l.Results, &InternalOperation{SELF_DESTRUCT, from, to, (*hexutil.Big)(value)})
}

func (l *OperationsTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *OperationsTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
