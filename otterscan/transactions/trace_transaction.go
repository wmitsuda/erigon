package otterscan

import (
	"context"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"math/big"
	"time"
)

type TraceEntry struct {
	Type  string         `json:"type"`
	Depth int            `json:"depth"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
	Input hexutil.Bytes  `json:"input"`
}

type TransactionTracer struct {
	ctx     context.Context
	Results []*TraceEntry
}

func NewTransactionTracer(ctx context.Context) *TransactionTracer {
	return &TransactionTracer{
		ctx:     ctx,
		Results: make([]*TraceEntry, 0),
	}
}

func (l *TransactionTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, callType vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) error {
	if precompile {
		return nil
	}

	inputCopy := make([]byte, len(input))
	copy(inputCopy, input)
	_value := new(big.Int)
	_value.Set(value)
	if callType == vm.CALLT {
		l.Results = append(l.Results, &TraceEntry{"CALL", depth, from, to, (*hexutil.Big)(_value), inputCopy})
		return nil
	}
	if callType == vm.STATICCALLT {
		l.Results = append(l.Results, &TraceEntry{"STATICCALL", depth, from, to, nil, inputCopy})
		return nil
	}
	if callType == vm.DELEGATECALLT {
		l.Results = append(l.Results, &TraceEntry{"DELEGATECALL", depth, from, to, nil, inputCopy})
		return nil
	}
	if callType == vm.CALLCODET {
		l.Results = append(l.Results, &TraceEntry{"CALLCODE", depth, from, to, (*hexutil.Big)(_value), inputCopy})
		return nil
	}
	if callType == vm.CREATET {
		l.Results = append(l.Results, &TraceEntry{"CREATE", depth, from, to, (*hexutil.Big)(value), inputCopy})
		return nil
	}
	if callType == vm.CREATE2T {
		l.Results = append(l.Results, &TraceEntry{"CREATE2", depth, from, to, (*hexutil.Big)(value), inputCopy})
		return nil
	}

	return nil
}

func (l *TransactionTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *TransactionTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *TransactionTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, t time.Duration, err error) error {
	return nil
}

func (l *TransactionTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	last := l.Results[len(l.Results)-1]
	l.Results = append(l.Results, &TraceEntry{"SELFDESTRUCT", last.Depth + 1, from, to, (*hexutil.Big)(value), nil})
}

func (l *TransactionTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (*TransactionTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
