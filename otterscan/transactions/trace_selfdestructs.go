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

type SelfDestruct struct {
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *hexutil.Big   `json:"value"`
}

type SelfDestructTracer struct {
	ctx     context.Context
	Results []*SelfDestruct
}

func NewSelfDestructTracer(ctx context.Context) *SelfDestructTracer {
	return &SelfDestructTracer{
		ctx:     ctx,
		Results: make([]*SelfDestruct, 0),
	}
}

func (l *SelfDestructTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, codeHash common.Hash) error {
	return nil
}

func (l *SelfDestructTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *SelfDestructTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (l *SelfDestructTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	return nil
}

func (l *SelfDestructTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	l.Results = append(l.Results, &SelfDestruct{from, to, (*hexutil.Big)(value)})
}

func (l *SelfDestructTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (l *SelfDestructTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
