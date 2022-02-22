package commands

import (
	"context"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/stack"
)

type CreateTracer struct {
	ctx     context.Context
	target  common.Address
	found   bool
	Creator common.Address
	Tx      types.Transaction
}

func NewCreateTracer(ctx context.Context, target common.Address) *CreateTracer {
	return &CreateTracer{
		ctx:    ctx,
		target: target,
		found:  false,
	}
}

func (t *CreateTracer) SetTransaction(tx types.Transaction) {
	t.Tx = tx
}

func (t *CreateTracer) Found() bool {
	return t.found
}

func (t *CreateTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) error {
	if t.found {
		return nil
	}
	if !create {
		return nil
	}
	if to != t.target {
		return nil
	}

	t.found = true
	t.Creator = from
	return nil
}

func (t *CreateTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, rData []byte, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (t *CreateTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, contract *vm.Contract, depth int, err error) error {
	return nil
}

func (t *CreateTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, d time.Duration, err error) error {
	return nil
}

func (t *CreateTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
}

func (t *CreateTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (t *CreateTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
