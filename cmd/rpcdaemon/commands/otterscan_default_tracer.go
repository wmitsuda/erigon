package commands

import (
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/vm"
)

type DefaultTracer struct {
}

func (t *DefaultTracer) CaptureStart(env *vm.EVM, depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int, code []byte) {
}

func (t *DefaultTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, depth int, err error) {
}

func (t *DefaultTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {
}

func (t *DefaultTracer) CaptureEnd(depth int, output []byte, startGas, endGas uint64, d time.Duration, err error) {
}

func (t *DefaultTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
}

func (t *DefaultTracer) CaptureAccountRead(account common.Address) error {
	return nil
}

func (t *DefaultTracer) CaptureAccountWrite(account common.Address) error {
	return nil
}
