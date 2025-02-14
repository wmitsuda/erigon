package state

import (
	//"fmt"

	"bytes"
	"container/heap"
	"encoding/binary"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type reconPair struct {
	txNum      uint64 // txNum where the item has been created
	key1, key2 []byte
	val        []byte
}

func (i reconPair) Less(than btree.Item) bool { return ReconnLess(i, than.(reconPair)) }

func ReconnLess(i, thanItem reconPair) bool {
	if i.txNum == thanItem.txNum {
		c1 := bytes.Compare(i.key1, thanItem.key1)
		if c1 == 0 {
			c2 := bytes.Compare(i.key2, thanItem.key2)
			return c2 < 0
		}
		return c1 < 0
	}
	return i.txNum < thanItem.txNum
}

type ReconnWork struct {
	lock          sync.RWMutex
	doneBitmap    roaring64.Bitmap
	triggers      map[uint64][]*TxTask
	workCh        chan *TxTask
	queue         TxTaskQueue
	rollbackCount uint64
}

// ReconState is the accumulator of changes to the state
type ReconState struct {
	*ReconnWork //has it's own mutex. allow avoid lock-contention between state.Get() and work.Done() methods

	lock         sync.RWMutex
	changes      map[string]*btree.BTreeG[reconPair] // table => [] (txNum; key1; key2; val)
	sizeEstimate uint64
}

func NewReconState(workCh chan *TxTask) *ReconState {
	rs := &ReconState{
		ReconnWork: &ReconnWork{
			workCh:   workCh,
			triggers: map[uint64][]*TxTask{},
		},
		changes: map[string]*btree.BTreeG[reconPair]{},
	}
	return rs
}

func (rs *ReconState) Put(table string, key1, key2, val []byte, txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = btree.NewG[reconPair](32, ReconnLess)
		rs.changes[table] = t
	}
	item := reconPair{key1: key1, key2: key2, val: val, txNum: txNum}
	old, ok := t.ReplaceOrInsert(item)
	rs.sizeEstimate += uint64(len(key1)) + uint64(len(key2)) + uint64(len(val))
	if ok {
		rs.sizeEstimate -= uint64(len(old.key1)) + uint64(len(old.key2)) + uint64(len(old.val))
	}
}

func (rs *ReconState) Get(table string, key1, key2 []byte, txNum uint64) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	i, ok := t.Get(reconPair{txNum: txNum, key1: key1, key2: key2})
	if !ok {
		return nil
	}
	return i.val
}

func (rs *ReconState) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		var err error
		t.Ascend(func(item reconPair) bool {
			if len(item.val) == 0 {
				return true
			}
			var composite []byte
			if item.key2 == nil {
				composite = make([]byte, 8+len(item.key1))
			} else {
				composite = make([]byte, 8+len(item.key1)+8+len(item.key2))
				binary.BigEndian.PutUint64(composite[8+len(item.key1):], FirstContractIncarnation)
				copy(composite[8+len(item.key1)+8:], item.key2)
			}
			binary.BigEndian.PutUint64(composite, item.txNum)
			copy(composite[8:], item.key1)
			if err = rwTx.Put(table, composite, item.val); err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
		t.Clear(true)
	}
	rs.sizeEstimate = 0
	return nil
}

func (rs *ReconnWork) Schedule() (*TxTask, bool) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for rs.queue.Len() < 16 {
		txTask, ok := <-rs.workCh
		if !ok {
			// No more work, channel is closed
			break
		}
		heap.Push(&rs.queue, txTask)
	}
	if rs.queue.Len() > 0 {
		return heap.Pop(&rs.queue).(*TxTask), true
	}
	return nil, false
}

func (rs *ReconnWork) CommitTxNum(txNum uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if tt, ok := rs.triggers[txNum]; ok {
		for _, t := range tt {
			heap.Push(&rs.queue, t)
		}
		delete(rs.triggers, txNum)
	}
	rs.doneBitmap.Add(txNum)
}

func (rs *ReconnWork) RollbackTx(txTask *TxTask, dependency uint64) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if rs.doneBitmap.Contains(dependency) {
		heap.Push(&rs.queue, txTask)
	} else {
		tt := rs.triggers[dependency]
		tt = append(tt, txTask)
		rs.triggers[dependency] = tt
	}
	rs.rollbackCount++
}

func (rs *ReconnWork) Done(txNum uint64) bool {
	rs.lock.RLock()
	c := rs.doneBitmap.Contains(txNum)
	rs.lock.RUnlock()
	return c
}

func (rs *ReconnWork) DoneCount() uint64 {
	rs.lock.RLock()
	c := rs.doneBitmap.GetCardinality()
	rs.lock.RUnlock()
	return c
}

func (rs *ReconnWork) RollbackCount() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.rollbackCount
}

func (rs *ReconnWork) QueueLen() int {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.queue.Len()
}

func (rs *ReconState) SizeEstimate() uint64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.sizeEstimate
}

type StateReconWriter struct {
	ac        *libstate.Aggregator22Context
	rs        *ReconState
	txNum     uint64
	tx        kv.Tx
	composite []byte
}

func NewStateReconWriter(ac *libstate.Aggregator22Context, rs *ReconState) *StateReconWriter {
	return &StateReconWriter{
		ac: ac,
		rs: rs,
	}
}

func (w *StateReconWriter) SetTxNum(txNum uint64) {
	w.txNum = txNum
}

func (w *StateReconWriter) SetTx(tx kv.Tx) {
	w.tx = tx
}

func (w *StateReconWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addr := address.Bytes()
	txKey, err := w.tx.GetOne(kv.XAccount, addr)
	if err != nil {
		return err
	}
	if txKey == nil {
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		return nil
	}
	value := make([]byte, account.EncodingLengthForStorage())
	if account.Incarnation > 0 {
		account.Incarnation = FirstContractIncarnation
	}
	account.EncodeForStorage(value)
	//fmt.Printf("account [%x]=>{Balance: %d, Nonce: %d, Root: %x, CodeHash: %x} txNum: %d\n", address, &account.Balance, account.Nonce, account.Root, account.CodeHash, w.txNum)
	w.rs.Put(kv.PlainStateR, addr, nil, value, w.txNum)
	return nil
}

func (w *StateReconWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	addr, codeHashBytes := address.Bytes(), codeHash.Bytes()
	txKey, err := w.tx.GetOne(kv.XCode, addr)
	if err != nil {
		return err
	}
	if txKey == nil {
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		return nil
	}
	w.rs.Put(kv.CodeR, codeHashBytes, nil, common.CopyBytes(code), w.txNum)
	if len(code) > 0 {
		//fmt.Printf("code [%x] => %d CodeHash: %x, txNum: %d\n", address, len(code), codeHash, w.txNum)
		w.rs.Put(kv.PlainContractR, dbutils.PlainGenerateStoragePrefix(addr, FirstContractIncarnation), nil, codeHashBytes, w.txNum)
	}
	return nil
}

func (w *StateReconWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	return nil
}

func (w *StateReconWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if cap(w.composite) < 20+32 {
		w.composite = make([]byte, 20+32)
	} else {
		w.composite = w.composite[:20+32]
	}
	addr, k := address.Bytes(), key.Bytes()

	copy(w.composite, addr)
	copy(w.composite[20:], k)
	txKey, err := w.tx.GetOne(kv.XStorage, w.composite)
	if err != nil {
		return err
	}
	if txKey == nil {
		return nil
	}
	if stateTxNum := binary.BigEndian.Uint64(txKey); stateTxNum != w.txNum {
		return nil
	}
	if !value.IsZero() {
		//fmt.Printf("storage [%x] [%x] => [%x], txNum: %d\n", address, *key, value.Bytes(), w.txNum)
		w.rs.Put(kv.PlainStateR, addr, k, value.Bytes(), w.txNum)
	}
	return nil
}

func (w *StateReconWriter) CreateContract(address common.Address) error {
	return nil
}
