package optimistic

import (
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"time"
)

type optimisticTxs []*optimisticTx

type optimisticTx struct {
	hash          common.Hash
	executionTime time.Duration
	readAddresses map[common.Address]int // address -> version
	writeAddress  map[common.Address]int // address -> version

	presetVersion map[common.Address]int
	allAddress    map[common.Address]int
	index         int
}

func newOptimisticTx(hash common.Hash, t time.Duration, index *big.Int) *optimisticTx {
	return &optimisticTx{
		hash:          hash,
		executionTime: t,
		readAddresses: make(map[common.Address]int),
		writeAddress:  make(map[common.Address]int),
		presetVersion: make(map[common.Address]int),
		allAddress:    make(map[common.Address]int),
		index:         int(index.Int64()),
	}
}

func (otx *optimisticTx) generateReadAndWrite(tx *transaction.Transaction) {
	storageTr := make([]*transaction.StorageTransition, 0)
	for _, tr := range tx.Transfers {
		if tr.GetLabel() == 0 { // state 转移(1,2,4)
			otx.stateTransitionAddress(tr.(*transaction.StateTransition))
		} else {
			storageTr = append(storageTr, tr.(*transaction.StorageTransition))
		}
	}
	otx.storageTransitionAddress(storageTr)

	for addr, _ := range otx.readAddresses {
		if _, ok := otx.allAddress[addr]; !ok {
			otx.allAddress[addr] = -1
		}
	}
}

func (otx *optimisticTx) stateTransitionAddress(tr *transaction.StateTransition) {
	if tr.Type == 3 {
		return
	}

	if tr.From != nil {
		addAddress2Map(&otx.readAddresses, tr.From.Address)
		addAddress2Map(&otx.writeAddress, tr.From.Address)
	}
	if tr.To != nil {
		addAddress2Map(&otx.readAddresses, tr.To.Address)
		addAddress2Map(&otx.writeAddress, tr.To.Address)
	}
}

func (otx *optimisticTx) storageTransitionAddress(trs []*transaction.StorageTransition) {
	var isWrite = make(map[common.Address]bool)
	for _, tr := range trs {
		//if otx.hash == common.HexToHash("0x82a709a952c500373129a35e53faf0651ad92f4858ba96006f5663c50623c796") {
		//	fmt.Println(tr.String())
		//}
		if _, ok := isWrite[tr.Contract]; !ok { // 未记录
			isWrite[tr.Contract] = false
		}
		if tr.NewValue != nil {
			isWrite[tr.Contract] = true
		}
	}
	for addr, write := range isWrite {
		if write { // 写了相关地址
			addAddress2Map(&otx.writeAddress, addr)
			addAddress2Map(&otx.readAddresses, addr)
		} else {
			addAddress2Map(&otx.readAddresses, addr)
			//fmt.Println("only read address: " + addr.Hex() + ", the tx hash is " + otx.hash.Hex())
		}
	}
}

func addAddress2Map(m *map[common.Address]int, addr common.Address) {
	if _, ok := (*m)[addr]; !ok { // 需要添加到map
		(*m)[addr] = -1
	}
}
