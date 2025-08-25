package classic

import (
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type classicGraphTxs []*classicGraphTx

type classicGraphTx struct {
	hash common.Hash
	tx   *transaction.Transaction

	// 读写集，map 实现
	//readAddresses map[common.Address]struct{}
	//writeAddress  map[common.Address]struct{}

	// 读写集，slice 实现
	readAddresses []common.Address
	writeAddress  []common.Address
	index         int

	mutex    sync.RWMutex
	inDegree int
}

func newClassicGraphTx(tx *transaction.Transaction) *classicGraphTx {
	return &classicGraphTx{
		hash: *tx.Hash,
		tx:   tx,
		//readAddresses: make(map[common.Address]int),
		//writeAddress:  make(map[common.Address]int),
		readAddresses: make([]common.Address, 0),
		writeAddress:  make([]common.Address, 0),
		index:         int(tx.Index.Int64()),
		inDegree:      0,
	}
}

func (cgtx *classicGraphTx) generateReadAndWrite(tx *transaction.Transaction) {
	storageTr := make([]*transaction.StorageTransition, 0)
	for _, tr := range tx.Transfers {
		if tr.GetLabel() == 0 { // state 转移(1,2,4)
			cgtx.stateTransitionAddress(tr.(*transaction.StateTransition))
		} else {
			storageTr = append(storageTr, tr.(*transaction.StorageTransition))
		}
	}
	cgtx.storageTransitionAddress(storageTr)
	//fmt.Println(cgtx.readAddresses)
	//fmt.Println(cgtx.writeAddress)
	//fmt.Println()

}

func (cgtx *classicGraphTx) stateTransitionAddress(tr *transaction.StateTransition) {
	if tr.Type == 3 { // 不对矿工结点进行计算
		return
	}

	if tr.From != nil {
		//addAddress2Map(&cgtx.readAddresses, tr.From.Address)
		//addAddress2Map(&cgtx.writeAddress, tr.From.Address)

		addAddr2Slice(&cgtx.readAddresses, tr.From.Address)
		addAddr2Slice(&cgtx.writeAddress, tr.From.Address)
	}
	if tr.To != nil {
		//addAddress2Map(&cgtx.readAddresses, tr.To.Address)
		//addAddress2Map(&cgtx.writeAddress, tr.To.Address)

		addAddr2Slice(&cgtx.readAddresses, tr.To.Address)
		addAddr2Slice(&cgtx.writeAddress, tr.To.Address)
	}
}

func (cgtx *classicGraphTx) storageTransitionAddress(trs []*transaction.StorageTransition) {
	var isWrite = make(map[common.Address]bool)
	for _, tr := range trs {
		//if cgtx.hash == common.HexToHash("0x82a709a952c500373129a35e53faf0651ad92f4858ba96006f5663c50623c796") {
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
			//addAddress2Map(&cgtx.writeAddress, addr)
			//addAddress2Map(&cgtx.readAddresses, addr)

			addAddr2Slice(&cgtx.writeAddress, addr)
			addAddr2Slice(&cgtx.readAddresses, addr)
		} else {
			//addAddress2Map(&cgtx.readAddresses, addr)

			addAddr2Slice(&cgtx.readAddresses, addr)
			//fmt.Println("only read address: " + addr.Hex() + ", the tx hash is " + cgtx.hash.Hex())
		}
	}
}

func addAddress2Map(m *map[common.Address]int, addr common.Address) {
	if _, ok := (*m)[addr]; !ok { // 需要添加到map
		(*m)[addr] = -1
	}
}

func addAddr2Slice(addresses *[]common.Address, address common.Address) {
	isExist := false
	for _, addr := range *addresses {
		if address == addr {
			isExist = true // Slice 中已存在相关地址
			break
		}
	}
	if !isExist {
		*addresses = append(*addresses, address)
	}
}
