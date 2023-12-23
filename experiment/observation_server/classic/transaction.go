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
	for addr, accessAddress := range *tx.AccessAddress {
		if accessAddress.CoarseRead {
			cgtx.readAddresses = append(cgtx.readAddresses, addr)
		}
		if accessAddress.CoarseWrite {
			cgtx.writeAddress = append(cgtx.writeAddress, addr)
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
