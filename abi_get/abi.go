package abi_get

import (
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"github.com/nanmu42/etherscan-api"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/big"
	"strings"
	"sync"
	"time"
)

func openLeveldb(path string, readOnly bool) (*leveldb.DB, error) {
	return leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: setting.MinHandles,
		BlockCacheCapacity:     setting.MinCache / 2 * opt.MiB,
		WriteBuffer:            setting.MinCache / 4 * opt.MiB, // Two of these are used internally
		ReadOnly:               readOnly,
	})
}

type blockInsertInfo struct {
	number *big.Int
	data   []byte
}

func newBlockInsertInfo(number *big.Int, data []byte) *blockInsertInfo {
	return &blockInsertInfo{
		number: new(big.Int).Set(number),
		data:   common.CopyBytes(data),
	}
}

func newEtherScan() *etherscan.Client {
	return etherscan.New(etherscan.Mainnet, setting.ApiKey)
}

func AmendContract(txs []*transaction.Transaction) string {
	txsStr := ""
	for _, tx := range txs {
		// 如果是智能合约交易且不为合约创建操作, 重新检查是否为智能合约
		if tx.Contract && tx.To != nil {
			tx.Contract = false
			for _, tr := range tx.Transfers {
				if tr.GetLabel() == 1 {
					tx.Contract = true // have a storage of contract, tx is smart contract
					break
				}
			}
		}
		txsStr = txsStr + tx.String() + ";"
	}
	return txsStr
}

func SaveAmendBlock(blockChan chan *blockInsertInfo, db *leveldb.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	var batch = new(leveldb.Batch)

	for block := range blockChan {
		batch.Put([]byte(block.number.String()), block.data)

		if block.number.Int64()%50 == 0 {
			err := db.Write(batch, nil)
			if err != nil {
				fmt.Println("Failed to store new native block, number is "+block.number.String()+", error is", err)
			} else {
				fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish new native blocks, number is %d\n", block.number)
			}
			batch.Reset()
		}
	}
}

func GetContractAddresses(txs []*transaction.Transaction, contracts *map[common.Address]struct{}) {
	for _, tx := range txs {
		if tx.Contract && tx.To != nil { // 如果是智能合约交易且不为合约创建操作
			//if *tx.Hash == common.HexToHash("0x5030a4c259fe5ed5d0eead213e388f2ac0d6042e37b44190f11b9b0cfa24b86b") {
			//	for _, tr := range tx.Transfers {
			//		fmt.Println(tr.String())
			//	}
			//}
			//if len(tx.Input) < 4 {
			//	fmt.Println(tx.Hash)
			//}

			if _, ok := (*contracts)[*tx.To]; !ok { // 未加入合约队列
				(*contracts)[*tx.To] = struct{}{}
			}
		}
	}
}

func SaveContractAddressesToDB(contracts *map[common.Address]struct{}) {
	contractdb, _ := openLeveldb(setting.ContractLeveldb, false)
	defer contractdb.Close()
	contractString := ""
	for addr, _ := range *contracts {
		contractString = contractString + addr.String() + " "
	}
	contractString = contractString[:len(contractString)-1]
	err := contractdb.Put([]byte(setting.AllContractAddressKey), []byte(contractString), nil)
	if err != nil {
		fmt.Println("save all contract address error", err)
	}
}

func GetAllContractAddressesSlice() *[]common.Address {
	contractdb, _ := openLeveldb(setting.ContractLeveldb, true)
	defer contractdb.Close()

	value, _ := contractdb.Get([]byte(setting.AllContractAddressKey), nil)
	contractStringSlice := strings.Split(string(value), " ")
	contractSlice := make([]common.Address, 0)
	for _, addrStr := range contractStringSlice {
		contractSlice = append(contractSlice, common.HexToAddress(addrStr))
	}
	return &contractSlice
}

type abiInfo struct {
	index   int
	address string
	abi     []byte
}

func newAbiInfo(index int, addr common.Address, abi []byte) *abiInfo {
	return &abiInfo{
		index:   index,
		address: addr.String(),
		abi:     common.CopyBytes(abi),
	}
}

func SaveContractABI(abiChan chan *abiInfo, db *leveldb.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	var batch = new(leveldb.Batch)

	for abi := range abiChan {
		batch.Put([]byte(abi.address), abi.abi)

		if abi.index%100 == 0 || abi.index == 86231 { // 最多有86232个合约地址
			err := db.Write(batch, nil)
			if err != nil {
				fmt.Println("Failed to store contract abi, address is "+abi.address+", error is", err)
			} else {
				fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish contract index %d\n", abi.index)
			}
			batch.Reset()
		}
	}
}
