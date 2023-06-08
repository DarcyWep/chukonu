package abi_get

import (
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"sync"
	"testing"
	"time"
)

func TestAmendContract(t *testing.T) {
	db, err := openLeveldb(setting.NativeDbPath, true) // get native transaction or merge transaction
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}

	newNativeDB, _ := openLeveldb(setting.NewNativeDbPath, false)
	blockChan := make(chan *blockInsertInfo, 512)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go SaveAmendBlock(blockChan, newNativeDB, &wg)

	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		var blockStr string = ""
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		lastTx := txs[len(txs)-1] // 最后一个tx
		txs = txs[:len(txs)-1]
		//fmt.Println(number, len(txs))
		if len(txs) == 0 {
			blockChan <- newBlockInsertInfo(new(big.Int).SetInt64(int64(number)), []byte(lastTx.String()))
			continue
		}
		blockStr = AmendContract(txs)
		blockStr = blockStr + lastTx.String()
		blockChan <- newBlockInsertInfo(new(big.Int).SetInt64(int64(number)), []byte(blockStr))
	}
	close(blockChan)
}

func TestGetAllContractAddressFromTxs(t *testing.T) {
	db, err := openLeveldb(setting.NewNativeDbPath, true) // get native transaction or merge transaction
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}

	//contractdb, _ := openLeveldb(setting.ContractLeveldb)
	contracts := make(map[common.Address]struct{})

	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}
		GetContractAddresses(txs, &contracts)
		fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d, contract's num: %d\n", number, len(contracts))
	}
	SaveContractAddressesToDB(&contracts)
	tmp := GetAllContractAddressesSlice()
	fmt.Println(tmp, len(*tmp)) // 86232
}

func TestGetContractABI(t *testing.T) {
	//创建连接指定网络的客户端
	etherscanClient := newEtherScan()
	contractAddresses := GetAllContractAddressesSlice() // 否则会引起资源争用

	contractdb, _ := openLeveldb(setting.ContractLeveldb, false)
	defer contractdb.Close()

	abiChan := make(chan *abiInfo, 512)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go SaveContractABI(abiChan, contractdb, &wg)

	for i, addr := range *contractAddresses {
		_, err := contractdb.Get([]byte(addr.String()), nil)
		if err == nil {
			continue
		}

		abiStr, _ := etherscanClient.ContractABI(addr.String())
		abiChan <- newAbiInfo(i, addr, []byte(abiStr))

		//if i%100 == 0 {
		//	err := contractdb.Write(batch, nil)
		//	if err != nil {
		//		fmt.Println("save contract abi error", err)
		//	}
		//	batch.Reset()
		//	fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish contract index %d\n", i)
		//}
		//err = contractdb.Put([]byte(addr.String()), []byte(abiStr), nil)
	}
	close(abiChan)
}

// 23200
