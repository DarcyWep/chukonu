package abi_get

import (
	"chukonu/setting"
	"encoding/json"
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
	db, err := openLeveldb(setting.NativeDbPath, true) // get native transaction or merge transaction
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
	tmp := GetAllContractAddressesSlice(setting.AllContractAddressKey)
	fmt.Println(tmp, len(*tmp)) // 86232
}

func TestGetContractABI(t *testing.T) {
	//创建连接指定网络的客户端
	//etherscanClient := newEtherScan()
	//allContractAddresses := GetAllContractAddressesSlice(setting.AllContractAddressKey)             // 否则会引起资源争用
	//finishContractAddresses := GetAllContractAddressesSlice(setting.FinishContractAddressKey)       // 否则会引起资源争用
	//notFinishContractAddresses := GetAllContractAddressesSlice(setting.NotFinishContractAddressKey) // 否则会引起资源争用
	//fmt.Println(len(*allContractAddresses), len(*finishContractAddresses), len(*notFinishContractAddresses))
	contractdb, _ := openLeveldb(setting.ContractLeveldb, false)
	defer contractdb.Close()

	//contractdb.Put([]byte("0x365Ae2495a131aF30F0F5FF2AC0cEc25a09eFeA8"), []byte(""), nil)

	//for _, addr := range *finishContractAddresses {
	//	abi, _ := contractdb.Get([]byte(addr.String()), nil)
	//	if len(abi) != 0 {
	//		fmt.Println(len(string(abi)), string(abi)[:20])
	//		time.Sleep(time.Millisecond)
	//	}
	//}

	//abiChan := make(chan *abiInfo, 512)
	//var wg sync.WaitGroup
	//wg.Add(1)
	//defer wg.Wait()
	//go SaveContractABI(abiChan, contractdb, &wg)
	//
	//for i, addr := range *notFinishContractAddresses {
	//	//fmt.Println(i, addr)
	//	//_, err := contractdb.Get([]byte(addr.String()), nil)
	//	//if err == nil {
	//	//	continue
	//	//}
	//
	//	abiStr, err := etherscanClient.ContractABI(addr.String())
	//	if err != nil && err.Error() != "etherscan server: NOTOK" {
	//		fmt.Println("etherscan get abi error,", addr, err)
	//	}
	//	abiChan <- newAbiInfo(i, addr, []byte(abiStr))
	//
	//	//if i%100 == 0 {
	//	//	err := contractdb.Write(batch, nil)
	//	//	if err != nil {
	//	//		fmt.Println("save contract abi error", err)
	//	//	}
	//	//	batch.Reset()
	//	//	fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish contract index %d\n", i)
	//	//}
	//	//err = contractdb.Put([]byte(addr.String()), []byte(abiStr), nil)
	//}
	//close(abiChan)
}

func TestAddContractFuncNameToTx(t *testing.T) {
	contractDB, _ := openLeveldb(setting.ContractLeveldb, false)
	defer contractDB.Close()

	blockDB, _ := openLeveldb(setting.NativeDbPath, true)
	defer blockDB.Close()

	newNativeDB, _ := openLeveldb(setting.NewNativeDbPath, false)
	defer newNativeDB.Close()

	//abi, _ := contractDB.Get([]byte("0x4DF812F6064def1e5e029f1ca858777CC98D2D81"), nil)
	//fmt.Println(string(abi))
	//newAbi := "[{\"constant\":false,\"inputs\":[{\"name\":\"_dataContractAddress\",\"type\":\"address\"}],\"name\":\"setDataContract\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_alliesContractAddress\",\"type\":\"address\"}],\"name\":\"setAlliesContract\",\"outputs\":[],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"name\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_spender\",\"type\":\"address\"},{\"name\":\"_value\",\"type\":\"uint256\"}],\"name\":\"approve\",\"outputs\":[{\"name\":\"success\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"dataContract\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_curratorAddress\",\"type\":\"address\"}],\"name\":\"setXauForGasCurrator\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_newOwnerAdress\",\"type\":\"address\"}],\"name\":\"setOwner\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"totalSupply\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_meltingContractAddress\",\"type\":\"address\"}],\"name\":\"setMeltingContract\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_from\",\"type\":\"address\"},{\"name\":\"_to\",\"type\":\"address\"},{\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"transferFrom\",\"outputs\":[{\"name\":\"status\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_source\",\"type\":\"address\"},{\"name\":\"_to\",\"type\":\"address\"},{\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"transferViaProxy\",\"outputs\":[{\"name\":\"status\",\"type\":\"bool\"},{\"name\":\"sendFrom\",\"type\":\"address\"},{\"name\":\"sentTo\",\"type\":\"address\"},{\"name\":\"sentToAmount\",\"type\":\"uint256\"},{\"name\":\"burnAddress\",\"type\":\"address\"},{\"name\":\"burnAmount\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"decimals\",\"outputs\":[{\"name\":\"\",\"type\":\"uint8\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_to\",\"type\":\"address\"}],\"name\":\"getGasForXau\",\"outputs\":[{\"name\":\"sucess\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"lockdown\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"alliesContract\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"totalGoldSupply\",\"outputs\":[{\"name\":\"\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"standard\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_from\",\"type\":\"address\"},{\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"doBurnFromContract\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"_owner\",\"type\":\"address\"}],\"name\":\"balanceOf\",\"outputs\":[{\"name\":\"balance\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_proxyContractAddress\",\"type\":\"address\"}],\"name\":\"setProxyContract\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_accountAddress\",\"type\":\"address\"}],\"name\":\"isAccountLocked\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[],\"name\":\"fillGas\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_xaurAmount\",\"type\":\"uint256\"},{\"name\":\"_goldAmount\",\"type\":\"uint256\"}],\"name\":\"doMelt\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_coinageAddresses\",\"type\":\"address[]\"},{\"name\":\"_coinageAmounts\",\"type\":\"uint256[]\"},{\"name\":\"_usdAmount\",\"type\":\"uint256\"},{\"name\":\"_xaurCoined\",\"type\":\"uint256\"},{\"name\":\"_goldBought\",\"type\":\"uint256\"}],\"name\":\"doCoinage\",\"outputs\":[{\"name\":\"\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"burningAdress\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"coinageContract\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"dev\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"symbol\",\"outputs\":[{\"name\":\"\",\"type\":\"string\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_to\",\"type\":\"address\"},{\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"transfer\",\"outputs\":[{\"name\":\"status\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[],\"name\":\"freezeCoin\",\"outputs\":[],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_source\",\"type\":\"address\"},{\"name\":\"_from\",\"type\":\"address\"},{\"name\":\"_to\",\"type\":\"address\"},{\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"transferFromViaProxy\",\"outputs\":[{\"name\":\"status\",\"type\":\"bool\"},{\"name\":\"sendFrom\",\"type\":\"address\"},{\"name\":\"sentTo\",\"type\":\"address\"},{\"name\":\"sentToAmount\",\"type\":\"uint256\"},{\"name\":\"burnAddress\",\"type\":\"address\"},{\"name\":\"burnAmount\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_spender\",\"type\":\"address\"},{\"name\":\"_value\",\"type\":\"uint256\"},{\"name\":\"_extraData\",\"type\":\"bytes\"}],\"name\":\"approveAndCall\",\"outputs\":[{\"name\":\"success\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"_owner\",\"type\":\"address\"},{\"name\":\"_spender\",\"type\":\"address\"}],\"name\":\"allowance\",\"outputs\":[{\"name\":\"remaining\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_block\",\"type\":\"uint256\"}],\"name\":\"lockAccount\",\"outputs\":[{\"name\":\"answer\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[{\"name\":\"_source\",\"type\":\"address\"},{\"name\":\"_owner\",\"type\":\"address\"},{\"name\":\"_spender\",\"type\":\"address\"}],\"name\":\"allowanceFromProxy\",\"outputs\":[{\"name\":\"remaining\",\"type\":\"uint256\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"proxyContract\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_source\",\"type\":\"address\"},{\"name\":\"_spender\",\"type\":\"address\"},{\"name\":\"_value\",\"type\":\"uint256\"}],\"name\":\"approveFromProxy\",\"outputs\":[{\"name\":\"success\",\"type\":\"bool\"}],\"type\":\"function\"},{\"constant\":true,\"inputs\":[],\"name\":\"meltingContract\",\"outputs\":[{\"name\":\"\",\"type\":\"address\"}],\"type\":\"function\"},{\"constant\":false,\"inputs\":[{\"name\":\"_coinageContractAddress\",\"type\":\"address\"}],\"name\":\"setCoinageContract\",\"outputs\":[],\"type\":\"function\"},{\"inputs\":[{\"name\":\"_burningAddress\",\"type\":\"address\"}],\"type\":\"constructor\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"from\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"to\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"value\",\"type\":\"uint256\"}],\"name\":\"Transfer\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"_owner\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"_spender\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"_value\",\"type\":\"uint256\"}],\"name\":\"Approval\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"from\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"value\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"burningType\",\"type\":\"uint256\"}],\"name\":\"Burn\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"xaurAmount\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"goldAmount\",\"type\":\"uint256\"}],\"name\":\"Melt\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"coinageId\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"usdAmount\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"xaurAmount\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"goldAmount\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"totalGoldSupply\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"totalSupply\",\"type\":\"uint256\"}],\"name\":\"Coinage\",\"type\":\"event\"}]"
	//contractDB.Put([]byte("0x4DF812F6064def1e5e029f1ca858777CC98D2D81"), []byte(newAbi), nil)
	//abi, _ = contractDB.Get([]byte("0x4DF812F6064def1e5e029f1ca858777CC98D2D81"), nil)
	//fmt.Println(string(abi))

	blockChan := make(chan *blockInsertInfo, 512)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go SaveAmendBlock(blockChan, newNativeDB, &wg)

	contractMap := make(map[common.Address]*Contract)
	funcSlice := make([]*Function, 0)
	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		var blockStr string = ""
		txs, _ := pureData.GetTransactionsByNumber(blockDB, new(big.Int).SetInt64(int64(number)))
		lastTx := txs[len(txs)-1] // 最后一个tx
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			blockChan <- newBlockInsertInfo(new(big.Int).SetInt64(int64(number)), []byte(lastTx.String()))
			continue
		}

		blockStr = AddContractFuncNameToTx(txs, contractDB, &contractMap, &funcSlice)
		blockStr = blockStr + lastTx.String()
		blockChan <- newBlockInsertInfo(new(big.Int).SetInt64(int64(number)), []byte(blockStr))
		//fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d, contract's num: %d\n", number, len(contracts))
	}

	SaveSortFunctions(&funcSlice, contractDB)
	SaveSortContracts(&contractMap, contractDB)
	close(blockChan)

	//
	//for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
	//
	//	txs, err := pureData.GetTransactionsByNumber(newNativeDB, new(big.Int).SetInt64(int64(number)))
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	fmt.Println(len(txs))
	//	for _, tx := range txs {
	//		fmt.Println(tx.String()[len(tx.String())-30:])
	//	}
	//}

	//funcBytes, _ := contractDB.Get([]byte(setting.SortContractsKey), nil)
	//funcs := make([]*Contract, 0)
	//json.Unmarshal(funcBytes, &funcs)
	//for _, fu := range funcs {
	//	fmt.Println(fu.Address, fu.CallNum, len(fu.Functions))
	//}
}

func TestTransactionCategories(t *testing.T) {
	contractDB, _ := openLeveldb(setting.ContractLeveldb, false)
	defer contractDB.Close()

	blockDB, _ := openLeveldb(setting.NativeDbPath, true)
	defer blockDB.Close()

	newNativeDB, _ := openLeveldb(setting.NewNativeDbPath, false)
	defer newNativeDB.Close()

	//os.Remove(setting.TxCategoriesCsv)
	//var txCategoriesCSV = file.NewWriteCSV(setting.TxCategoriesCsv)
	//defer txCategoriesCSV.Close()
	//txCategoriesCSV.Write(&[]string{"block number", "txs sum", "ordinary transfer", "erc transfer", "erc approve", "internal num", "other", "optimisable ratio", "other ratio"})
	//
	//for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
	//	txs, _ := pureData.GetTransactionsByNumber(blockDB, new(big.Int).SetInt64(int64(number)))
	//	txs = txs[:len(txs)-1]
	//	if len(txs) == 0 {
	//		continue
	//	}
	//	ordinaryTransferNum, ercTransferNum, ercApproveNum, internalTxNum, other := 0, 0, 0, 0, 0
	//	for _, tx := range txs {
	//		// 普通转账
	//		if !tx.Contract {
	//			ordinaryTransferNum += 1
	//			continue
	//		}
	//		// 合约创建 或者是
	//		if tx.Contract && tx.To == nil {
	//			other += 1
	//			if len(tx.Input) < 4 {
	//				fmt.Println(tx.Hash)
	//			}
	//			continue
	//		}
	//		// 产生内部交易 (无参数合约调用)
	//		if tx.Contract && len(tx.Input) < 4 {
	//			internalTxNum += 1
	//			//fmt.Println(tx.Hash)
	//			continue
	//		}
	//
	//		if tx.Contract && common.Bytes2Hex(tx.Input[:4]) == setting.TransferKey {
	//			ercTransferNum += 1
	//			continue
	//		}
	//		if tx.Contract && common.Bytes2Hex(tx.Input[:4]) == setting.ApproveKey {
	//			ercApproveNum += 1
	//			continue
	//		}
	//		other += 1 // 其他合约
	//	}
	//	//fmt.Println(len(txs), ordinaryTransferNum, ercTransferNum, ercApproveNum, internalTxNum, other)
	//	otherRatio := (float64(other) / float64(len(txs))) * 100
	//	wStr := []string{(txs)[0].BlockNumber.String(), fmt.Sprintf("%d", len(txs)),
	//		fmt.Sprintf("%d", ordinaryTransferNum), fmt.Sprintf("%d", ercTransferNum),
	//		fmt.Sprintf("%d", ercApproveNum), fmt.Sprintf("%d", internalTxNum),
	//		fmt.Sprintf("%d", other), fmt.Sprintf("%.2f", 100-otherRatio),
	//		fmt.Sprintf("%.2f", otherRatio)}
	//	txCategoriesCSV.Write(&wStr)
	//
	//	//fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d, %s\n", number, wStr)
	//	fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d\n", number)
	//}

	funcBytes, _ := contractDB.Get([]byte(setting.SortFunctionsKey), nil)
	callSum := 0
	funcs := make([]*Function, 0)
	json.Unmarshal(funcBytes, &funcs)
	for _, fu := range funcs {
		callSum += fu.CallNum
	}

	funcs = funcs[:100]
	for _, fu := range funcs {
		fmt.Println(fu.Sign, fu.Name, fu.CallNum, float64(fu.CallNum)/float64(callSum)*100+0.005)
	}

	//contractBytes, _ := contractDB.Get([]byte(setting.SortContractsKey), nil)
	//contracts := make([]*Contract, 0)
	//json.Unmarshal(contractBytes, &contracts)
	//contracts = contracts[:100]
	//for _, c := range contracts {
	//	fmt.Println(c.Address, c.CallNum, len(c.Functions))
	//}
}
