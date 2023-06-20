package abi_get

import (
	"chukonu/setting"
	"encoding/json"
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/syndtr/goleveldb/leveldb"
	"sort"
	"strings"
)

type Function struct {
	Sign    string
	CallNum int
	Name    string
}

func newFunction(sign string, name string) *Function {
	return &Function{Sign: sign, CallNum: 0, Name: name}
}

type Contract struct {
	Address   common.Address
	CallNum   int
	ABI       string
	Functions []*Function //(funcHash, function)
}

func newContract(addr common.Address, callNum int, abi string) *Contract {
	return &Contract{
		Address:   addr,
		CallNum:   callNum,
		ABI:       abi,
		Functions: make([]*Function, 0),
	}
}

func (c *Contract) addFunction(sign, name string) {
	var newF *Function = nil
	for _, f := range c.Functions { // 检查func是否已经写入
		if f.Sign == sign {
			newF = f
			break
		}
	}
	if newF == nil { // func未写入
		newF = newFunction(sign, name)
		c.Functions = append(c.Functions, newF)
	}
	newF.CallNum += 1
}

func AddContractFuncNameToTx(txs []*transaction.Transaction, contractDB *leveldb.DB, contractMap *map[common.Address]*Contract, funcSlice *[]*Function) string {
	var txsStr = ""
	for _, tx := range txs {
		if tx.Contract && tx.To != nil { // 如果是智能合约操作，且不为合约创建
			getContractFuncName(tx, contractDB, contractMap, funcSlice)
		}
	}
	for _, tx := range txs {
		txsStr = txsStr + tx.String() + ";"
		//fmt.Println()
	}
	return txsStr
}

func getContractFuncName(tx *transaction.Transaction, contractDB *leveldb.DB, contractMap *map[common.Address]*Contract, funcSlice *[]*Function) {
	var (
		contract *Contract = nil
		//funcName string    = ""
	)
	if _, ok := (*contractMap)[*tx.To]; ok {
		contract = (*contractMap)[*tx.To]
	} else {
		abi, err := contractDB.Get([]byte(tx.To.String()), nil)
		if err != nil {
			fmt.Println("read contract abi error,", err, tx.To)
			return
		}
		contract = newContract(*tx.To, 0, string(abi))
		(*contractMap)[*tx.To] = contract
	}
	contract.CallNum += 1

	// 签名不正确 (此处无需知道相应的ABI，因为即使知道，签名不正确也无法获取)
	if len(tx.Input) < 4 {
		tx.ContractFuncName = ""
		contract.addFunction(common.Bytes2Hex(tx.Input), "")
		addFunction(funcSlice, common.Bytes2Hex(tx.Input), "")
		return
	}

	// 签名正确都是没有相应的ABI, 也是无法解析函数名
	if len(contract.ABI) == 0 {
		tx.ContractFuncName = ""
		contract.addFunction(common.Bytes2Hex(tx.Input[:4]), "")
		addFunction(funcSlice, common.Bytes2Hex(tx.Input[:4]), "")
		return
	}

	contractAbi, err := abi.JSON(strings.NewReader(contract.ABI))
	if err != nil { // ABI解析出错，当成没有ABI的来处理
		fmt.Println("json contract abi error,", err, contract.Address, tx.Hash)

		tx.ContractFuncName = ""
		contract.addFunction(common.Bytes2Hex(tx.Input[:4]), "")
		addFunction(funcSlice, common.Bytes2Hex(tx.Input[:4]), "")
		return
	}
	method, err := contractAbi.MethodById(tx.Input[:4])
	if err != nil && (err.Error())[:17] != "no method with id" {
		tx.ContractFuncName = ""
		contract.addFunction(common.Bytes2Hex(tx.Input[:4]), "")
		addFunction(funcSlice, common.Bytes2Hex(tx.Input[:4]), "")
		fmt.Println(tx.Hash, err)
	} else if err != nil { // 此时的error对应于没有相应的方法 (no method with id), 无需报错
		tx.ContractFuncName = ""
		contract.addFunction(common.Bytes2Hex(tx.Input[:4]), "")
		addFunction(funcSlice, common.Bytes2Hex(tx.Input[:4]), "")
	} else {
		tx.ContractFuncName = method.Sig
		contract.addFunction(common.Bytes2Hex(tx.Input[:4]), method.Sig)
		addFunction(funcSlice, common.Bytes2Hex(tx.Input[:4]), method.Sig)
		//fmt.Println(method.Name, method.Sig, common.Bytes2Hex(method.ID), common.Bytes2Hex(tx.Input[:4]))
	}
}

func addFunction(slice *[]*Function, sign, name string) {
	var newF *Function = nil
	for _, f := range *slice { // 检查func是否已经写入
		if f.Sign == sign {
			newF = f
			break
		}
	}
	if newF == nil { // func未写入
		newF = newFunction(sign, name)
		*slice = append(*slice, newF)
	}
	// 同一个函数签名的Name可能解析得多个，但我们暂时只取一个
	if name != "" && newF.Name == "" {
		newF.Name = name
	}
	newF.CallNum += 1
}

func SaveSortFunctions(functions *[]*Function, contractDB *leveldb.DB) {
	sort.Slice(*functions, func(i, j int) bool {
		return (*functions)[i].CallNum > (*functions)[j].CallNum // 降序
	})
	funcBytes, err := json.Marshal(functions)
	if err != nil {
		fmt.Println("json functions error,", err)
	} else {
		err1 := contractDB.Put([]byte(setting.SortFunctionsKey), funcBytes, nil)
		if err1 != nil {
			fmt.Println("storing sort functions error,", err1)
		}
		//f := make([]*Function, 0)
		//json.Unmarshal(funcBytes, &f)
		//for i, fu := range *functions {
		//	fmt.Println(fu.Sign, fu.Name, fu.CallNum)
		//	fmt.Println(f[i].Sign, f[i].Name, f[i].CallNum)
		//}
	}
}

func SaveSortContracts(contractMap *map[common.Address]*Contract, contractDB *leveldb.DB) {
	contracts := make([]*Contract, 0)
	for _, c := range *contractMap {
		contracts = append(contracts, c)
		sort.Slice(c.Functions, func(i, j int) bool {
			return c.Functions[i].CallNum > c.Functions[j].CallNum // 降序
		})
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].CallNum > contracts[j].CallNum // 降序
	})

	contractBytes, err := json.Marshal(contracts)
	if err != nil {
		fmt.Println("json contracts error,", err)
	} else {
		err1 := contractDB.Put([]byte(setting.SortContractsKey), contractBytes, nil)
		if err1 != nil {
			fmt.Println("storing contracts error,", err1)
		}

		//con := make([]*Contract, 0)
		//json.Unmarshal(contractBytes, &con)
		//fmt.Println(len(contracts), len(con))
		//for i, c := range contracts {
		//	fmt.Println(c.Address, c.CallNum, len(c.Functions))
		//	fmt.Println(con[i].Address, con[i].CallNum, len(con[i].Functions))
		//	fmt.Println()
		//}

		//fmt.Println(len(contracts), len(contractBytes))
	}
}
