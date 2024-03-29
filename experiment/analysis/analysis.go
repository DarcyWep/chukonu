package analysis

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"sort"
	"time"
)

type (
	AddressesExistMap map[common.Address]struct{}
	AddressMapTxs     map[common.Address]types.Transactions
	TxContainAddress  map[common.Hash][]common.Address
	HashExistMap      map[common.Hash]struct{}
	MinerFee          map[common.Address]*big.Int
)

type AddrRelatedTxNum struct {
	Addr  common.Address
	TxNum int
	IsHot bool
}

func ContractRatio() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, database.DefaultRawConfig())
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 14000000
	blockStable, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockStable.Header()
		preRoot    common.Hash    = parent.Root
		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
	)

	chuKoNuStateDB, err := state.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
	if err != nil {
		fmt.Println(err)
	}
	chuKoNuProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)
	var (
		txsLen     = 0
		txs        = make(types.Transactions, 0)
		count      = 0
		compareLen = 64 * 200

		hotTxSum          = 0
		hotContractTxSum  = 0
		clodTxSum         = 0
		clodContractTxSum = 0
		allAddressNum     = 0
		hotAddressNum     = 0
		pureHotAddressNum = 0
		coldAddressNum    = 0
	)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020002), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB.Copy(), vm.Config{EnablePreimageRecording: false}, false)
		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false}, false)

		for _, tx := range block.Transactions() {
			txs = append(txs, tx)
		}
		txsLen += block.Transactions().Len() // 多一个矿工奖励交易
		if txsLen >= compareLen {            // 对比 testTxsLen 个交易
			root, _ := chuKoNuStateDB.Commit(true)                            // 用以保证后续执行的正确性
			chuKoNuStateDB.Database().TrieDB().Reference(root, common.Hash{}) // metadata reference to keep trie alive
			chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
			chuKoNuStateDB.Database().TrieDB().Dereference(preRoot)
			preRoot = root

			txs = txs[:compareLen]
			hotTxs, clodTxs, allAddress, hotAddress, pureHotAddress, coldAddress := AddressTxsAnalysis(txs)
			hotTxSum += len(hotTxs)
			clodTxSum += len(clodTxs)
			allAddressNum += len(allAddress)
			hotAddressNum += len(hotAddress)
			pureHotAddressNum += len(pureHotAddress)
			coldAddressNum += len(coldAddress)

			for _, tx := range hotTxs {
				if tx.To() == nil {
					hotContractTxSum += 1
				} else if len(tx.Data()) != 0 {
					//if string(tx.Data()) == "0x" {
					//	fmt.Println(tx.Data())
					//}
					hotContractTxSum += 1
				}
			}
			for _, tx := range clodTxs {
				if tx.To() == nil {
					clodContractTxSum += 1
				} else if len(tx.Data()) != 0 {
					//if string(tx.Data()) == "0x" {
					//	fmt.Println(tx.Data())
					//}
					//fmt.Println(common.BytesToHash(tx.Data()), len(tx.Data()))
					clodContractTxSum += 1
				}
			}
			count += 1
			if count == 10 {
				txSum := hotTxSum + clodTxSum
				fmt.Println("Contract Ratio in Hot Txs:", float64(hotContractTxSum)/float64(txSum)*100, hotContractTxSum, hotTxSum)
				fmt.Println("Contract Ratio in Clod Txs:", float64(clodContractTxSum)/float64(txSum)*100, clodContractTxSum, clodTxSum)
				fmt.Println("Pure Hot Address Ratio:", float64(pureHotAddressNum)/float64(allAddressNum)*100, pureHotAddressNum, allAddressNum)
				fmt.Println("Hot Address Ratio:", float64(hotAddressNum-pureHotAddressNum)/float64(allAddressNum)*100, hotAddressNum-pureHotAddressNum, allAddressNum)
				fmt.Println("Clod Address Ratio:", float64(coldAddressNum)/float64(allAddressNum)*100, coldAddressNum, allAddressNum)
				break
			}
		}
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
}

func AddressTxsAnalysis(txs types.Transactions) (types.Transactions, types.Transactions, AddressesExistMap, AddressesExistMap, AddressesExistMap, AddressesExistMap) {
	var addrMapTxs = make(AddressMapTxs) // 地址所对应的所有交易, 用于计算热交易
	for _, tx := range txs {
		for addr, _ := range *tx.AccessPre {
			if _, ok := addrMapTxs[addr]; !ok {
				addrMapTxs[addr] = make(types.Transactions, 0)
			}
			addrMapTxs[addr] = append(addrMapTxs[addr], tx)
		}
	}

	hotTxsHash, pureHotAddress := getHotTxsAndRelatedAddress(addrMapTxs, len(txs))
	hotTxs, clodTxs := analysisTransactions(hotTxsHash, txs)
	var (
		allAddress  = make(AddressesExistMap) // 所有地址
		hotAddress  = make(AddressesExistMap) // 所有地址
		coldAddress = make(AddressesExistMap) // 所有地址
	)
	for _, tx := range hotTxs {
		for addr, _ := range *tx.AccessPre {
			if _, ok := hotAddress[addr]; !ok {
				hotAddress[addr] = struct{}{}
				allAddress[addr] = struct{}{}
			}
		}
	}
	for _, tx := range clodTxs {
		for addr, _ := range *tx.AccessPre {
			if _, ok := allAddress[addr]; !ok {
				coldAddress[addr] = struct{}{}
				allAddress[addr] = struct{}{}
			}
		}
	}
	return hotTxs, clodTxs, allAddress, hotAddress, pureHotAddress, coldAddress
}

// getHotTxs 提取热门交易相关信息, 热门交易达到80%结束
// 返回热账户交易所涉及的所有地址
func getHotTxsAndRelatedAddress(addrMapTxs AddressMapTxs, txSum int) (HashExistMap, AddressesExistMap) {
	listAddr := sortAddr(addrMapTxs)
	var (
		hotTxsHash   = make(HashExistMap)      // 已经被判断为热门交易的交易
		pureHotAddrs = make(AddressesExistMap) // 纯热地址
	)
	for _, val := range listAddr {
		if val.TxNum == 2 {
			break
		}
		val.IsHot = true
		addr := val.Addr

		if _, ok := pureHotAddrs[addr]; !ok { // 存入纯热账户
			pureHotAddrs[addr] = struct{}{} // 存入热点账户中
		}

		txs := addrMapTxs[addr]
		for _, tx := range txs {
			if _, ok := hotTxsHash[tx.Hash()]; !ok { // 当前账户未存入热点
				hotTxsHash[tx.Hash()] = struct{}{} // 存入热点交易中
			}
		}
		txHotRate := float64(len(hotTxsHash)) * 100 / float64(txSum)
		if txHotRate > 80.0 {
			break
		}
	}

	//hotStateHotTx, clodStateHotTx, clodStateClodTx := float64(k)*100/float64(len(listAddr)), float64(len(hotAddrs)-k)*100/float64(len(listAddr)), float64(len(listAddr)-len(hotAddrs))*100/float64(len(listAddr))
	//hotTx, clodTx := float64(len(hotTxsHash))*100/float64(txSum), float64(txSum-len(hotTxsHash))*100/float64(txSum)
	//fmt.Println(fmt.Sprintf("%.2f", hotStateHotTx), fmt.Sprintf("%.2f", clodStateHotTx), fmt.Sprintf("%.2f", clodStateClodTx),
	//	fmt.Sprintf("%.2f", hotTx), fmt.Sprintf("%.2f", clodTx))
	//fmt.Println(k)
	//for key, val := range hotAddrs{
	//	fmt.Println(key, val)
	//}
	return hotTxsHash, pureHotAddrs
}

// Sort Addr By TxNum
func sortAddr(addrMapTxs AddressMapTxs) []*AddrRelatedTxNum {
	var listAddr []*AddrRelatedTxNum
	for addr, txs := range addrMapTxs {
		listAddr = append(listAddr, &AddrRelatedTxNum{addr, len(txs), false})
	}

	sort.Slice(listAddr, func(i, j int) bool {
		if listAddr[i].TxNum > listAddr[j].TxNum {
			return true // 降序
		}
		if listAddr[i].TxNum == listAddr[j].TxNum && listAddr[i].Addr.Hex() > listAddr[j].Addr.Hex() {
			return true
		}
		return false
	})
	//fmt.Println(len(listAddr))
	//for i, val := range listAddr {
	//	fmt.Println(i + 1, val.Addr, val.TxNum)
	//}
	return listAddr
}

func analysisTransactions(hotTxsHash HashExistMap, txs types.Transactions) (types.Transactions, types.Transactions) {
	var (
		hotTxs  = make(types.Transactions, 0) // 热事务(按序)
		clodTxs = make(types.Transactions, 0) // 冷事务(按序)
	)

	// 划分冷热交易
	for _, tx := range txs {
		if _, ok := hotTxsHash[tx.Hash()]; ok { // 交易为热交易
			hotTxs = append(hotTxs, tx)
		} else {
			clodTxs = append(clodTxs, tx)
		}
	}

	return hotTxs, clodTxs
}
