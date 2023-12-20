package conflict_detection

import (
	"chukonu/concurrency_control/conflict/nezha/core/state"
	"chukonu/config"
	"chukonu/conflict_detection/classic"
	"chukonu/core"
	corestate "chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	testTxsLen = 10000
	compareLen = 10000
)

func DetectionOverhead() {
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
		parent     *types.Header      = blockStable.Header()
		stateCache corestate.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
	)

	chuKoNuStateDB, err := corestate.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
	if err != nil {
		fmt.Println(err)
	}

	chuKuNoProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)
	var (
		txsLen                                                 = 0
		txs                                                    = make(types.Transactions, 0)
		accessAddrNormal             []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
		count                                                  = 0
		all1, all2, all3, all4, all5 time.Duration             = 0, 0, 0, 0, 0
	)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020002), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		_, normal, _, _, _, _ := chuKuNoProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
		accessAddrNormal = append(accessAddrNormal, *normal...)

		txs = append(txs, block.Transactions()...)

		txsLen += block.Transactions().Len()
		if txsLen >= testTxsLen { // 对比 testTxsLen 个交易
			txs = txs[:compareLen]
			t1 := dmvccDetectionOverhead(txs, compareLen)
			t2 := chukonuDetectionOverhead(txs)

			os.RemoveAll(setting.NezhaDB)
			statedb, _ := state.NewState(setting.NezhaDB, nil)
			t3 := nezhaDetection(txs, statedb)

			t4 := classic.ClassicDetectionOverhead(txs)

			t5 := coarseDetectionOverhead(txs, compareLen)

			all1 += t1
			all2 += t2
			all3 += t3
			all4 += t4
			all5 += t5
			root, err := chuKoNuStateDB.Commit(true)
			if err != nil {
				fmt.Println("state db commit error", err)
				return
			}
			chuKoNuStateDB, _ = corestate.NewChuKoNuStateDB(root, stateCache, nil, nil)
			txsLen = 0
			txs = txs[:0]
			accessAddrNormal = accessAddrNormal[:0]
			count += 1
			if count == 10 {
				fmt.Println("DMVCC (Fine-grained) Detection Overhead:", all1/10)
				fmt.Println("ChuKoNu (two-tier) Detection Overhead:", all2/10)
				fmt.Println("Nezha Detection Overhead:", all3/10)
				fmt.Println("Classic Dependency Graph Detection Overhead:", all4/10)
				fmt.Println("Coarse-grained Detection Overhead:", all5/10)
				break
			}
		}
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
}

func coarseDetectionOverhead(txs types.Transactions, testSpan int) time.Duration {
	start := time.Now()
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[common.Address][]*transaction.Transaction)
	accessToken := make(map[common.Address]bool)
	for i, tx := range txs {
		needAddress[i] = len(*tx.AccessPre)
		for addr, accessAddress := range *tx.AccessPre {
			if _, ok := accessSequence[addr]; !ok {
				accessSequence[addr] = make([]*transaction.Transaction, 0)
				accessToken[addr] = false
			}
			if !accessToken[addr] {
				getAddress[i] += 1
				if accessAddress.CoarseWrite {
					accessToken[addr] = true
				}
			}
		}
	}
	p := 0
	for i, num := range needAddress {
		if num == getAddress[i] {
			p += 1
		}
	}
	return time.Since(start)
}

func dmvccDetectionOverhead(txs types.Transactions, testSpan int) time.Duration {
	start := time.Now()
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[string][]*transaction.Transaction)
	accessToken := make(map[string]bool)
	for i, tx := range txs {
		for addr, accessAddress := range *tx.AccessPre {
			if accessAddress.IsRead || accessAddress.IsWrite { // 读/写了账户状态

				needAddress[i] += 1
				if _, ok := accessSequence[addr.Hex()]; !ok {
					accessSequence[addr.Hex()] = make([]*transaction.Transaction, 0)
					accessToken[addr.Hex()] = false
				}
				if !accessToken[addr.Hex()] {
					getAddress[i] += 1
					if accessAddress.IsWrite {
						accessToken[addr.Hex()] = true
					}
				}
			} else {

			}

			for slot, accessSlot := range *accessAddress.Slots {
				needAddress[i] += 1
				slotKey := addr.Hex() + "_" + slot.Hex()
				if _, ok := accessSequence[slotKey]; !ok {
					accessSequence[slotKey] = make([]*transaction.Transaction, 0)
					accessToken[slotKey] = false
				}
				if !accessToken[slotKey] {
					getAddress[i] += 1
					if accessSlot.IsWrite {
						accessToken[slotKey] = true
					}
				}
			}
		}
	}
	p := 0
	for i, num := range needAddress {
		if num == getAddress[i] {
			p += 1
		}
	}
	return time.Since(start)
}

type account struct {
	address       common.Address
	sequence      types.Transactions
	slotSequences map[common.Hash]types.Transactions
}

func newAccount(address common.Address) *account {
	return &account{
		address:       address,
		sequence:      make(types.Transactions, 0),
		slotSequences: make(map[common.Hash]types.Transactions),
	}
}
func (a *account) appendSequence(tx *types.Transaction) {
	a.sequence = append(a.sequence, tx)
}

func chukonuDetectionOverhead(txs types.Transactions) time.Duration {
	start := time.Now()
	accessSequence := make(map[common.Address]*account)

	for _, tx := range txs {
		for addr, accessAddress := range *tx.AccessPre {
			if accessAddress.CoarseRead || accessAddress.CoarseWrite { // 读/写了账户状态
				if _, ok := accessSequence[addr]; !ok {
					accessSequence[addr] = newAccount(addr)
				}
				accessSequence[addr].appendSequence(tx)
			}
		}
	}
	type txsByAddr struct {
		addr    common.Address
		accInfo *account
		num     int
	}
	var listTxsByAddr []txsByAddr
	for key, acc := range accessSequence {
		listTxsByAddr = append(listTxsByAddr, txsByAddr{key, acc, len(acc.sequence)})
	}
	sort.Slice(listTxsByAddr, func(i, j int) bool {
		return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	})

	var (
		slotConflictDetectionWg  sync.WaitGroup
		slotConflictDetectionNum = runtime.NumCPU()
		accountInfoCh            = make(chan *account, 10000)
	)
	slotConflictDetectionWg.Add(slotConflictDetectionNum)
	for i := 0; i < slotConflictDetectionNum; i++ {
		go slotConflictDetection(accountInfoCh, &slotConflictDetectionWg)
	}
	for _, val := range listTxsByAddr {
		accountInfoCh <- val.accInfo
	}
	close(accountInfoCh)
	slotConflictDetectionWg.Wait()

	return time.Since(start)
}

func slotConflictDetection(accountInfoCh chan *account, wg *sync.WaitGroup) {
	defer wg.Done()
	for accInfo := range accountInfoCh {
		// 	地址对应的队列
		for _, tx := range accInfo.sequence {
			addressRW := (*tx.AccessPre)[accInfo.address]

			if len(*addressRW.Slots) == 0 { // 如果没有访问Slot, 则下一个事务
				continue
			}

			// 有slot的访问
			for slot, _ := range *addressRW.Slots {
				if _, ok := accInfo.slotSequences[slot]; !ok {
					accInfo.slotSequences[slot] = make(types.Transactions, 0)
				}
				accInfo.slotSequences[slot] = append(accInfo.slotSequences[slot], tx)
			}
		}
	}
}
