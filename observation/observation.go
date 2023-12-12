package observation

import (
	"chukonu/observation/classic"
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"time"
)

func parallelism(txs []*transaction.Transaction, testSpan int) {
	p1 := coarseParallelism(txs, testSpan)
	p2 := fineParallelism(txs, testSpan)
	fmt.Println(p1, p2)
}

func coarseParallelism(txs []*transaction.Transaction, testSpan int) int {
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[common.Address][]*transaction.Transaction)
	accessToken := make(map[common.Address]bool)
	for i, tx := range txs {
		needAddress[i] = len(*tx.AccessAddress)
		for addr, accessAddress := range *tx.AccessAddress {
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
	return p
}

func fineParallelism(txs []*transaction.Transaction, testSpan int) int {
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[string][]*transaction.Transaction)
	accessToken := make(map[string]bool)
	for i, tx := range txs {
		for addr, accessAddress := range *tx.AccessAddress {
			if accessAddress.IsRead || accessAddress.IsWrite { // 读/写了账户状态
				if *tx.Hash == common.HexToHash("0xc2f8da59f37c506e4d398ca32e2c70baa0f522e76d2b6f83fa3fd20491888e67") {
					fmt.Println(tx.Hash, addr, tx.Contract, accessAddress.IsRead, accessAddress.IsWrite)
				}

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
	return p
}

func detectionOverhead(txs []*transaction.Transaction, testSpan int, num int) (time.Duration, time.Duration, time.Duration) {
	t1 := coarseDetectionOverhead(txs, testSpan)
	t2 := fineDetectionOverhead(txs, testSpan)
	t3 := classic.ClassicDetectionOverhead(txs)
	fmt.Println(num, t1.Microseconds(), t2.Microseconds(), t3.Microseconds())
	return t1, t2, t3
}

func coarseDetectionOverhead(txs []*transaction.Transaction, testSpan int) time.Duration {
	start := time.Now()
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[common.Address][]*transaction.Transaction)
	accessToken := make(map[common.Address]bool)
	for i, tx := range txs {
		needAddress[i] = len(*tx.AccessAddress)
		for addr, accessAddress := range *tx.AccessAddress {
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

func fineDetectionOverhead(txs []*transaction.Transaction, testSpan int) time.Duration {
	start := time.Now()
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[string][]*transaction.Transaction)
	accessToken := make(map[string]bool)
	for i, tx := range txs {
		for addr, accessAddress := range *tx.AccessAddress {
			if accessAddress.IsRead || accessAddress.IsWrite { // 读/写了账户状态
				//if *tx.Hash == common.HexToHash("0xc2f8da59f37c506e4d398ca32e2c70baa0f522e76d2b6f83fa3fd20491888e67") {
				//	fmt.Println(tx.Hash, addr, tx.Contract, accessAddress.IsRead, accessAddress.IsWrite)
				//}

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
