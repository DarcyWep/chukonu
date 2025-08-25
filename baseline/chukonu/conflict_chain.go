package chukonu

import (
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"sort"
)

func ConflictChain(txs []*transaction.Transaction) {
	ctxs := make(chukonuTxs, 0)
	for _, tx := range txs {
		ctx := newChuKoNuTx(tx)
		ctx.generateReadAndWrite(tx)
		ctxs = append(ctxs, ctx)
	}
	conflictChain(&ctxs) // 依赖队列的长度
}

func conflictChain(ctxs *chukonuTxs) {
	seQueue := make(distributeMap)

	// 	地址对应的队列
	for _, ctx := range *ctxs {
		for addr, _ := range ctx.allAddress {
			if _, ok := seQueue[addr]; !ok {
				seQueue[addr] = make(chukonuTxs, 0)
			}
			seQueue[addr] = append(seQueue[addr], ctx)
		}
	}

	type txsByAddr struct {
		addr common.Address
		num  int
	}

	var listTxsByAddr []txsByAddr
	for key, vch := range seQueue {
		listTxsByAddr = append(listTxsByAddr, txsByAddr{key, len(vch)})
	}

	sort.Slice(listTxsByAddr, func(i, j int) bool {
		return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	})

	for _, val := range listTxsByAddr {
		fmt.Println(val.addr, val.num)
		for _, tx := range seQueue[val.addr] {
			fmt.Println(tx.hash)
		}
		fmt.Println()
	}
}
