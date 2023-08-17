package chukonu

import (
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"sort"
	"sync"
	"time"
)

func ChuKoNu(txs []*transaction.Transaction) float64 {
	ctxs := make(chukonuTxs, 0)
	for _, tx := range txs {
		ctx := newChuKoNuTx(tx)
		ctx.generateReadAndWrite(tx)
		ctxs = append(ctxs, ctx)
	}

	var (
		disNum  = 2
		execNum = 6
		//num      = runtime.NumCPU()
		checkNum = 2
		disWg    sync.WaitGroup
		disCh    = make(distributeChan, chanSize)

		checkWg sync.WaitGroup
		//checkCh  = make(checkChan, chanSize)
		checkCh = make([]checkChan, checkNum)

		execWg sync.WaitGroup
		execCh = make(executionChan, chanSize)

		closeWg sync.WaitGroup
		closeCh = make(closeChan, chanSize/2)
	)
	startTime := time.Now()
	disWg.Add(disNum)
	for i := 0; i < disNum; i++ {
		go chukonuDistributeTxs(disCh, &checkCh, &disWg, checkNum)
	}

	checkWg.Add(checkNum) //并发: Cpu核数=并发线程数
	for i := 0; i < checkNum; i++ {
		checkCh[i] = make(checkChan, chanSize/checkNum+1)
		go chukonuCheckTxs(checkCh[i], execCh, &checkWg)
	}

	execWg.Add(execNum)
	for i := 0; i < execNum; i++ {
		go chukonuExecutionTxs(execCh, disCh, closeCh, &execWg)
	}

	queueLen := chukonuConstructionOrder(&ctxs, disCh) // 依赖队列的长度

	closeWg.Add(1)
	go chukonuCloseChuKoNu(closeCh, disCh, &checkCh, execCh, queueLen, &closeWg, checkNum)

	closeWg.Wait()
	disWg.Wait()
	checkWg.Wait()
	execWg.Wait()
	return float64(len(txs)) / time.Since(startTime).Seconds()
}

func chukonuConstructionOrder(ctxs *chukonuTxs, disCh distributeChan) int {
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
		list := seQueue[val.addr]
		disCh <- &depQueue{depAddr: val.addr, pendingTxs: &list, canExec: false, index: 0, len: len(list)}
	}
	return len(listTxsByAddr)

	//for addr, _ := range seQueue {
	//	list := seQueue[addr]
	//	disCh <- &depQueue{depAddr: addr, pendingTxs: &list, canExec: false, index: 0, len: len(list)}
	//}
	//return len(seQueue)
}

func chukonuDistributeTxs(disCh distributeChan, checkCh *[]checkChan, wg *sync.WaitGroup, num int) {
	defer wg.Done()
	for queue := range disCh {
		tx := (*queue.pendingTxs)[queue.index] // queue.index 当前可执行的交易下标，递增

		queue.canExec = true              // 合约地址依赖都为可执行
		(*checkCh)[tx.index%num] <- queue // 发送给对应的检查线程，根据tx.index进行选择
	}
}

func chukonuCheckTxs(checkCh checkChan, execCh executionChan, wg *sync.WaitGroup) {
	wg.Done()
	queueByAddr := make(map[common.Hash][]*depQueue)
	for queue := range checkCh {
		tx := (*queue.pendingTxs)[queue.index]
		var depQ []*depQueue
		if _, ok := queueByAddr[tx.hash]; !ok { // 第一次接收到该交易的依赖
			depQ = make([]*depQueue, 0)
		} else {
			depQ = queueByAddr[tx.hash]
		}
		depQ = append(depQ, queue)
		if len(depQ) == tx.accessSum { // 可以执行
			execCh <- &depQ
			delete(queueByAddr, tx.hash)
		} else { // 还不能执行
			queueByAddr[tx.hash] = depQ
		}
	}
}

func chukonuExecutionTxs(execCh executionChan, disCh distributeChan, closeCh closeChan, wg *sync.WaitGroup) {
	wg.Done()
	for depQ := range execCh {
		q0 := (*depQ)[0]
		tx := (*q0.pendingTxs)[q0.index]

		//time.Sleep(tx.tx.ExecutionTime)

		if tx.isOpt {
			time.Sleep(tx.optExecutedTime)
		} else {
			time.Sleep(tx.tx.ExecutionTime)
		}

		// 执行完成，处理剩余的待处理队列
		for _, q := range *depQ {
			q.index += 1
			if q.index < q.len { // 还有事务需要执行
				//fmt.Println("q.index < q.len:", q.index, q.len)
				disCh <- q
			} else {
				//fmt.Println("q.index >= q.len:", q.index, q.len)
				closeCh <- true
			}
		}
	}
}

func chukonuCloseChuKoNu(closeCh closeChan, disCh distributeChan, checkCh *[]checkChan, execCh executionChan, queueLen int, wg *sync.WaitGroup, num int) {
	defer wg.Done()
	var finishNum = 0
	for _ = range closeCh {
		finishNum += 1
		if finishNum == queueLen {
			close(closeCh)
			close(disCh)
			for i := 0; i < num; i++ {
				close((*checkCh)[i])
			}
			close(execCh)
		}
	}
}
