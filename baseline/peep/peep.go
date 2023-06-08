package peep

import (
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"math/rand"
	"runtime"
	"sync"
	"time"
)

const chanSize = 2048

var ProNum = 8

type distributeMap map[common.Address]peepTxs
type distributeChan chan *txQueueByAddr
type checkChan chan *txQueueByAddr
type executionChan chan *[]*txQueueByAddr
type closeChan chan bool

type txQueueByAddr struct {
	depAddr    common.Address //所依赖的地址
	pendingTxs *peepTxs       // 该地址等待执行的队列
	canExec    bool           // 队首依赖是否可以执行
	index      int            // 当前执行的交易在pending中的下标
	len        int            // pending的总长度
}

func Peep(txs []*transaction.Transaction) float64 {
	ptxs := make(peepTxs, 0)
	for _, tx := range txs {
		ptx := newPeepTx(tx)
		ptx.generateReadAndWrite(tx)
		ptxs = append(ptxs, ptx)
	}
	startTime := time.Now()
	var (
		disWg sync.WaitGroup
		disCh = make(distributeChan, chanSize)

		checkWg sync.WaitGroup
		checkCh = make(checkChan, chanSize)

		execWg sync.WaitGroup
		execCh = make(executionChan, chanSize)

		closeWg sync.WaitGroup
		closeCh = make(closeChan, chanSize)
	)
	runtime.GOMAXPROCS(ProNum)
	addressLen := constructionOrder(disCh, &ptxs)

	disNum := ProNum / 8 * 2
	disWg.Add(disNum)
	for i := 0; i < disNum; i++ {
		go distributeTxs(disCh, checkCh, &disWg)
	}

	checkWg.Add(1)
	go checkTxs(checkCh, execCh, &checkWg)

	execNum := ProNum - disNum
	execWg.Add(execNum)
	for i := 0; i < execNum; i++ {
		go executionTxs(execCh, disCh, closeCh, &execWg)
	}

	closeWg.Add(1)
	go func(closeCh closeChan, disCh distributeChan, checkCh checkChan, execCh executionChan, addressLen int, wg *sync.WaitGroup) {
		defer wg.Done()
		var finishNum = 0
		for _ = range closeCh {
			finishNum += 1
			if finishNum == addressLen {
				close(closeCh)
				close(disCh)
				close(checkCh)
				close(execCh)
			}
		}
	}(closeCh, disCh, checkCh, execCh, addressLen, &closeWg)

	closeWg.Wait()
	disWg.Wait()
	checkWg.Wait()
	execWg.Wait()

	return float64(len(txs)) / time.Since(startTime).Seconds()
}

func constructionOrder(disCh distributeChan, ptxs *peepTxs) int {

	// 地址所对应的热交易, 热交易执行前准备
	addrMapPtxs := make(distributeMap, 0)
	for _, ptx := range *ptxs {
		for addr, _ := range ptx.allAddress {
			if _, ok := addrMapPtxs[addr]; !ok {
				addrMapPtxs[addr] = make(peepTxs, 0)
			}
			addrMapPtxs[addr] = append(addrMapPtxs[addr], ptx)
		}
	}

	//type txsByAddr struct {
	//	addr common.Address
	//	num  int
	//}
	//
	//var listTxsByAddr []txsByAddr
	//for key, vch := range addrMapPtxs {
	//	listTxsByAddr = append(listTxsByAddr, txsByAddr{key, len(vch)})
	//}
	//
	//sort.Slice(listTxsByAddr, func(i, j int) bool {
	//	return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	//})
	//
	//for _, val := range listTxsByAddr {
	//	list := addrMapPtxs[val.addr]
	//	disCh <- &txQueueByAddr{depAddr: val.addr, pendingTxs: &list, canExec: false, index: 0, len: len(list)}
	//}
	//return len(listTxsByAddr)

	for key, _ := range addrMapPtxs {
		list := addrMapPtxs[key]
		disCh <- &txQueueByAddr{depAddr: key, pendingTxs: &list, canExec: false, index: 0, len: len(list)}
	}
	return len(addrMapPtxs)
}

func distributeTxs(disCh distributeChan, checkCh checkChan, wg *sync.WaitGroup) {
	defer wg.Done()
	for queue := range disCh {
		//time.Sleep(time.Duration(int64(float64(time.Microsecond) * (rand.Float64() + 0.5))))
		//time.Sleep(time.Duration(200 + rand.Int63n(800))) // 悲观锁
		time.Sleep(22 * time.Microsecond) // 悲观锁
		//time.Sleep(time.Nanosecond * 800)
		queue.canExec = true // 皆为可执行
		checkCh <- queue     // 都需要check
	}
}

func checkTxs(checkCh checkChan, execCh executionChan, wg *sync.WaitGroup) {
	defer wg.Done()
	queueByAddr := make(map[common.Hash][]*txQueueByAddr)
	for queue := range checkCh {
		tx := (*queue.pendingTxs)[queue.index]
		var depQ []*txQueueByAddr
		if _, ok := queueByAddr[tx.hash]; !ok { // 第一次接收到该交易的依赖
			depQ = make([]*txQueueByAddr, 0)
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

func executionTxs(execCh executionChan, disCh distributeChan, closeCh closeChan, wg *sync.WaitGroup) {
	defer wg.Done()
	for depQ := range execCh {
		q0 := (*depQ)[0]
		tx := (*q0.pendingTxs)[q0.index]

		time.Sleep(tx.tx.ExecutionTime)

		// 执行完成，处理剩余的待处理队列
		for _, q := range *depQ {
			q.canExec = false
			q.index += 1
			if q.index < q.len { // 还有事务需要执行
				disCh <- q
			} else {
				closeCh <- true
			}
		}
	}

}

func init() {
	rand.Seed(time.Now().UnixNano())
}
