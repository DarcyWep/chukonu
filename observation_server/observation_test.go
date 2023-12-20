package observation_server

import (
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestParallelism(t *testing.T) {
	db, err := setting.OpenLeveldb("/Users/darcywep/Projects/GoProjects/chukonu/data/new_nativedb") // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}
	val, _ := db.Get([]byte("block.Coinbase"), nil)
	addresses := strings.Split(string(val), " ")
	coinbaseMap := make(map[string]string, 0)
	min, max, addSpan := big.NewInt(14000001), big.NewInt(14200001), big.NewInt(1)
	var k = 0
	for i := new(big.Int).Set(min); i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		coinbaseMap[i.String()] = addresses[k]
		k++
	}
	//for key, val := range coinbaseMap {
	//	fmt.Println(key, val)
	//}

	allTxs := make([]*transaction.Transaction, 0)
	k = 0
	times := 10000
	var allT1, allT2, allT3 time.Duration = 0, 0, 0
	testSpan := 500
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		txs, err := pureData.GetTransactionsByNumber(db, i)
		if err != nil {
			fmt.Println(err)
			return
		}
		txs = txs[:len(txs)-1]
		for _, tx := range txs {
			if tx.From.Hex() != coinbaseMap[i.String()] && tx.To != nil && tx.To.Hex() != coinbaseMap[i.String()] {
				delete(*tx.AccessAddress, common.HexToAddress(coinbaseMap[i.String()]))
			}
			allTxs = append(allTxs, tx)
		}
		if len(allTxs) >= testSpan {
			//parallelism(allTxs[:testSpan], testSpan)
			t1, t2, t3 := detectionOverhead(allTxs[:testSpan], testSpan, k)
			allT1, allT2, allT3 = allT1+t1, allT2+t2, allT3+t3
			k += 1
			if k == times {
				fmt.Println(time.Duration(allT1.Nanoseconds()/int64(times)).Nanoseconds(),
					time.Duration(allT2.Nanoseconds()/int64(times)).Nanoseconds(),
					time.Duration(allT3.Nanoseconds()/int64(times)).Nanoseconds())
				break
			}
			allTxs = allTxs[testSpan:]
		}
	}
}
