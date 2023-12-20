package conflict_detection

import (
	"chukonu/concurrency_control/conflict/nezha/core/types"
	coretypes "chukonu/core/types"
	"math/rand"
)

type Records struct {
	Rds []Record `json:"RECORDS"`
}

type Record struct {
	Hash        string      `json:"hash"`
	BlockHash   string      `json:"blockHash"`
	BlockNumber string      `json:"blockNumber"`
	Info        Transaction `json:"info"`
}

type Transfer struct {
	From  Balance `json:"from"`
	To    Balance `json:"to"`
	Type  uint8   `json:"type"`
	Nonce uint8   `json:"nonce"`
}

type Balance struct {
	Address  string  `json:"address"`
	Value    float64 `json:"value"`
	BeforeTx float64 `json:"beforeTx"`
	AfterTx  float64 `json:"afterTx"`
}

type Transaction struct {
	Count                uint8      `json:"count"`
	AccessList           string     `json:"accessList"`
	Transfer             []Transfer `json:"balance"`
	BlockHash            string     `json:"blockHash"`
	BlockNumber          string     `json:"blockNumber"`
	ChainId              string     `json:"chainId"`
	From                 string     `json:"from"`
	Gas                  string     `json:"gas"`
	GasPrice             string     `json:"gasPrice"`
	Hash                 string     `json:"hash"`
	Input                string     `json:"input"`
	MaxFeePerGas         string     `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string     `json:"maxPriorityFeePerGas"`
	Nonce                string     `json:"nonce"`
	R                    string     `json:"r"`
	S                    string     `json:"s"`
	To                   string     `json:"to"`
	TransactionIndex     string     `json:"transactionIndex"`
	Type                 string     `json:"type"`
	V                    string     `json:"v"`
	Value                string     `json:"value"`
}

// createBatchTxs retrieves tx data by querying database and writes to file
func createBatchTxs(txs coretypes.Transactions) []*types.Transaction {
	ntxs := make([]*types.Transaction, 0)
	for _, tx := range txs {
		trans := tx.AccessPre
		payload := createNewPayloads(trans)
		if len(payload) == 0 {
			continue
		}
		hash := tx.Hash()
		ntxs = append(ntxs, types.NewTransaction(&hash, 0, []byte("FromAddress"), []byte("ToAddress"), payload))
	}
	return ntxs
}

// createNewPayloads creates tx payloads (after block height 12,000,000)
func createNewPayloads(rwSet *coretypes.AccessAddressMap) types.Payload {
	var payload = make(map[string][]*types.RWSet)

	for addr, rw := range *rwSet {
		if rw.IsRead {
			rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
			payload[addr.Hex()] = append(payload[addr.Hex()], rw1)
		}
		if rw.IsWrite {
			r := rand.Intn(40)
			rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(-r)}
			payload[addr.Hex()] = append(payload[addr.Hex()], rw2)
		}
	}
	return payload
}
