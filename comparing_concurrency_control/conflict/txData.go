package conflict

import (
	"chukonu/comparing_concurrency_control/conflict/nezha/core/types"
	"github.com/DarcyWep/pureData/transaction"
	"math/rand"
	"strconv"
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
func createBatchTxs(pureTxs []*transaction.Transaction) []*types.Transaction {
	ntxs := make([]*types.Transaction, 0)
	for _, ptx := range pureTxs {
		trans := ptx.Transfers
		payload := createNewPayloads(trans)
		if len(payload) == 0 {
			continue
		}
		ntxs = append(ntxs, types.NewTransaction(ptx.Hash, 0, []byte("FromAddress"), []byte("ToAddress"), payload))
	}
	return ntxs
}

// createNewPayloads creates tx payloads (after block height 12,000,000)
func createNewPayloads(trans []transaction.Transfer) types.Payload {
	var payload = make(map[string][]*types.RWSet)

	for _, tran := range trans {
		if tran.GetLabel() == 0 {
			state, _ := tran.(*transaction.StateTransition)
			switch state.Type {
			case 1:
				// read-write (common transfer)
				r := rand.Intn(40)
				rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
				rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(-r)}
				addr1 := state.From.Address.String()
				payload[addr1] = append(payload[addr1], rw1, rw2)
				rw3 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
				addr2 := state.To.Address.String()
				payload[addr2] = append(payload[addr2], rw3)
			case 2:
				// withhold transaction fee from the caller
				r := rand.Intn(10)
				rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
				rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(-r)}
				addr := state.From.Address.String()
				payload[addr] = append(payload[addr], rw1, rw2)
			//case 3:
			//	// transfer transaction fee to the miner
			//	r := rand.Intn(10)
			//	rw := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
			//	addr := state.To.Address.String()
			//	payload[addr] = append(payload[addr], rw)
			case 4:
				// destroy contract
				rw := &types.RWSet{Label: "w", Addr: []byte("1"), Value: int64(0)}
				addr := state.To.Address.String()
				payload[addr] = append(payload[addr], rw)
			//case 5:
			//	// mining reward
			//	r := rand.Intn(20)
			//	rw := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
			//	addr := state.To.Address.String()
			//	payload[addr] = append(payload[addr], rw)
			default:
				continue
			}
		} else {
			storage, _ := tran.(*transaction.StorageTransition)
			if storage.NewValue == nil {
				// sload (read operation)
				a := strconv.Itoa(rand.Intn(3) + 2)
				rw := &types.RWSet{Label: "r", Addr: []byte(a)}
				addr := storage.Contract.String()
				payload[addr] = append(payload[addr], rw)
			} else {
				// sstore (write operation)
				if storage.PreValue.Big().Cmp(storage.NewValue.Big()) == 1 {
					// deduce operation
					r := rand.Intn(20)
					a := strconv.Itoa(rand.Intn(3) + 2)
					rw1 := &types.RWSet{Label: "r", Addr: []byte(a)}
					rw2 := &types.RWSet{Label: "w", Addr: []byte(a), Value: int64(-r)}
					addr := storage.Contract.String()
					payload[addr] = append(payload[addr], rw1, rw2)
				} else {
					// incremental operation
					r := rand.Intn(30)
					a := strconv.Itoa(rand.Intn(3) + 2)
					rw := &types.RWSet{Label: "iw", Addr: []byte(a), Value: int64(r)}
					addr := storage.Contract.String()
					payload[addr] = append(payload[addr], rw)
				}
			}
		}
	}
	return payload
}
