package database

import "runtime"

type RawConfig struct {
	Path      string
	Cache     int
	Handles   int
	Ancient   string
	Namespace string
	ReadOnly  bool
}

func defaultRawConfig() *RawConfig {
	if runtime.GOOS == "darwin" { // MacOS
		return &RawConfig{
			Path:      "/Volumes/ETH_DATA/ethereum/geth/chaindata",
			Cache:     2048,
			Handles:   5120,
			Ancient:   "/Volumes/ETH_DATA/ethereum/geth/chaindata/ancient",
			Namespace: "eth/db/chaindata/",
			ReadOnly:  false,
		}
	} else {
		return &RawConfig{
			//Path:      "/experiment/ethereum/geth/chaindata",
			Path:    "/experiment/mysql/ethereum/geth/chaindata",
			Cache:   2048,
			Handles: 5120,
			//Ancient:   "/experiment/ethereum/geth/chaindata/ancient",
			Ancient:   "/experiment/mysql/ethereum/geth/chaindata/ancient",
			Namespace: "eth/db/chaindata/",
			ReadOnly:  false,
		}
	}
}

type StateDBConfig struct {
	Cache     int
	Journal   string
	Preimages bool
}

func defaultStateDBConfig() *StateDBConfig {
	if runtime.GOOS == "darwin" { // MacOS
		return &StateDBConfig{
			Cache:     614,
			Journal:   "/Users/darcywep/Projects/ethereum/execution/geth/triecache",
			Preimages: false,
		}
	} else {
		return &StateDBConfig{
			Cache: 614,
			//Journal:   "/experiment/ethereum/geth/triecache",
			Journal:   "/experiment/mysql/ethereum/geth/triecache",
			Preimages: false,
		}
	}
}
