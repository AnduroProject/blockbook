package coordinate

import (
	"encoding/json"

	"github.com/juju/errors"
	"github.com/golang/glog"
	"github.com/trezor/blockbook/bchain"
	"github.com/trezor/blockbook/bchain/coins/btc"
)

// CoordinateRPC is an interface to JSON-RPC namecoin service.
type CoordinateRPC struct {
	*btc.BitcoinRPC
}

type ResGetBlockFull struct {
	Error  *bchain.RPCError `json:"error"`
	Result bchain.Block     `json:"result"`
}

type CmdGetRawTransaction struct {
	Method string `json:"method"`
	Params struct {
		Txid    string `json:"txid"`
		Verbose bool   `json:"verbose"`
	} `json:"params"`
}

type ResGetRawTransaction struct {
	Error  *bchain.RPCError `json:"error"`
	Result json.RawMessage  `json:"result"`
}

type CmdGetBlock struct {
	Method string `json:"method"`
	Params struct {
		BlockHash string `json:"blockhash"`
		Verbosity int    `json:"verbosity"`
	} `json:"params"`
}



// NewCoordinateRPC returns new CoordinateRPC instance.
func NewCoordinateRPC(config json.RawMessage, pushHandler func(bchain.NotificationType)) (bchain.BlockChain, error) {
	b, err := btc.NewBitcoinRPC(config, pushHandler)
	if err != nil {
		return nil, err
	}

	s := &CoordinateRPC{
		b.(*btc.BitcoinRPC),
	}
	s.RPCMarshaler = btc.JSONMarshalerV1{}
	s.ChainConfig.SupportsEstimateFee = false

	return s, nil
}

// Initialize initializes CoordinateRPC instance.
func (b *CoordinateRPC) Initialize() error {
	ci, err := b.GetChainInfo()
	if err != nil {
		return err
	}
	chainName := ci.Chain

	glog.Info("Chain name ", chainName)
	params := GetChainParams(chainName)

	// always create parser
	b.Parser = NewCoordinateParser(params, b.ChainConfig)

	// parameters for getInfo request
	if params.Net == MainnetMagic {
		b.Testnet = false
		b.Network = "livenet"
	} else {
		b.Testnet = true
		b.Network = "testnet"
	}

	glog.Info("rpc: block chain ", params.Name)

	return nil
}

// GetBlock returns block with given hash.
func (b *CoordinateRPC) GetBlock(hash string, height uint32) (*bchain.Block, error) {
	glog.Warningf("GetBlock test 1")
	var err error
	if hash == "" {
		hash, err = b.GetBlockHash(height)
		if err != nil {
			return nil, err
		}
	}
	if !b.ParseBlocks {
		return b.GetBlockFull(hash)
	}
	return b.GetBlockWithoutHeader(hash, height)
}

// IsErrBlockNotFound returns true if error means block was not found
func IsErrBlockNotFound(err *bchain.RPCError) bool {
	return err.Message == "Block not found" ||
		err.Message == "Block height out of range"
}


// GetBlockFull returns block with given hash
func (b *CoordinateRPC) GetBlockFull(hash string) (*bchain.Block, error) {
	glog.Warningf("GetBlockFull test 1")
	glog.V(1).Info("rpc: getblock (verbosity=2) ", hash)

	res := ResGetBlockFull{}
	req := CmdGetBlock{Method: "getblock"}
	req.Params.BlockHash = hash
	req.Params.Verbosity = 2
	err := b.Call(&req, &res)

	if err != nil {
		return nil, errors.Annotatef(err, "hash %v", hash)
	}
	if res.Error != nil {
		if IsErrBlockNotFound(res.Error) {
			return nil, bchain.ErrBlockNotFound
		}
		return nil, errors.Annotatef(res.Error, "hash %v", hash)
	}

	for i := range res.Result.Txs {
		tx := &res.Result.Txs[i]
		if tx.Version != 2 {
			continue
		}
		for j := range tx.Vout {
			vout := &tx.Vout[j]
			// convert vout.JsonValue to big.Int and clear it, it is only temporary value used for unmarshal
			vout.ValueSat, err = b.Parser.AmountToBigInt(vout.JsonValue)
			if err != nil {
				return nil, err
			}
			vout.JsonValue = ""
		}
	}

	return &res.Result, nil
}

// IsMissingTx return true if error means missing tx
func IsMissingTx(err *bchain.RPCError) bool {
	// err.Code == -5 "No such mempool or blockchain transaction"
	return err.Code == -5
}

// getRawTransaction returns json as returned by backend, with all coin specific data
func (b *CoordinateRPC) getRawTransaction(txid string) (json.RawMessage, error) {
	glog.V(1).Info("rpc: getrawtransaction ", txid)

	res := ResGetRawTransaction{}
	req := CmdGetRawTransaction{Method: "getrawtransaction"}
	req.Params.Txid = txid
	req.Params.Verbose = true
	err := b.Call(&req, &res)

	if err != nil {
		return nil, errors.Annotatef(err, "txid %v", txid)
	}
	if res.Error != nil {
		if IsMissingTx(res.Error) {
			return nil, bchain.ErrTxNotFound
		}
		return nil, errors.Annotatef(res.Error, "txid %v", txid)
	}
	return res.Result, nil
}

// GetTransaction returns a transaction by the transaction ID
func (b *CoordinateRPC) GetTransaction(txid string) (*bchain.Tx, error) {
	glog.Warningf("GetTransaction test 1")
	r, err := b.getRawTransaction(txid)
	if err != nil {
		return nil, err
	}
	tx, err := b.Parser.ParseTxFromJson(r)
	if err != nil {
		return nil, errors.Annotatef(err, "txid %v", txid)
	}
	tx.CoinSpecificData = r
	return tx, nil
}