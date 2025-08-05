package coordinate

import (
	"encoding/json"
	"math/big"

	"github.com/golang/glog"
	"github.com/trezor/blockbook/common"
	"github.com/martinboehm/btcd/wire"
	"github.com/martinboehm/btcutil/chaincfg"
	"github.com/trezor/blockbook/bchain"
	"github.com/trezor/blockbook/bchain/coins/btc"
)

const (
	// MainnetMagic is mainnet network constant
	MainnetMagic wire.BitcoinNet = 0xf8beb9d8
)

var (
	// MainNetParams are parser parameters for mainnet
	MainNetParams chaincfg.Params
)

func init() {
	MainNetParams = chaincfg.MainNetParams
	MainNetParams.Net = MainnetMagic
	MainNetParams.Bech32HRPSegwit = "tc"
}

// CoordinateParser handle
type CoordinateParser struct {
	*btc.BitcoinLikeParser
}

// NewCoordinateParser returns new CoordinateParser instance
func NewCoordinateParser(params *chaincfg.Params, c *btc.Configuration) *CoordinateParser {
	p := &CoordinateParser{BitcoinLikeParser: btc.NewBitcoinLikeParser(params, c)}
	p.VSizeSupport = true
	return p
}

// GetChainParams contains network parameters for the main Namecoin network,
// and the test Namecoin network
func GetChainParams(chain string) *chaincfg.Params {
	if !chaincfg.IsRegistered(&MainNetParams) {
		err := chaincfg.Register(&MainNetParams)
		if err != nil {
			panic(err)
		}
	}
	switch chain {
	default:
		return &MainNetParams
	}
}

// // ParseBlock parses raw block to our Block struct
// // it has special handling for Auxpow blocks that cannot be parsed by standard btc wire parser
// func (p *CoordinateParser) ParseBlock(b []byte) (*bchain.Block, error) {
// 	r := bytes.NewReader(b)
// 	w := wire.MsgBlock{}
// 	h := wire.BlockHeader{}
// 	err := h.Deserialize(r)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if (h.Version & utils.VersionAuxpow) != 0 {
// 		if err = utils.SkipAuxpow(r); err != nil {
// 			return nil, err
// 		}
// 	}

// 	err = utils.DecodeTransactions(r, 0, wire.WitnessEncoding, &w)
// 	if err != nil {
// 		return nil, err
// 	}

// 	txs := make([]bchain.Tx, len(w.Transactions))
// 	for ti, t := range w.Transactions {
// 		txs[ti] = p.TxFromMsgTx(t, false)
// 	}

// 	return &bchain.Block{
// 		BlockHeader: bchain.BlockHeader{
// 			Size: len(b),
// 			Time: h.Timestamp.Unix(),
// 		},
// 		Txs: txs,
// 	}, nil
// }

// ScriptPubKey contains data about output script
type ScriptPubKey struct {
	// Asm       string   `json:"asm"`
	Hex string `json:"hex,omitempty"`
	// Type      string   `json:"type"`
	Addresses []string `json:"addresses"` // removed from Bitcoind 22.0.0
	Address   string   `json:"address"`   // used in Bitcoind 22.0.0
}

// Vout contains data about tx output
type Vout struct {
	ValueSat     big.Int
	JsonValue    common.JSONNumber `json:"value"`
	N            uint32            `json:"n"`
	ScriptPubKey ScriptPubKey      `json:"scriptPubKey"`
}


// Tx is blockchain transaction
// unnecessary fields are commented out to avoid overhead
type Tx struct {
	Hex         string       `json:"hex"`
	Txid        string       `json:"txid"`
	Version     int32        `json:"version"`
	AssetType   int32  `json:"assetType"`
    Precision   int32  `json:"precision"`
    Ticker      string `json:"ticker"`
    Headline    string `json:"headline"`
    Payload     string `json:"payload"`     // hex encoded
    PayloadData string `json:"payloadData"` // base64 or UTF-8
	LockTime    uint32       `json:"locktime"`
	VSize       int64        `json:"vsize,omitempty"`
	Vin         []bchain.Vin `json:"vin"`
	Vout        []Vout       `json:"vout"`
	BlockHeight uint32       `json:"blockHeight,omitempty"`
	// BlockHash     string `json:"blockhash,omitempty"`
	Confirmations    uint32      `json:"confirmations,omitempty"`
	Time             int64       `json:"time,omitempty"`
	Blocktime        int64       `json:"blocktime,omitempty"`
	CoinSpecificData interface{} `json:"-"`
}

// ParseTxFromJson parses JSON message containing transaction and returns Tx struct
// Bitcoind version 22.0.0 removed ScriptPubKey.Addresses from the API and replaced it by a single Address
func (p *CoordinateParser) ParseTxFromJson(msg json.RawMessage) (*bchain.Tx, error) {
	var bitcoinTx Tx
	var tx bchain.Tx
	err := json.Unmarshal(msg, &bitcoinTx)
	if err != nil {
		return nil, err
	}

	// it is necessary to copy bitcoinTx to Tx to make it compatible
	tx.Hex = bitcoinTx.Hex
	tx.Txid = bitcoinTx.Txid
	tx.Version = bitcoinTx.Version
	tx.LockTime = bitcoinTx.LockTime
	tx.VSize = bitcoinTx.VSize
	tx.Vin = bitcoinTx.Vin
	tx.BlockHeight = bitcoinTx.BlockHeight
	tx.Confirmations = bitcoinTx.Confirmations
	tx.Time = bitcoinTx.Time
	tx.Blocktime = bitcoinTx.Blocktime
	tx.CoinSpecificData = bitcoinTx.CoinSpecificData
	tx.Vout = make([]bchain.Vout, len(bitcoinTx.Vout))

	for i := range bitcoinTx.Vout {
		bitcoinVout := &bitcoinTx.Vout[i]
		vout := &tx.Vout[i]
		// convert vout.JsonValue to big.Int and clear it, it is only temporary value used for unmarshal
		vout.ValueSat, err = p.AmountToBigInt(bitcoinVout.JsonValue)
		if err != nil {
			return nil, err
		}
		vout.N = bitcoinVout.N
		vout.ScriptPubKey.Hex = bitcoinVout.ScriptPubKey.Hex
		// convert single Address to Addresses if Addresses are empty
		if len(bitcoinVout.ScriptPubKey.Addresses) == 0 {
			vout.ScriptPubKey.Addresses = []string{bitcoinVout.ScriptPubKey.Address}
		} else {
			vout.ScriptPubKey.Addresses = bitcoinVout.ScriptPubKey.Addresses
		}
	}

	return &tx, nil
}


// GetAddrDescForUnknownInput returns nil AddressDescriptor
func (p *CoordinateParser) GetAddrDescForUnknownInput(tx *bchain.Tx, input int) bchain.AddressDescriptor {
	var iTxid string
	if len(tx.Vin) > input {
		iTxid = tx.Vin[input].Txid
	}
	glog.Warningf("tx %v, input tx %v not found in txAddresses for coordinate", tx.Txid, iTxid)
	return nil
}