package coordinate

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/big"

	vlq "github.com/bsm/go-vlq"
	"github.com/martinboehm/btcd/wire"
	"github.com/martinboehm/btcutil/chaincfg"
	"github.com/trezor/blockbook/bchain"
	"github.com/trezor/blockbook/bchain/coins/btc"
	"github.com/trezor/blockbook/common"
)

const (
	TxVersionPreconf     = 9
	TxVersionAssetCreate = 10
	TxVersionAssetXfer   = 11
	TxVersionPegin       = 12

	MainnetMagic  wire.BitcoinNet = 0xd8b8bff8
	Testnet4Magic wire.BitcoinNet = 0x283f161f
	RegtestMagic  wire.BitcoinNet = 0xdab5bffa
)

var (
	MainNetParams  chaincfg.Params
	TestNet4Params chaincfg.Params
	RegtestParams  chaincfg.Params
)

func init() {
	MainNetParams = chaincfg.MainNetParams
	MainNetParams.Net = MainnetMagic
	MainNetParams.PubKeyHashAddrID = []byte{0}
	MainNetParams.ScriptHashAddrID = []byte{5}
	MainNetParams.Bech32HRPSegwit = "cc"

	TestNet4Params = chaincfg.TestNet3Params
	TestNet4Params.Net = Testnet4Magic
	TestNet4Params.PubKeyHashAddrID = []byte{111}
	TestNet4Params.ScriptHashAddrID = []byte{196}
	TestNet4Params.Bech32HRPSegwit = "tc"

	RegtestParams = chaincfg.RegressionNetParams
	RegtestParams.Net = RegtestMagic
	RegtestParams.PubKeyHashAddrID = []byte{111}
	RegtestParams.ScriptHashAddrID = []byte{196}
	RegtestParams.Bech32HRPSegwit = "ccrt"
}

// GetChainParams returns chain parameters based on the chain identifier
func GetChainParams(chain string) *chaincfg.Params {
	if !chaincfg.IsRegistered(&MainNetParams) {
		if err := chaincfg.Register(&MainNetParams); err != nil {
			panic(err)
		}
	}
	if !chaincfg.IsRegistered(&TestNet4Params) {
		if err := chaincfg.Register(&TestNet4Params); err != nil {
			panic(err)
		}
	}
	if !chaincfg.IsRegistered(&RegtestParams) {
		if err := chaincfg.Register(&RegtestParams); err != nil {
			panic(err)
		}
	}
	switch chain {
	case "testnet4":
		return &TestNet4Params
	case "regtest":
		return &RegtestParams
	default:
		return &MainNetParams
	}
}

// ---------------------------------------------------------------------------
// JSON types for daemon's getrawtransaction output
// ---------------------------------------------------------------------------

type CoordinateVin struct {
	Coinbase  string           `json:"coinbase"`
	Txid      string           `json:"txid"`
	Vout      uint32           `json:"vout"`
	ScriptSig bchain.ScriptSig `json:"scriptSig"`
	Sequence  uint32           `json:"sequence"`
	Addresses []string         `json:"addresses"`
	AssetId   string           `json:"assetid"` // present on asset inputs only
}

type CoordinateVout struct {
	ValueSat     big.Int
	JsonValue    common.JSONNumber      `json:"value"`
	N            uint32                 `json:"n"`
	ScriptPubKey CoordinateScriptPubKey `json:"scriptPubKey"`
}

type CoordinateScriptPubKey struct {
	Hex       string   `json:"hex"`
	Addresses []string `json:"addresses"`
	Address   string   `json:"address"`
}

type CoordinateTx struct {
	Hex           string           `json:"hex"`
	Txid          string           `json:"txid"`
	Version       int32            `json:"version"`
	LockTime      uint32           `json:"locktime"`
	VSize         int64            `json:"vsize,omitempty"`
	Vin           []CoordinateVin  `json:"vin"`
	Vout          []CoordinateVout `json:"vout"`
	BlockHeight   uint32           `json:"blockHeight,omitempty"`
	Confirmations uint32           `json:"confirmations,omitempty"`
	Time          int64            `json:"time,omitempty"`
	Blocktime     int64            `json:"blocktime,omitempty"`
	// v10 ASSET_CREATE metadata fields
	Precision   int32  `json:"precision,omitempty"`
	AssetType   int32  `json:"assettype,omitempty"`
	Ticker      string `json:"ticker,omitempty"`
	Headline    string `json:"headline,omitempty"`
	PayloadData string `json:"payloaddata,omitempty"`
}

// ---------------------------------------------------------------------------
// CoordinateParser
// ---------------------------------------------------------------------------

// CoordinateParser handles Coordinate address/tx parsing.
// Asset tagging is NOT done here — it happens in the DB layer
// (processAssetsCoordinateType) which has access to the UTXO set.
type CoordinateParser struct {
	*btc.BitcoinLikeParser
}

// NewCoordinateParser returns new CoordinateParser instance
func NewCoordinateParser(params *chaincfg.Params, c *btc.Configuration) *CoordinateParser {
	p := &CoordinateParser{
		BitcoinLikeParser: btc.NewBitcoinLikeParser(params, c),
	}
	p.VSizeSupport = true
	return p
}

// SupportsAssets returns true — enables asset-aware UTXO packing in the DB layer.
func (p *CoordinateParser) SupportsAssets() bool {
	return true
}

// ParseBlock is not supported for Coordinate (JSON-only).
func (p *CoordinateParser) ParseBlock(b []byte) (*bchain.Block, error) {
	return nil, errors.New("ParseBlock not supported for Coordinate: use JSON RPC")
}

// ParseTx is not supported for Coordinate (JSON-only).
func (p *CoordinateParser) ParseTx(b []byte) (*bchain.Tx, error) {
	return nil, errors.New("ParseTx not supported for Coordinate: use JSON RPC")
}

// ParseTxFromJson parses Coordinate transaction JSON.
//
// Preserves vin[].assetid in bchain.Vin.AssetId for the DB layer.
// NO output asset tagging — that requires the UTXO set.
func (p *CoordinateParser) ParseTxFromJson(msg json.RawMessage) (*bchain.Tx, error) {
	var coordTx CoordinateTx
	err := json.Unmarshal(msg, &coordTx)
	if err != nil {
		return nil, err
	}

	tx := bchain.Tx{
		Hex:           coordTx.Hex,
		Txid:          coordTx.Txid,
		Version:       coordTx.Version,
		LockTime:      coordTx.LockTime,
		VSize:         coordTx.VSize,
		BlockHeight:   coordTx.BlockHeight,
		Confirmations: coordTx.Confirmations,
		Time:          coordTx.Time,
		Blocktime:     coordTx.Blocktime,
	}

	// Convert vin — preserve assetid
	tx.Vin = make([]bchain.Vin, len(coordTx.Vin))
	for i := range coordTx.Vin {
		src := &coordTx.Vin[i]
		tx.Vin[i] = bchain.Vin{
			Coinbase:  src.Coinbase,
			Txid:      src.Txid,
			Vout:      src.Vout,
			ScriptSig: src.ScriptSig,
			Sequence:  src.Sequence,
			Addresses: src.Addresses,
			AssetId:   src.AssetId,
		}
	}

	// Convert vout — no asset tagging
	tx.Vout = make([]bchain.Vout, len(coordTx.Vout))
	for i := range coordTx.Vout {
		src := &coordTx.Vout[i]
		vout := &tx.Vout[i]

		vout.ValueSat, err = p.AmountToBigInt(src.JsonValue)
		if err != nil {
			return nil, err
		}
		vout.N = src.N
		vout.ScriptPubKey.Hex = src.ScriptPubKey.Hex
		if len(src.ScriptPubKey.Addresses) == 0 && src.ScriptPubKey.Address != "" {
			vout.ScriptPubKey.Addresses = []string{src.ScriptPubKey.Address}
		} else {
			vout.ScriptPubKey.Addresses = src.ScriptPubKey.Addresses
		}
	}

	return &tx, nil
}

// ---------------------------------------------------------------------------
// PackTx / UnpackTx — store JSON instead of wire-format bytes
// ---------------------------------------------------------------------------

// coordinatePackedVin is the minimal vin representation for DB storage.
type coordinatePackedVin struct {
	Coinbase  string           `json:"coinbase,omitempty"`
	Txid      string           `json:"txid,omitempty"`
	Vout      uint32           `json:"vout,omitempty"`
	ScriptSig bchain.ScriptSig `json:"scriptSig,omitempty"`
	Sequence  uint32           `json:"sequence,omitempty"`
	Addresses []string         `json:"addresses,omitempty"`
	AssetId   string           `json:"assetid,omitempty"`
}

// coordinatePackedVout stores value as a decimal string to avoid precision loss.
type coordinatePackedVout struct {
	ValueSat string   `json:"valueSat"`
	N        uint32   `json:"n"`
	Hex      string   `json:"hex,omitempty"`
	Addrs    []string `json:"addrs,omitempty"`
}

// coordinatePackedTx is the compact JSON representation stored in RocksDB.
type coordinatePackedTx struct {
	Hex      string                 `json:"hex,omitempty"`
	Txid     string                 `json:"txid"`
	Version  int32                  `json:"version"`
	LockTime uint32                 `json:"locktime,omitempty"`
	VSize    int64                  `json:"vsize,omitempty"`
	Vin      []coordinatePackedVin  `json:"vin"`
	Vout     []coordinatePackedVout `json:"vout"`
}

// PackTx serialises a Coordinate transaction for DB storage.
// Format: [4 bytes height][vlq blockTime][JSON of coordinatePackedTx]
func (p *CoordinateParser) PackTx(tx *bchain.Tx, height uint32, blockTime int64) ([]byte, error) {
	packed := coordinatePackedTx{
		Hex:      tx.Hex,
		Txid:     tx.Txid,
		Version:  tx.Version,
		LockTime: tx.LockTime,
		VSize:    tx.VSize,
		Vin:      make([]coordinatePackedVin, len(tx.Vin)),
		Vout:     make([]coordinatePackedVout, len(tx.Vout)),
	}
	for i := range tx.Vin {
		packed.Vin[i] = coordinatePackedVin{
			Coinbase:  tx.Vin[i].Coinbase,
			Txid:      tx.Vin[i].Txid,
			Vout:      tx.Vin[i].Vout,
			ScriptSig: tx.Vin[i].ScriptSig,
			Sequence:  tx.Vin[i].Sequence,
			Addresses: tx.Vin[i].Addresses,
			AssetId:   tx.Vin[i].AssetId,
		}
	}
	for i := range tx.Vout {
		packed.Vout[i] = coordinatePackedVout{
			ValueSat: tx.Vout[i].ValueSat.String(),
			N:        tx.Vout[i].N,
			Hex:      tx.Vout[i].ScriptPubKey.Hex,
			Addrs:    tx.Vout[i].ScriptPubKey.Addresses,
		}
	}
	jsonData, err := json.Marshal(packed)
	if err != nil {
		return nil, err
	}
	header := make([]byte, 4+vlq.MaxLen64)
	binary.BigEndian.PutUint32(header[0:4], height)
	vl := vlq.PutInt(header[4:], blockTime)
	buf := make([]byte, 4+vl+len(jsonData))
	copy(buf, header[:4+vl])
	copy(buf[4+vl:], jsonData)
	return buf, nil
}

// UnpackTx deserialises a Coordinate transaction from DB storage.
func (p *CoordinateParser) UnpackTx(buf []byte) (*bchain.Tx, uint32, error) {
	height := binary.BigEndian.Uint32(buf)
	bt, l := vlq.Int(buf[4:])

	var packed coordinatePackedTx
	if err := json.Unmarshal(buf[4+l:], &packed); err != nil {
		return nil, 0, err
	}

	tx := bchain.Tx{
		Hex:       packed.Hex,
		Txid:      packed.Txid,
		Version:   packed.Version,
		LockTime:  packed.LockTime,
		VSize:     packed.VSize,
		Blocktime: bt,
		Vin:       make([]bchain.Vin, len(packed.Vin)),
		Vout:      make([]bchain.Vout, len(packed.Vout)),
	}
	for i := range packed.Vin {
		tx.Vin[i] = bchain.Vin{
			Coinbase:  packed.Vin[i].Coinbase,
			Txid:      packed.Vin[i].Txid,
			Vout:      packed.Vin[i].Vout,
			ScriptSig: packed.Vin[i].ScriptSig,
			Sequence:  packed.Vin[i].Sequence,
			Addresses: packed.Vin[i].Addresses,
			AssetId:   packed.Vin[i].AssetId,
		}
	}
	for i := range packed.Vout {
		val := new(big.Int)
		val.SetString(packed.Vout[i].ValueSat, 10)
		tx.Vout[i] = bchain.Vout{
			ValueSat: *val,
			N:        packed.Vout[i].N,
		}
		tx.Vout[i].ScriptPubKey.Hex = packed.Vout[i].Hex
		tx.Vout[i].ScriptPubKey.Addresses = packed.Vout[i].Addrs
	}
	return &tx, height, nil
}