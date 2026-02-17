//go:build unittest

package coordinate

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"reflect"
	"testing"

	"github.com/martinboehm/btcutil/chaincfg"
	"github.com/trezor/blockbook/bchain/coins/btc"
)

func TestMain(m *testing.M) {
	c := m.Run()
	chaincfg.ResetParams()
	os.Exit(c)
}

func testParser() *CoordinateParser {
	return NewCoordinateParser(GetChainParams("regtest"), &btc.Configuration{})
}

// ---------------------------------------------------------------------------
// SupportsAssets
// ---------------------------------------------------------------------------

func TestSupportsAssets(t *testing.T) {
	p := testParser()
	if !p.SupportsAssets() {
		t.Error("SupportsAssets() = false, want true")
	}
}

// ---------------------------------------------------------------------------
// Address parsing (bech32 ccrt prefix for regtest)
// ---------------------------------------------------------------------------

func TestGetAddrDescFromAddress(t *testing.T) {
	p := testParser()
	tests := []struct {
		name    string
		address string
		want    string // hex of addrDesc
		wantErr bool
	}{
		{
			// P2PKH regtest (same addr bytes as testnet, PubKeyHashAddrID=111)
			name:    "P2PKH regtest",
			address: "mfcWp7DB6NuaZsExybTTXpVgWz559Np4Ti",
			want:    "76a914010d39800f86122416e28f485029acf77507169288ac",
			wantErr: false,
		},
		{
			// bech32 with wrong checksum for ccrt prefix should fail
			name:    "bech32 invalid checksum",
			address: "ccrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kwpa3a",
			wantErr: true,
		},
		{
			name:    "invalid address",
			address: "invalidaddress",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.GetAddrDescFromAddress(tt.address)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAddrDescFromAddress(%q) error = %v, wantErr %v", tt.address, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				h := hex.EncodeToString(got)
				if h != tt.want {
					t.Errorf("GetAddrDescFromAddress(%q) = %v, want %v", tt.address, h, tt.want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ParseTxFromJson — v10 ASSET_CREATE
// ---------------------------------------------------------------------------

// Test vector: v10 tx creating asset with ticker "GOLD", precision 4
// output[0] = controller (0 sat), output[1] = supply (100000000 sat)
var testV10Json = json.RawMessage(`{
	"txid": "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb",
	"version": 10,
	"locktime": 0,
	"vin": [
		{
			"txid": "1111111111111111111111111111111111111111111111111111111111111111",
			"vout": 0,
			"scriptSig": {"hex": "483045022100"},
			"sequence": 4294967295,
			"addresses": ["ccrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kwpa3a"]
		}
	],
	"vout": [
		{
			"value": "0.00000000",
			"n": 0,
			"scriptPubKey": {
				"hex": "0014751e76e8199196d454941c45d1b3a323f1433bd6",
				"address": "ccrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kwpa3a"
			}
		},
		{
			"value": "1.00000000",
			"n": 1,
			"scriptPubKey": {
				"hex": "0014abcdef1234567890abcdef1234567890abcdef12",
				"address": "ccrt1q40xm7ydg4v7ys4003ydgav0y2t003ms5y4aeh"
			}
		}
	],
	"blockHeight": 100,
	"time": 1700000000,
	"blocktime": 1700000000,
	"precision": 4,
	"assettype": 0,
	"ticker": "GOLD",
	"headline": "Digital Gold Token"
}`)

func TestParseTxFromJson_V10_AssetCreate(t *testing.T) {
	p := testParser()
	tx, err := p.ParseTxFromJson(testV10Json)
	if err != nil {
		t.Fatalf("ParseTxFromJson(v10) error = %v", err)
	}

	// Version
	if tx.Version != 10 {
		t.Errorf("Version = %d, want 10", tx.Version)
	}

	// Txid
	if tx.Txid != "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb" {
		t.Errorf("Txid = %q", tx.Txid)
	}

	// Vin count
	if len(tx.Vin) != 1 {
		t.Fatalf("len(Vin) = %d, want 1", len(tx.Vin))
	}

	// Vin[0] preserved
	if tx.Vin[0].Txid != "1111111111111111111111111111111111111111111111111111111111111111" {
		t.Errorf("Vin[0].Txid = %q", tx.Vin[0].Txid)
	}
	if tx.Vin[0].Vout != 0 {
		t.Errorf("Vin[0].Vout = %d", tx.Vin[0].Vout)
	}

	// Vout count
	if len(tx.Vout) != 2 {
		t.Fatalf("len(Vout) = %d, want 2", len(tx.Vout))
	}

	// Vout[0] = controller (0 sat)
	if tx.Vout[0].ValueSat.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("Vout[0].ValueSat = %s, want 0", tx.Vout[0].ValueSat.String())
	}

	// Vout[1] = supply (1 BTC = 100000000 sat)
	if tx.Vout[1].ValueSat.Cmp(big.NewInt(100000000)) != 0 {
		t.Errorf("Vout[1].ValueSat = %s, want 100000000", tx.Vout[1].ValueSat.String())
	}

	// Address from "address" field (not "addresses")
	if len(tx.Vout[1].ScriptPubKey.Addresses) != 1 ||
		tx.Vout[1].ScriptPubKey.Addresses[0] != "ccrt1q40xm7ydg4v7ys4003ydgav0y2t003ms5y4aeh" {
		t.Errorf("Vout[1].Addresses = %v", tx.Vout[1].ScriptPubKey.Addresses)
	}

	// BlockHeight
	if tx.BlockHeight != 100 {
		t.Errorf("BlockHeight = %d, want 100", tx.BlockHeight)
	}
}

// ---------------------------------------------------------------------------
// ParseTxFromJson — v11 ASSET_TRANSFER with vin.assetid
// ---------------------------------------------------------------------------

var testV11Json = json.RawMessage(`{
	"txid": "eeff00112233445566778899aabbccddeeff00112233445566778899aabbccdd",
	"version": 11,
	"locktime": 0,
	"vin": [
		{
			"txid": "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb",
			"vout": 1,
			"scriptSig": {"hex": ""},
			"sequence": 4294967295,
			"assetid": "00000064000000000000"
		}
	],
	"vout": [
		{
			"value": "0.60000000",
			"n": 0,
			"scriptPubKey": {
				"hex": "0014aaaa",
				"addresses": ["ccrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kwpa3a"]
			}
		},
		{
			"value": "0.40000000",
			"n": 1,
			"scriptPubKey": {
				"hex": "0014bbbb",
				"addresses": ["ccrt1q40xm7ydg4v7ys4003ydgav0y2t003ms5y4aeh"]
			}
		}
	],
	"time": 1700001000,
	"blocktime": 1700001000
}`)

func TestParseTxFromJson_V11_AssetTransfer(t *testing.T) {
	p := testParser()
	tx, err := p.ParseTxFromJson(testV11Json)
	if err != nil {
		t.Fatalf("ParseTxFromJson(v11) error = %v", err)
	}

	if tx.Version != 11 {
		t.Errorf("Version = %d, want 11", tx.Version)
	}

	// AssetId preserved on vin
	if tx.Vin[0].AssetId != "00000064000000000000" {
		t.Errorf("Vin[0].AssetId = %q, want '00000064000000000000'", tx.Vin[0].AssetId)
	}

	// Output values
	if tx.Vout[0].ValueSat.Cmp(big.NewInt(60000000)) != 0 {
		t.Errorf("Vout[0].ValueSat = %s, want 60000000", tx.Vout[0].ValueSat.String())
	}
	if tx.Vout[1].ValueSat.Cmp(big.NewInt(40000000)) != 0 {
		t.Errorf("Vout[1].ValueSat = %s, want 40000000", tx.Vout[1].ValueSat.String())
	}
}

// ---------------------------------------------------------------------------
// ParseTxFromJson — Regular (v2) transaction, no asset fields
// ---------------------------------------------------------------------------

var testRegularJson = json.RawMessage(`{
	"txid": "99887766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa",
	"version": 2,
	"locktime": 500000,
	"vin": [
		{
			"txid": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
			"vout": 3,
			"scriptSig": {"hex": "4830"},
			"sequence": 4294967294
		}
	],
	"vout": [
		{
			"value": "0.50000000",
			"n": 0,
			"scriptPubKey": {
				"hex": "76a914aabbccdd88ac",
				"addresses": ["ccrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kwpa3a"]
			}
		}
	],
	"time": 1700002000,
	"blocktime": 1700002000
}`)

func TestParseTxFromJson_RegularTx(t *testing.T) {
	p := testParser()
	tx, err := p.ParseTxFromJson(testRegularJson)
	if err != nil {
		t.Fatalf("ParseTxFromJson(regular) error = %v", err)
	}

	if tx.Version != 2 {
		t.Errorf("Version = %d, want 2", tx.Version)
	}

	// AssetId should be empty on regular tx vin
	if tx.Vin[0].AssetId != "" {
		t.Errorf("Vin[0].AssetId = %q, want empty", tx.Vin[0].AssetId)
	}

	// LockTime
	if tx.LockTime != 500000 {
		t.Errorf("LockTime = %d, want 500000", tx.LockTime)
	}

	// Value
	if tx.Vout[0].ValueSat.Cmp(big.NewInt(50000000)) != 0 {
		t.Errorf("Vout[0].ValueSat = %s, want 50000000", tx.Vout[0].ValueSat.String())
	}
}

// ---------------------------------------------------------------------------
// ParseTxFromJson — Coinbase
// ---------------------------------------------------------------------------

var testCoinbaseJson = json.RawMessage(`{
	"txid": "aaaa000000000000000000000000000000000000000000000000000000000001",
	"version": 2,
	"locktime": 0,
	"vin": [
		{
			"coinbase": "0364000101",
			"sequence": 4294967295
		}
	],
	"vout": [
		{
			"value": "50.00000000",
			"n": 0,
			"scriptPubKey": {
				"hex": "0014751e76e8199196d454941c45d1b3a323f1433bd6",
				"address": "ccrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kwpa3a"
			}
		}
	]
}`)

func TestParseTxFromJson_Coinbase(t *testing.T) {
	p := testParser()
	tx, err := p.ParseTxFromJson(testCoinbaseJson)
	if err != nil {
		t.Fatalf("ParseTxFromJson(coinbase) error = %v", err)
	}

	if tx.Vin[0].Coinbase != "0364000101" {
		t.Errorf("Vin[0].Coinbase = %q, want '0364000101'", tx.Vin[0].Coinbase)
	}
	if tx.Vin[0].Txid != "" {
		t.Errorf("Vin[0].Txid = %q, want empty", tx.Vin[0].Txid)
	}
}

// ---------------------------------------------------------------------------
// ParseTxFromJson — Malformed JSON
// ---------------------------------------------------------------------------

func TestParseTxFromJson_BadJson(t *testing.T) {
	p := testParser()
	_, err := p.ParseTxFromJson(json.RawMessage(`{bad json`))
	if err == nil {
		t.Error("ParseTxFromJson(bad json) should return error")
	}
}

// ---------------------------------------------------------------------------
// ParseBlock should fail
// ---------------------------------------------------------------------------

func TestParseBlock_Unsupported(t *testing.T) {
	p := testParser()
	_, err := p.ParseBlock([]byte{0x00})
	if err == nil {
		t.Error("ParseBlock should return error for Coordinate")
	}
}

// ---------------------------------------------------------------------------
// ParseTx should fail
// ---------------------------------------------------------------------------

func TestParseTx_Unsupported(t *testing.T) {
	p := testParser()
	_, err := p.ParseTx([]byte{0x00})
	if err == nil {
		t.Error("ParseTx should return error for Coordinate")
	}
}

// ---------------------------------------------------------------------------
// Multiple addresses in "addresses" array vs single "address" field
// ---------------------------------------------------------------------------

var testMultiAddrJson = json.RawMessage(`{
	"txid": "1100000000000000000000000000000000000000000000000000000000000011",
	"version": 2,
	"locktime": 0,
	"vin": [],
	"vout": [
		{
			"value": "1.00000000",
			"n": 0,
			"scriptPubKey": {
				"hex": "0014aabb",
				"addresses": ["addr1", "addr2"]
			}
		},
		{
			"value": "2.00000000",
			"n": 1,
			"scriptPubKey": {
				"hex": "0014ccdd",
				"address": "single_addr"
			}
		}
	]
}`)

func TestParseTxFromJson_AddressFields(t *testing.T) {
	p := testParser()
	tx, err := p.ParseTxFromJson(testMultiAddrJson)
	if err != nil {
		t.Fatalf("ParseTxFromJson error = %v", err)
	}

	// "addresses" array → preserved as-is
	if !reflect.DeepEqual(tx.Vout[0].ScriptPubKey.Addresses, []string{"addr1", "addr2"}) {
		t.Errorf("Vout[0].Addresses = %v, want [addr1 addr2]", tx.Vout[0].ScriptPubKey.Addresses)
	}

	// "address" (singular) → converted to single-element slice
	if !reflect.DeepEqual(tx.Vout[1].ScriptPubKey.Addresses, []string{"single_addr"}) {
		t.Errorf("Vout[1].Addresses = %v, want [single_addr]", tx.Vout[1].ScriptPubKey.Addresses)
	}
}

// ---------------------------------------------------------------------------
// GetChainParams coverage
// ---------------------------------------------------------------------------

func TestGetChainParams(t *testing.T) {
	tests := []struct {
		chain string
		hrp   string
	}{
		{"main", "cc"},
		{"testnet4", "tc"},
		{"regtest", "ccrt"},
		{"unknown", "cc"}, // defaults to mainnet
	}
	for _, tt := range tests {
		params := GetChainParams(tt.chain)
		if params.Bech32HRPSegwit != tt.hrp {
			t.Errorf("GetChainParams(%q).Bech32HRPSegwit = %q, want %q", tt.chain, params.Bech32HRPSegwit, tt.hrp)
		}
	}
}