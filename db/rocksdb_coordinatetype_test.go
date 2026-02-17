//go:build unittest

package db

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"os"
	"reflect"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/trezor/blockbook/bchain"
	"github.com/trezor/blockbook/bchain/coins/btc"
	"github.com/trezor/blockbook/bchain/coins/coordinate"
)

// ═══════════════════════════════════════════════════════════════════════════
// Test helpers
// ═══════════════════════════════════════════════════════════════════════════

func coordinateTestParser() *coordinate.CoordinateParser {
	return coordinate.NewCoordinateParser(
		coordinate.GetChainParams("regtest"),
		&btc.Configuration{BlockAddressesToKeep: 1},
	)
}

func setupCoordinateDB(t *testing.T) *RocksDB {
	t.Helper()
	tmp, err := os.MkdirTemp("", "testdb_coord")
	if err != nil {
		t.Fatal(err)
	}
	p := coordinateTestParser()
	d, err := NewRocksDB(tmp, 100000, -1, p, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func closeAndDestroyCoordinateDB(t *testing.T, d *RocksDB) {
	t.Helper()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(d.path)
}

func mustHexDecode(h string) []byte {
	b, err := hex.DecodeString(h)
	if err != nil {
		panic(err)
	}
	return b
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: IsAssetAware
// ═══════════════════════════════════════════════════════════════════════════

func TestCoordinate_IsAssetAware(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	if !d.IsAssetAware() {
		t.Error("IsAssetAware() = false for Coordinate parser, want true")
	}
}

func TestBitcoin_IsNotAssetAware(t *testing.T) {
	// We can't create a Bitcoin RocksDB here because Coordinate's init()
	// already registered params with the same PubKeyHashAddrID (111).
	// Instead, verify the interface check that NewRocksDB uses:

	// 1. CoordinateParser implements AssetSupporter → SupportsAssets()=true
	p := coordinateTestParser()
	as, ok := interface{}(p).(AssetSupporter)
	if !ok {
		t.Fatal("CoordinateParser should implement AssetSupporter")
	}
	if !as.SupportsAssets() {
		t.Error("CoordinateParser.SupportsAssets() = false, want true")
	}

	// 2. A plain BitcoinLikeParser does NOT implement AssetSupporter
	//    This is what makes IsAssetAware()=false for Bitcoin chains
	var plainParser bchain.BlockChainParser = p.BitcoinLikeParser
	_, ok = plainParser.(AssetSupporter)
	if ok {
		t.Error("BitcoinLikeParser should NOT implement AssetSupporter")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: packDescHeight / unpackDescHeight
// ═══════════════════════════════════════════════════════════════════════════

func TestPackDescHeight(t *testing.T) {
	tests := []struct {
		height uint32
	}{
		{0},
		{1},
		{100},
		{500000},
		{0xFFFFFFFF},
		{0xFFFFFFFE},
	}
	for _, tt := range tests {
		packed := packDescHeight(tt.height)
		if len(packed) != 4 {
			t.Errorf("packDescHeight(%d) len = %d, want 4", tt.height, len(packed))
		}
		got := unpackDescHeight(packed)
		if got != tt.height {
			t.Errorf("unpackDescHeight(packDescHeight(%d)) = %d", tt.height, got)
		}
	}

	// Verify descending order: lower height → higher packed value
	h100 := packDescHeight(100)
	h200 := packDescHeight(200)
	if bytes.Compare(h100, h200) <= 0 {
		t.Error("packDescHeight(100) should be > packDescHeight(200) for descending iteration")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: uitoa / opKey
// ═══════════════════════════════════════════════════════════════════════════

func TestUitoa(t *testing.T) {
	tests := []struct {
		v    uint32
		want string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{12345, "12345"},
		{4294967295, "4294967295"},
	}
	for _, tt := range tests {
		got := uitoa(tt.v)
		if got != tt.want {
			t.Errorf("uitoa(%d) = %q, want %q", tt.v, got, tt.want)
		}
	}
}

func TestOpKey(t *testing.T) {
	got := opKey("abc123", 7)
	if got != "abc123:7" {
		t.Errorf("opKey('abc123', 7) = %q, want 'abc123:7'", got)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Controller outpoint pack / unpack / format / parse
// ═══════════════════════════════════════════════════════════════════════════

// Known txid for deterministic tests
const testTxid1 = "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb"
const testTxid2 = "1111111111111111111111111111111111111111111111111111111111111111"

func TestPackUnpackControllerOutpoint(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	tests := []struct {
		txid string
		vout uint32
	}{
		{testTxid1, 0},
		{testTxid1, 1},
		{testTxid2, 255},
		{testTxid2, 65535},
	}
	for _, tt := range tests {
		packed, err := d.packControllerOutpoint(tt.txid, tt.vout)
		if err != nil {
			t.Fatalf("packControllerOutpoint(%s, %d) error: %v", tt.txid, tt.vout, err)
		}

		gotTxid, gotVout := d.unpackControllerOutpoint(packed)
		if gotTxid != tt.txid {
			t.Errorf("unpack txid = %s, want %s", gotTxid, tt.txid)
		}
		if gotVout != tt.vout {
			t.Errorf("unpack vout = %d, want %d", gotVout, tt.vout)
		}
	}
}

func TestFormatControllerOutpoint(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	packed, _ := d.packControllerOutpoint(testTxid1, 0)
	got := d.FormatControllerOutpoint(packed)
	want := testTxid1 + ":0"
	if got != want {
		t.Errorf("FormatControllerOutpoint = %q, want %q", got, want)
	}

	// Nil controller
	if d.FormatControllerOutpoint(nil) != "" {
		t.Error("FormatControllerOutpoint(nil) should return empty")
	}
}

func TestParseControllerString(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	// Round-trip: pack → format → parse → compare
	original, _ := d.packControllerOutpoint(testTxid1, 0)
	formatted := d.FormatControllerOutpoint(original)
	parsed, err := d.ParseControllerString(formatted)
	if err != nil {
		t.Fatalf("ParseControllerString error: %v", err)
	}
	if !bytes.Equal(original, parsed) {
		t.Errorf("ParseControllerString round-trip failed:\n  original: %x\n  parsed:   %x", original, parsed)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: AssetRegistryEntry pack / unpack
// ═══════════════════════════════════════════════════════════════════════════

func TestPackUnpackAssetRegistryEntry_Normal(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)
	entry := &AssetRegistryEntry{
		Ticker:            "GOLD",
		Headline:          "Digital Gold Token",
		Precision:         4,
		AssetType:         0,
		TotalSupply:       *big.NewInt(100000000),
		CurrentController: ctrl,
		IsRedirect:        false,
	}

	packed := d.packAssetRegistryEntry(entry)
	if packed[0] != 0 {
		t.Fatalf("type byte = %d, want 0 for normal entry", packed[0])
	}

	got, err := d.unpackAssetRegistryEntry(packed)
	if err != nil {
		t.Fatalf("unpack error: %v", err)
	}

	if got.Ticker != "GOLD" {
		t.Errorf("Ticker = %q, want GOLD", got.Ticker)
	}
	if got.Headline != "Digital Gold Token" {
		t.Errorf("Headline = %q, want 'Digital Gold Token'", got.Headline)
	}
	if got.Precision != 4 {
		t.Errorf("Precision = %d, want 4", got.Precision)
	}
	if got.AssetType != 0 {
		t.Errorf("AssetType = %d, want 0", got.AssetType)
	}
	if got.TotalSupply.Cmp(big.NewInt(100000000)) != 0 {
		t.Errorf("TotalSupply = %s, want 100000000", got.TotalSupply.String())
	}
	if !bytes.Equal(got.CurrentController, ctrl) {
		t.Errorf("CurrentController mismatch")
	}
	if got.IsRedirect {
		t.Error("IsRedirect = true, want false")
	}
}

func TestPackUnpackAssetRegistryEntry_Redirect(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	newCtrl, _ := d.packControllerOutpoint(testTxid2, 0)
	redirect := &AssetRegistryEntry{
		IsRedirect:        true,
		CurrentController: newCtrl,
	}

	packed := d.packAssetRegistryEntry(redirect)
	if packed[0] != 1 {
		t.Fatalf("type byte = %d, want 1 for redirect", packed[0])
	}

	got, err := d.unpackAssetRegistryEntry(packed)
	if err != nil {
		t.Fatalf("unpack error: %v", err)
	}
	if !got.IsRedirect {
		t.Error("IsRedirect = false, want true")
	}
	if !bytes.Equal(got.CurrentController, newCtrl) {
		t.Error("CurrentController mismatch on redirect")
	}
}

func TestPackUnpackAssetRegistryEntry_Empty(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	got, err := d.unpackAssetRegistryEntry(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("unpackAssetRegistryEntry(nil) should return nil")
	}

	got, err = d.unpackAssetRegistryEntry([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("unpackAssetRegistryEntry(empty) should return nil")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: AddrAssetBalance pack / unpack
// ═══════════════════════════════════════════════════════════════════════════

func TestPackUnpackAddrAssetBalance(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	tests := []struct {
		name string
		ab   *AddrAssetBalance
	}{
		{
			name: "normal",
			ab: &AddrAssetBalance{
				Txs:        5,
				BalanceSat: *big.NewInt(100000000),
				SentSat:    *big.NewInt(50000000),
			},
		},
		{
			name: "zero",
			ab: &AddrAssetBalance{
				Txs: 0,
			},
		},
		{
			name: "large values",
			ab: &AddrAssetBalance{
				Txs:        999999,
				BalanceSat: *big.NewInt(9000000000000000),
				SentSat:    *big.NewInt(8000000000000000),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := d.packAddrAssetBalance(tt.ab)
			got, err := d.unpackAddrAssetBalance(packed)
			if err != nil {
				t.Fatalf("unpack error: %v", err)
			}
			if got.Txs != tt.ab.Txs {
				t.Errorf("Txs = %d, want %d", got.Txs, tt.ab.Txs)
			}
			if got.BalanceSat.Cmp(&tt.ab.BalanceSat) != 0 {
				t.Errorf("BalanceSat = %s, want %s", got.BalanceSat.String(), tt.ab.BalanceSat.String())
			}
			if got.SentSat.Cmp(&tt.ab.SentSat) != 0 {
				t.Errorf("SentSat = %s, want %s", got.SentSat.String(), tt.ab.SentSat.String())
			}
		})
	}
}

func TestUnpackAddrAssetBalance_NilEmpty(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	got, err := d.unpackAddrAssetBalance(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("unpackAddrAssetBalance(nil) should return nil")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: packAddrBalance with assetAware = true (Controller round-trip)
// ═══════════════════════════════════════════════════════════════════════════

func TestPackAddrBalance_AssetAware(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	txidLen := d.chainParser.PackedTxidLen()
	btxID1, _ := d.chainParser.PackTxid(testTxid1)
	btxID2, _ := d.chainParser.PackTxid(testTxid2)
	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)

	ab := &AddrBalance{
		Txs:        3,
		SentSat:    *big.NewInt(50000),
		BalanceSat: *big.NewInt(150000),
		Utxos: []Utxo{
			{
				// Regular BTC UTXO — no controller
				BtxID:    btxID1,
				Vout:     0,
				Height:   100,
				ValueSat: *big.NewInt(50000),
			},
			{
				// Asset UTXO — with controller
				BtxID:        btxID1,
				Vout:         1,
				Height:       100,
				ValueSat:     *big.NewInt(100000),
				Controller:   ctrl,
				IsController: false,
			},
			{
				// Controller UTXO
				BtxID:        btxID2,
				Vout:         0,
				Height:       100,
				ValueSat:     *big.NewInt(0),
				Controller:   ctrl,
				IsController: true,
			},
		},
	}

	varBuf := make([]byte, maxPackedBigintBytes)
	buf := make([]byte, 32)

	packed := packAddrBalance(ab, buf, varBuf, true) // assetAware=true

	got, err := unpackAddrBalance(packed, txidLen, AddressBalanceDetailUTXO, true)
	if err != nil {
		t.Fatalf("unpackAddrBalance error: %v", err)
	}

	if got.Txs != 3 {
		t.Errorf("Txs = %d, want 3", got.Txs)
	}
	if got.BalanceSat.Cmp(big.NewInt(150000)) != 0 {
		t.Errorf("BalanceSat = %s, want 150000", got.BalanceSat.String())
	}
	if len(got.Utxos) != 3 {
		t.Fatalf("len(Utxos) = %d, want 3", len(got.Utxos))
	}

	// UTXO 0: BTC, no controller
	if len(got.Utxos[0].Controller) != 0 {
		t.Errorf("Utxo[0].Controller = %x, want nil", got.Utxos[0].Controller)
	}
	if got.Utxos[0].IsController {
		t.Error("Utxo[0].IsController = true, want false")
	}

	// UTXO 1: Asset supply, has controller, IsController=false
	if !bytes.Equal(got.Utxos[1].Controller, ctrl) {
		t.Errorf("Utxo[1].Controller = %x, want %x", got.Utxos[1].Controller, ctrl)
	}
	if got.Utxos[1].IsController {
		t.Error("Utxo[1].IsController = true, want false")
	}
	if got.Utxos[1].ValueSat.Cmp(big.NewInt(100000)) != 0 {
		t.Errorf("Utxo[1].ValueSat = %s, want 100000", got.Utxos[1].ValueSat.String())
	}

	// UTXO 2: Controller coin, IsController=true
	if !bytes.Equal(got.Utxos[2].Controller, ctrl) {
		t.Errorf("Utxo[2].Controller = %x, want %x", got.Utxos[2].Controller, ctrl)
	}
	if !got.Utxos[2].IsController {
		t.Error("Utxo[2].IsController = false, want true")
	}
}

// Verify non-assetAware round-trip doesn't break (no Controller persisted)
func TestPackAddrBalance_NonAssetAware(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	txidLen := d.chainParser.PackedTxidLen()
	btxID1, _ := d.chainParser.PackTxid(testTxid1)
	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)

	ab := &AddrBalance{
		Txs:        1,
		SentSat:    *big.NewInt(0),
		BalanceSat: *big.NewInt(100),
		Utxos: []Utxo{
			{
				BtxID:      btxID1,
				Vout:       0,
				Height:     1,
				ValueSat:   *big.NewInt(100),
				Controller: ctrl, // set but assetAware=false so not serialized
			},
		},
	}

	varBuf := make([]byte, maxPackedBigintBytes)
	buf := make([]byte, 32)

	packed := packAddrBalance(ab, buf, varBuf, false)
	got, err := unpackAddrBalance(packed, txidLen, AddressBalanceDetailUTXO, false)
	if err != nil {
		t.Fatalf("unpackAddrBalance error: %v", err)
	}

	// Controller should NOT be preserved
	if len(got.Utxos[0].Controller) != 0 {
		t.Errorf("Controller should be nil when assetAware=false, got %x", got.Utxos[0].Controller)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: packAssetTxEntry
// ═══════════════════════════════════════════════════════════════════════════

func TestPackAssetTxEntry(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	btxID, _ := d.chainParser.PackTxid(testTxid1)
	indexes := []int32{0, 1, 5}
	packed := d.packAssetTxEntry(btxID, indexes)

	// Should start with the packed txid
	if !bytes.HasPrefix(packed, btxID) {
		t.Error("packAssetTxEntry should start with btxID")
	}

	// The packed data is cfAddresses-compatible, so unpackTxIndexes should read it
	txi, err := d.unpackTxIndexes(packed)
	if err != nil {
		t.Fatalf("unpackTxIndexes error: %v", err)
	}
	if len(txi) != 1 {
		t.Fatalf("len(txi) = %d, want 1", len(txi))
	}
	if !bytes.Equal(txi[0].btxID, btxID) {
		t.Error("btxID mismatch")
	}
	if !reflect.DeepEqual(txi[0].indexes, indexes) {
		t.Errorf("indexes = %v, want %v", txi[0].indexes, indexes)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: appendVarint32
// ═══════════════════════════════════════════════════════════════════════════

func TestPackVarint32_RoundTrip(t *testing.T) {
	// Verify packVarint32/unpackVarint32 round-trip for the shifted+marker
	// values that packAssetTxEntry produces
	tests := []int32{0, 1, 2, 5, 63, 64, 127, 128, 8191, 8192, 1048575}
	for _, idx := range tests {
		// Simulate what packAssetTxEntry does: shift left, set last-marker
		v := (idx << 1) | 1
		buf := make([]byte, 10)
		l := packVarint32(v, buf)
		got, gl := unpackVarint32(buf[:l])
		if got != v {
			t.Errorf("packVarint32/unpackVarint32(%d): got %d, want %d", v, got, v)
		}
		if gl != l {
			t.Errorf("consumed %d bytes, packed %d", gl, l)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: DB registry write + read + resolve redirect chain
// ═══════════════════════════════════════════════════════════════════════════

func TestAssetRegistry_WriteReadResolve(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	ctrlA, _ := d.packControllerOutpoint(testTxid1, 0) // original
	ctrlB, _ := d.packControllerOutpoint(testTxid2, 0) // mint-more

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	// Write original entry at ctrlA
	entryA := &AssetRegistryEntry{
		Ticker:            "GOLD",
		Headline:          "Gold Token",
		Precision:         4,
		TotalSupply:       *big.NewInt(1000000),
		CurrentController: ctrlA,
	}
	keyA := append([]byte(assetRegistryPrefix), ctrlA...)
	wb.PutCF(d.cfh[cfDefault], keyA, d.packAssetRegistryEntry(entryA))

	// Write redirect: ctrlA → ctrlB
	redirect := &AssetRegistryEntry{IsRedirect: true, CurrentController: ctrlB}
	wb.PutCF(d.cfh[cfDefault], keyA, d.packAssetRegistryEntry(redirect))

	// Write new entry at ctrlB
	entryB := &AssetRegistryEntry{
		Ticker:            "GOLD",
		Headline:          "Gold Token",
		Precision:         4,
		TotalSupply:       *big.NewInt(2000000),
		CurrentController: ctrlB,
	}
	keyB := append([]byte(assetRegistryPrefix), ctrlB...)
	wb.PutCF(d.cfh[cfDefault], keyB, d.packAssetRegistryEntry(entryB))

	err := d.db.Write(d.wo, wb)
	if err != nil {
		t.Fatal(err)
	}

	// Read ctrlA — should be redirect
	gotA, err := d.GetAssetRegistryEntry(ctrlA)
	if err != nil {
		t.Fatal(err)
	}
	if !gotA.IsRedirect {
		t.Error("ctrlA should be redirect")
	}

	// Read ctrlB — should be normal
	gotB, err := d.GetAssetRegistryEntry(ctrlB)
	if err != nil {
		t.Fatal(err)
	}
	if gotB.IsRedirect {
		t.Error("ctrlB should not be redirect")
	}
	if gotB.Ticker != "GOLD" {
		t.Errorf("ctrlB.Ticker = %q, want GOLD", gotB.Ticker)
	}
	if gotB.TotalSupply.Cmp(big.NewInt(2000000)) != 0 {
		t.Errorf("ctrlB.TotalSupply = %s, want 2000000", gotB.TotalSupply.String())
	}

	// ResolveCurrentController: ctrlA → follows redirect → ctrlB
	resolved := d.ResolveCurrentController(ctrlA)
	if !bytes.Equal(resolved, ctrlB) {
		t.Errorf("ResolveCurrentController(A) = %x, want %x (B)", resolved, ctrlB)
	}

	// ResolveCurrentController: ctrlB → stays ctrlB
	resolved2 := d.ResolveCurrentController(ctrlB)
	if !bytes.Equal(resolved2, ctrlB) {
		t.Errorf("ResolveCurrentController(B) = %x, want %x (B)", resolved2, ctrlB)
	}

	// ResolveCurrentController: unknown → returns itself
	ctrlUnknown := []byte{0xff, 0xff}
	resolved3 := d.ResolveCurrentController(ctrlUnknown)
	if !bytes.Equal(resolved3, ctrlUnknown) {
		t.Errorf("ResolveCurrentController(unknown) should return itself")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Per-address asset balance DB read/write
// ═══════════════════════════════════════════════════════════════════════════

func TestAddrAssetBalance_WriteRead(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)
	addrDesc := bchain.AddressDescriptor(mustHexDecode("0014751e76e8199196d454941c45d1b3a323f1433bd6"))

	// Should be nil before write
	got, err := d.GetAddrAssetBalance(addrDesc, ctrl)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("should be nil before write")
	}

	// Write
	wb := grocksdb.NewWriteBatch()
	aab := &AddrAssetBalance{
		Txs:        5,
		BalanceSat: *big.NewInt(12345678),
		SentSat:    *big.NewInt(87654321),
	}
	key := d.makeAddrAssetKey(addrDesc, ctrl)
	wb.PutCF(d.cfh[cfDefault], key, d.packAddrAssetBalance(aab))
	if err := d.db.Write(d.wo, wb); err != nil {
		t.Fatal(err)
	}
	wb.Destroy()

	// Read back
	got, err = d.GetAddrAssetBalance(addrDesc, ctrl)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("got nil after write")
	}
	if got.Txs != 5 {
		t.Errorf("Txs = %d, want 5", got.Txs)
	}
	if got.BalanceSat.Cmp(big.NewInt(12345678)) != 0 {
		t.Errorf("BalanceSat = %s, want 12345678", got.BalanceSat.String())
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: GetAddrDescAssets — lists all assets for an address
// ═══════════════════════════════════════════════════════════════════════════

func TestGetAddrDescAssets(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	addrDesc := bchain.AddressDescriptor(mustHexDecode("0014aaaa"))
	ctrlA, _ := d.packControllerOutpoint(testTxid1, 0)
	ctrlB, _ := d.packControllerOutpoint(testTxid2, 0)

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	aab1 := &AddrAssetBalance{Txs: 3, BalanceSat: *big.NewInt(100)}
	aab2 := &AddrAssetBalance{Txs: 7, BalanceSat: *big.NewInt(200)}

	wb.PutCF(d.cfh[cfDefault], d.makeAddrAssetKey(addrDesc, ctrlA), d.packAddrAssetBalance(aab1))
	wb.PutCF(d.cfh[cfDefault], d.makeAddrAssetKey(addrDesc, ctrlB), d.packAddrAssetBalance(aab2))
	if err := d.db.Write(d.wo, wb); err != nil {
		t.Fatal(err)
	}

	assets, err := d.GetAddrDescAssets(addrDesc)
	if err != nil {
		t.Fatal(err)
	}
	if len(assets) != 2 {
		t.Fatalf("len(assets) = %d, want 2", len(assets))
	}

	// Verify both controllers found (order may vary by key sort)
	foundA, foundB := false, false
	for _, a := range assets {
		if bytes.Equal(a.Controller, ctrlA) {
			foundA = true
			if a.Balance.Txs != 3 {
				t.Errorf("ctrlA Txs = %d, want 3", a.Balance.Txs)
			}
		}
		if bytes.Equal(a.Controller, ctrlB) {
			foundB = true
			if a.Balance.Txs != 7 {
				t.Errorf("ctrlB Txs = %d, want 7", a.Balance.Txs)
			}
		}
	}
	if !foundA {
		t.Error("ctrlA not found")
	}
	if !foundB {
		t.Error("ctrlB not found")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Key prefix isolation — different addresses don't leak
// ═══════════════════════════════════════════════════════════════════════════

func TestAddrAssetKeyIsolation(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	addr1 := bchain.AddressDescriptor(mustHexDecode("0014aaaa"))
	addr2 := bchain.AddressDescriptor(mustHexDecode("0014bbbb"))
	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	wb.PutCF(d.cfh[cfDefault], d.makeAddrAssetKey(addr1, ctrl),
		d.packAddrAssetBalance(&AddrAssetBalance{Txs: 1, BalanceSat: *big.NewInt(100)}))
	wb.PutCF(d.cfh[cfDefault], d.makeAddrAssetKey(addr2, ctrl),
		d.packAddrAssetBalance(&AddrAssetBalance{Txs: 2, BalanceSat: *big.NewInt(200)}))
	d.db.Write(d.wo, wb)

	// addr1 should only see its own asset
	assets1, _ := d.GetAddrDescAssets(addr1)
	if len(assets1) != 1 {
		t.Errorf("addr1 assets count = %d, want 1", len(assets1))
	}
	if assets1[0].Balance.Txs != 1 {
		t.Errorf("addr1 Txs = %d, want 1", assets1[0].Balance.Txs)
	}

	// addr2 should only see its own asset
	assets2, _ := d.GetAddrDescAssets(addr2)
	if len(assets2) != 1 {
		t.Errorf("addr2 assets count = %d, want 1", len(assets2))
	}
	if assets2[0].Balance.Txs != 2 {
		t.Errorf("addr2 Txs = %d, want 2", assets2[0].Balance.Txs)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Global asset tx history write + read
// ═══════════════════════════════════════════════════════════════════════════

func TestGlobalAssetTxHistory(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)
	btxID1, _ := d.chainParser.PackTxid(testTxid1)
	btxID2, _ := d.chainParser.PackTxid(testTxid2)

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	// Write tx at height 100
	val1 := d.packAssetTxEntry(btxID1, []int32{0, 1})
	key1 := d.makeGlobalAssetTxKey(ctrl, 100)
	wb.PutCF(d.cfh[cfDefault], key1, val1)

	// Write tx at height 200
	val2 := d.packAssetTxEntry(btxID2, []int32{0})
	key2 := d.makeGlobalAssetTxKey(ctrl, 200)
	wb.PutCF(d.cfh[cfDefault], key2, val2)

	d.db.Write(d.wo, wb)

	// Read back all txs (height 0 to maxUint32)
	var txids []string
	var heights []uint32
	err := d.GetAssetTransactions(ctrl, 0, 0xFFFFFFFF, func(txid string, height uint32, indexes []int32) error {
		txids = append(txids, txid)
		heights = append(heights, height)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(txids) != 2 {
		t.Fatalf("got %d txids, want 2", len(txids))
	}

	// Should be newest first (descending height)
	if heights[0] != 200 {
		t.Errorf("first height = %d, want 200 (newest first)", heights[0])
	}
	if heights[1] != 100 {
		t.Errorf("second height = %d, want 100", heights[1])
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: Per-address per-asset tx history
// ═══════════════════════════════════════════════════════════════════════════

func TestAddrAssetTxHistory(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	addrDesc := bchain.AddressDescriptor(mustHexDecode("0014aaaa"))
	ctrl, _ := d.packControllerOutpoint(testTxid1, 0)
	btxID, _ := d.chainParser.PackTxid(testTxid1)

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()

	val := d.packAssetTxEntry(btxID, []int32{0, 1})
	key := d.makeAddrAssetTxKey(addrDesc, ctrl, 150)
	wb.PutCF(d.cfh[cfDefault], key, val)
	d.db.Write(d.wo, wb)

	var count int
	err := d.GetAddrDescAssetTransactions(addrDesc, ctrl, 0, 0xFFFFFFFF,
		func(txid string, height uint32, indexes []int32) error {
			count++
			if txid != testTxid1 {
				t.Errorf("txid = %s, want %s", txid, testTxid1)
			}
			if height != 150 {
				t.Errorf("height = %d, want 150", height)
			}
			if !reflect.DeepEqual(indexes, []int32{0, 1}) {
				t.Errorf("indexes = %v, want [0 1]", indexes)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("callback count = %d, want 1", count)
	}

	// Height filter: only 100-140 should return nothing
	count = 0
	d.GetAddrDescAssetTransactions(addrDesc, ctrl, 100, 140,
		func(txid string, height uint32, indexes []int32) error {
			count++
			return nil
		})
	if count != 0 {
		t.Errorf("height filter [100,140] should find 0, got %d", count)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test: fillAssetMetadataFromTx
// ═══════════════════════════════════════════════════════════════════════════

func TestFillAssetMetadataFromTx(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	// Simulate CoinSpecificData as json.RawMessage (what coordinaterpc stores)
	tx := &bchain.Tx{
		CoinSpecificData: []byte(`{"ticker":"SILVER","headline":"Silver Token","precision":6,"assettype":1}`),
	}
	entry := &AssetRegistryEntry{Precision: 8}
	d.fillAssetMetadataFromTx(tx, entry)

	if entry.Ticker != "SILVER" {
		t.Errorf("Ticker = %q, want SILVER", entry.Ticker)
	}
	if entry.Headline != "Silver Token" {
		t.Errorf("Headline = %q, want 'Silver Token'", entry.Headline)
	}
	if entry.Precision != 6 {
		t.Errorf("Precision = %d, want 6", entry.Precision)
	}
	if entry.AssetType != 1 {
		t.Errorf("AssetType = %d, want 1", entry.AssetType)
	}
}

func TestFillAssetMetadataFromTx_NilData(t *testing.T) {
	d := setupCoordinateDB(t)
	defer closeAndDestroyCoordinateDB(t, d)

	tx := &bchain.Tx{CoinSpecificData: nil}
	entry := &AssetRegistryEntry{Precision: 8}
	d.fillAssetMetadataFromTx(tx, entry)

	// Should not crash, precision stays default
	if entry.Precision != 8 {
		t.Errorf("Precision = %d, want 8 (unchanged)", entry.Precision)
	}
}