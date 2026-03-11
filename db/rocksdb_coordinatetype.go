package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math/big"
    "github.com/bsm/go-vlq"
	"github.com/golang/glog"
	"github.com/linxGnu/grocksdb"
	"github.com/trezor/blockbook/bchain"
)

// IsAssetAware returns true if asset UTXO tracking is enabled.
func (d *RocksDB) IsAssetAware() bool {
	return d.assetAware
}

// ==========================================================================
// Coordinate Asset Indexing for Blockbook
//
// Storage layout (all in cfDefault with key prefixes):
//
//   "ac:" + packedController
//     → Asset registry: ticker, precision, assetType, totalSupply, currentCtrl
//     → Or redirect entry pointing to new controller
//
//   "aa:" + addrDesc + packedController
//     → Per-address asset balance: txCount, balanceSat, sentSat
//
//   "ax:" + addrDesc + packedController + descHeight(4B)
//     → Per-address per-asset tx history (same format as cfAddresses)
//
//   "gt:" + packedController + descHeight(4B)
//     → Global asset tx history (same format as cfAddresses)
//
// Heights stored descending (^height) so iteration gives newest first.
// ==========================================================================

const (
	assetRegistryPrefix = "ac:"
	addrAssetPrefix     = "aa:"
	addrAssetTxPrefix   = "ax:"
	globalAssetTxPrefix = "gt:"
)

// ---------------------------------------------------------------------------
// Controller outpoint encoding: packedTxid + varuint(vout)
// ---------------------------------------------------------------------------

func (d *RocksDB) packControllerOutpoint(txid string, vout uint32) ([]byte, error) {
	btxID, err := d.chainParser.PackTxid(txid)
	if err != nil {
		return nil, err
	}
	var varBuf [maxPackedBigintBytes]byte
	l := packVaruint(uint(vout), varBuf[:])
	out := make([]byte, len(btxID)+l)
	copy(out, btxID)
	copy(out[len(btxID):], varBuf[:l])
	return out, nil
}

func (d *RocksDB) unpackControllerOutpoint(controller []byte) (string, uint32) {
	if len(controller) == 0 {
		return "", 0
	}
	txidLen := d.chainParser.PackedTxidLen()
	if len(controller) < txidLen+1 {
		return "", 0
	}
	txid, err := d.chainParser.UnpackTxid(controller[:txidLen])
	if err != nil {
		return "", 0
	}
	vout, _ := unpackVaruint(controller[txidLen:])
	return txid, uint32(vout)
}

// FormatControllerOutpoint converts packed controller → "txid:vout" string.
// Called by api/worker.go for UTXO and address responses.
func (d *RocksDB) FormatControllerOutpoint(controller []byte) string {
	if len(controller) == 0 {
		return ""
	}
	txid, vout := d.unpackControllerOutpoint(controller)
	if txid == "" {
		return hex.EncodeToString(controller)
	}
	return txid + ":" + uitoa(vout)
}

// ParseControllerString converts "txid:vout" string → packed bytes.
// Called by api/worker.go when parsing ?contract= query parameter.
func (d *RocksDB) ParseControllerString(s string) ([]byte, error) {
	idx := len(s) - 1
	for idx >= 0 && s[idx] != ':' {
		idx--
	}
	if idx <= 0 {
		return nil, nil
	}
	txid := s[:idx]
	var vout uint32
	for _, c := range s[idx+1:] {
		if c >= '0' && c <= '9' {
			vout = vout*10 + uint32(c-'0')
		}
	}
	return d.packControllerOutpoint(txid, vout)
}

// ---------------------------------------------------------------------------
// Descending height (for newest-first iteration)
// ---------------------------------------------------------------------------

func packDescHeight(height uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, ^height)
	return buf
}

func unpackDescHeight(buf []byte) uint32 {
	return ^binary.BigEndian.Uint32(buf)
}

// ---------------------------------------------------------------------------
// Asset registry entry
// ---------------------------------------------------------------------------

type AssetRegistryEntry struct {
	Ticker            string
	Headline          string
	Precision         int32
	AssetType         int32
	TotalSupply       big.Int
	CurrentController []byte
	IsRedirect        bool
}

func (d *RocksDB) packAssetRegistryEntry(e *AssetRegistryEntry) []byte {
	if e.IsRedirect {
		buf := []byte{1}
		return append(buf, e.CurrentController...)
	}
	buf := []byte{0}
	var varBuf [maxPackedBigintBytes]byte
	// ticker
	l := packVaruint(uint(len(e.Ticker)), varBuf[:])
	buf = append(buf, varBuf[:l]...)
	buf = append(buf, []byte(e.Ticker)...)
	// headline
	l = packVaruint(uint(len(e.Headline)), varBuf[:])
	buf = append(buf, varBuf[:l]...)
	buf = append(buf, []byte(e.Headline)...)
	// precision
	l = packVaruint(uint(e.Precision), varBuf[:])
	buf = append(buf, varBuf[:l]...)
	// assetType
	l = packVaruint(uint(e.AssetType), varBuf[:])
	buf = append(buf, varBuf[:l]...)
	// totalSupply
	l = packBigint(&e.TotalSupply, varBuf[:])
	buf = append(buf, varBuf[:l]...)
	// currentController
	l = packVaruint(uint(len(e.CurrentController)), varBuf[:])
	buf = append(buf, varBuf[:l]...)
	buf = append(buf, e.CurrentController...)
	return buf
}

func (d *RocksDB) unpackAssetRegistryEntry(data []byte) (*AssetRegistryEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	e := &AssetRegistryEntry{}
	if data[0] == 1 {
		e.IsRedirect = true
		e.CurrentController = append([]byte(nil), data[1:]...)
		return e, nil
	}
	p := 1
	tLen, l := unpackVaruint(data[p:])
	p += l
	e.Ticker = string(data[p : p+int(tLen)])
	p += int(tLen)

	hLen, l := unpackVaruint(data[p:])
	p += l
	e.Headline = string(data[p : p+int(hLen)])
	p += int(hLen)

	precision, l := unpackVaruint(data[p:])
	p += l
	e.Precision = int32(precision)

	assetType, l := unpackVaruint(data[p:])
	p += l
	e.AssetType = int32(assetType)

	e.TotalSupply, l = unpackBigint(data[p:])
	p += l

	ctrlLen, l := unpackVaruint(data[p:])
	p += l
	e.CurrentController = append([]byte(nil), data[p:p+int(ctrlLen)]...)
	return e, nil
}

// GetAssetRegistryEntry returns metadata for an asset by its controller outpoint.
func (d *RocksDB) GetAssetRegistryEntry(controller []byte) (*AssetRegistryEntry, error) {
	key := append([]byte(assetRegistryPrefix), controller...)
	val, err := d.db.GetCF(d.ro, d.cfh[cfDefault], key)
	if err != nil {
		glog.Warningf("ASSET-DEBUG GetRegistryEntry: error=%v keyHex=%s", err, hex.EncodeToString(key))
		return nil, err
	}
	defer val.Free()
	if val.Data() == nil {
		glog.V(1).Infof("ASSET-DEBUG GetRegistryEntry: NOT FOUND keyHex=%s ctrlHex=%s", hex.EncodeToString(key), hex.EncodeToString(controller))
		return nil, nil
	}
	entry, err := d.unpackAssetRegistryEntry(val.Data())
	if err == nil && entry != nil {
		glog.V(1).Infof("ASSET-DEBUG GetRegistryEntry: FOUND ticker=%q headline=%q redirect=%v", entry.Ticker, entry.Headline, entry.IsRedirect)
	}
	return entry, err
}

// ResolveCurrentController follows redirect chain → current controller.
func (d *RocksDB) ResolveCurrentController(controller []byte) []byte {
	current := controller
	for i := 0; i < 100; i++ {
		entry, err := d.GetAssetRegistryEntry(current)
		if err != nil || entry == nil {
			return current
		}
		if !entry.IsRedirect {
			return entry.CurrentController
		}
		if bytes.Equal(entry.CurrentController, current) {
			return current
		}
		current = entry.CurrentController
	}
	return current
}

// ---------------------------------------------------------------------------
// Per-address asset balance
// ---------------------------------------------------------------------------

// AddrAssetBalance stores per-address per-asset balance and tx count.
type AddrAssetBalance struct {
	Txs        uint32
	BalanceSat big.Int
	SentSat    big.Int
}

func (d *RocksDB) packAddrAssetBalance(ab *AddrAssetBalance) []byte {
	var varBuf [maxPackedBigintBytes]byte
	buf := make([]byte, 0, 32)
	l := packVaruint(uint(ab.Txs), varBuf[:])
	buf = append(buf, varBuf[:l]...)
	l = packBigint(&ab.BalanceSat, varBuf[:])
	buf = append(buf, varBuf[:l]...)
	l = packBigint(&ab.SentSat, varBuf[:])
	buf = append(buf, varBuf[:l]...)
	return buf
}

func (d *RocksDB) unpackAddrAssetBalance(data []byte) (*AddrAssetBalance, error) {
	if len(data) == 0 {
		return nil, nil
	}
	ab := &AddrAssetBalance{}
	txs, l := unpackVaruint(data)
	ab.Txs = uint32(txs)
	var l2 int
	ab.BalanceSat, l2 = unpackBigint(data[l:])
	ab.SentSat, _ = unpackBigint(data[l+l2:])
	return ab, nil
}

func (d *RocksDB) makeAddrAssetKey(addrDesc bchain.AddressDescriptor, controller []byte) []byte {
	key := make([]byte, 0, len(addrAssetPrefix)+len(addrDesc)+len(controller))
	key = append(key, []byte(addrAssetPrefix)...)
	key = append(key, addrDesc...)
	key = append(key, controller...)
	return key
}

// GetAddrAssetBalance returns balance for one address+asset pair.
// Accepts either address string or addrDesc bytes.
func (d *RocksDB) GetAddrAssetBalance(addrDesc bchain.AddressDescriptor, controller []byte) (*AddrAssetBalance, error) {
	key := d.makeAddrAssetKey(addrDesc, controller)
	val, err := d.db.GetCF(d.ro, d.cfh[cfDefault], key)
	if err != nil {
		return nil, err
	}
	defer val.Free()
	if val.Data() == nil {
		return nil, nil
	}
	return d.unpackAddrAssetBalance(val.Data())
}

// AddrAssetInfo pairs a packed controller with its per-address balance.
type AddrAssetInfo struct {
	Controller []byte
	Balance    *AddrAssetBalance
}

// GetAddrDescAssets returns ALL assets held by an address with balances.
// This is used to build the Token list in the address API response.
func (d *RocksDB) GetAddrDescAssets(addrDesc bchain.AddressDescriptor) ([]*AddrAssetInfo, error) {
	prefix := make([]byte, 0, len(addrAssetPrefix)+len(addrDesc))
	prefix = append(prefix, []byte(addrAssetPrefix)...)
	prefix = append(prefix, addrDesc...)

	result := make([]*AddrAssetInfo, 0, 4)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := d.db.NewIteratorCF(ro, d.cfh[cfDefault])
	defer it.Close()

	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Key().Data()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		controller := append([]byte(nil), key[len(prefix):]...)
		ab, err := d.unpackAddrAssetBalance(it.Value().Data())
		if err != nil {
			continue
		}
		if ab == nil {
			continue
		}
		result = append(result, &AddrAssetInfo{
			Controller: controller,
			Balance:    ab,
		})
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Per-address per-asset tx history
// ---------------------------------------------------------------------------

func (d *RocksDB) makeAddrAssetTxKey(addrDesc bchain.AddressDescriptor, controller []byte, height uint32) []byte {
	key := make([]byte, 0, len(addrAssetTxPrefix)+len(addrDesc)+len(controller)+4)
	key = append(key, []byte(addrAssetTxPrefix)...)
	key = append(key, addrDesc...)
	key = append(key, controller...)
	key = append(key, packDescHeight(height)...)
	return key
}

// GetAddrDescAssetTransactions iterates per-address per-asset tx history.
// lower/higher are block height bounds. Callback receives txid + height + indexes.
func (d *RocksDB) GetAddrDescAssetTransactions(
	addrDesc bchain.AddressDescriptor,
	controller []byte,
	lower, higher uint32,
	fn GetTransactionsCallback,
) error {
	txidLen := d.chainParser.PackedTxidLen()

	prefix := make([]byte, 0, len(addrAssetTxPrefix)+len(addrDesc)+len(controller))
	prefix = append(prefix, []byte(addrAssetTxPrefix)...)
	prefix = append(prefix, addrDesc...)
	prefix = append(prefix, controller...)

	startKey := append(append([]byte(nil), prefix...), packDescHeight(higher)...)
	stopKey := append(append([]byte(nil), prefix...), packDescHeight(lower)...)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := d.db.NewIteratorCF(ro, d.cfh[cfDefault])
	defer it.Close()

	indexes := make([]int32, 0, 16)
	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if bytes.Compare(key, stopKey) > 0 {
			break
		}
		height := unpackDescHeight(key[len(key)-4:])
		val := append([]byte(nil), it.Value().Data()...)
		for len(val) > txidLen {
			tx, err := d.chainParser.UnpackTxid(val[:txidLen])
			if err != nil {
				return err
			}
			indexes = indexes[:0]
			val = val[txidLen:]
			for len(val) > 0 {
				index, l := unpackVarint32(val)
				indexes = append(indexes, index>>1)
				val = val[l:]
				if index&1 == 1 {
					break
				}
			}
			if err := fn(tx, height, indexes); err != nil {
				if _, ok := err.(*StopIteration); ok {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Global asset tx history
// ---------------------------------------------------------------------------

func (d *RocksDB) makeGlobalAssetTxKey(controller []byte, height uint32) []byte {
	key := make([]byte, 0, len(globalAssetTxPrefix)+len(controller)+4)
	key = append(key, []byte(globalAssetTxPrefix)...)
	key = append(key, controller...)
	key = append(key, packDescHeight(height)...)
	return key
}

// GetAssetTransactions iterates global tx history for an asset.
func (d *RocksDB) GetAssetTransactions(
	controller []byte,
	lower, higher uint32,
	fn GetTransactionsCallback,
) error {
	txidLen := d.chainParser.PackedTxidLen()

	prefix := make([]byte, 0, len(globalAssetTxPrefix)+len(controller))
	prefix = append(prefix, []byte(globalAssetTxPrefix)...)
	prefix = append(prefix, controller...)

	startKey := append(append([]byte(nil), prefix...), packDescHeight(higher)...)
	stopKey := append(append([]byte(nil), prefix...), packDescHeight(lower)...)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()

	it := d.db.NewIteratorCF(ro, d.cfh[cfDefault])
	defer it.Close()

	indexes := make([]int32, 0, 16)
	for it.Seek(startKey); it.Valid(); it.Next() {
		key := it.Key().Data()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if bytes.Compare(key, stopKey) > 0 {
			break
		}
		height := unpackDescHeight(key[len(key)-4:])
		val := append([]byte(nil), it.Value().Data()...)
		for len(val) > txidLen {
			tx, err := d.chainParser.UnpackTxid(val[:txidLen])
			if err != nil {
				return err
			}
			indexes = indexes[:0]
			val = val[txidLen:]
			for len(val) > 0 {
				index, l := unpackVarint32(val)
				indexes = append(indexes, index>>1)
				val = val[l:]
				if index&1 == 1 {
					break
				}
			}
			if err := fn(tx, height, indexes); err != nil {
				if _, ok := err.(*StopIteration); ok {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// controllerInfo — used during block processing
// ---------------------------------------------------------------------------

type controllerInfo struct {
	Controller   []byte
	IsController bool
}

// ---------------------------------------------------------------------------
// processAssetsCoordinateType
//
// Called from ConnectBlock AFTER processAddressesBitcoinType.
//
// Phase 1: v10 ASSET_CREATE
//   - controller = pack(this_txid, 0)
//   - Tag output[0] = controller (IsController=true), output[1] = supply
//   - Detect mint-more: if any input has IsController → redirect old→new
//   - Store/update asset registry with metadata (ticker, precision, etc.)
//
// Phase 2: v11 ASSET_TRANSFER
//   - Find controller from spent inputs (DB or same-block map)
//   - Sum non-controller asset input values (exclude controller coin value)
//   - Resolve controller → current via registry redirect chain
//   - Fill outputs top-to-bottom until sum consumed → set Controller
//
// Phase 3: Write indexes
//   - Per-address asset balance (aa:)
//   - Per-address per-asset tx history (ax:)
//   - Global asset tx history (gt:)
// ---------------------------------------------------------------------------

func (d *RocksDB) processAssetsCoordinateType(
	block *bchain.Block,
	wb *grocksdb.WriteBatch,
	txAddressesMap map[string]*TxAddresses,
	balances map[string]*AddrBalance,
) error {

	ctrlMap := make(map[string]*controllerInfo) // "txid:vout" → info

	type addrAssetKey struct {
		addrDesc   string
		controller string
	}
	affected := make(map[addrAssetKey]bool)

	type assetTxEntry struct {
		controller []byte
		btxID      []byte
		indexes    []int32
	}
	var assetTxs []assetTxEntry

	glog.Infof("ASSET-DEBUG processAssetsCoordinateType: block=%d txs=%d", block.Height, len(block.Txs))

	// ── Phase 1: v10 ASSET_CREATE ──────────────────────────────

	for txi := range block.Txs {
		tx := &block.Txs[txi]
		if tx.Version != 10 || len(tx.Vout) < 2 {
			continue
		}

		glog.Infof("ASSET-DEBUG Phase1: FOUND v10 tx=%s vouts=%d coinSpecific=%v coinSpecificType=%T", tx.Txid, len(tx.Vout), tx.CoinSpecificData != nil, tx.CoinSpecificData)

		btxID, err := d.chainParser.PackTxid(tx.Txid)
		if err != nil {
			return err
		}

		ctrlOut, err := d.packControllerOutpoint(tx.Txid, 0)
		if err != nil {
			return err
		}

		// Detect mint-more: check if any input is an old controller
		var oldCtrl []byte
		for i := range tx.Vin {
			vin := &tx.Vin[i]
			if vin.Txid == "" {
				continue
			}
			ci := ctrlMap[opKey(vin.Txid, vin.Vout)]
			if ci == nil {
				ci = d.lookupSpentController(vin.Txid, vin.Vout, txAddressesMap)
			}
			if ci != nil && ci.IsController {
				oldCtrl = ci.Controller
				break
			}
		}

		// Tag output[0] = controller, output[1] = supply
		d.tagUtxoController(balances, txAddressesMap, btxID, 0, ctrlOut, true)
		d.tagUtxoController(balances, txAddressesMap, btxID, 1, ctrlOut, false)
		ctrlMap[opKey(tx.Txid, 0)] = &controllerInfo{ctrlOut, true}
		ctrlMap[opKey(tx.Txid, 1)] = &controllerInfo{ctrlOut, false}

		// Track affected addresses
		ta := txAddressesMap[string(btxID)]
		if ta != nil {
			for oi := 0; oi < 2 && oi < len(ta.Outputs); oi++ {
				if len(ta.Outputs[oi].AddrDesc) > 0 {
					affected[addrAssetKey{string(ta.Outputs[oi].AddrDesc), string(ctrlOut)}] = true
				}
			}
			for ii := range ta.Inputs {
				if len(ta.Inputs[ii].AddrDesc) > 0 {
					affected[addrAssetKey{string(ta.Inputs[ii].AddrDesc), string(ctrlOut)}] = true
				}
			}
		}

		// Asset tx history entry
		assetTxs = append(assetTxs, assetTxEntry{ctrlOut, btxID, []int32{0, 1}})

		// Build registry entry
		supply := &tx.Vout[1].ValueSat
		entry := &AssetRegistryEntry{
			CurrentController: ctrlOut,
			Precision:         8,
		}

		if oldCtrl != nil && !bytes.Equal(oldCtrl, ctrlOut) {
			// Mint-more: carry forward metadata, add supply
			oldEntry, _ := d.GetAssetRegistryEntry(oldCtrl)
			if oldEntry != nil && !oldEntry.IsRedirect {
				entry.Ticker = oldEntry.Ticker
				entry.Headline = oldEntry.Headline
				entry.Precision = oldEntry.Precision
				entry.AssetType = oldEntry.AssetType
				entry.TotalSupply.Add(&oldEntry.TotalSupply, supply)
			} else {
				entry.TotalSupply.Set(supply)
			}
			// Write redirect: old → new
			redirect := &AssetRegistryEntry{IsRedirect: true, CurrentController: ctrlOut}
			rKey := append([]byte(assetRegistryPrefix), oldCtrl...)
			wb.PutCF(d.cfh[cfDefault], rKey, d.packAssetRegistryEntry(redirect))
		} else {
			// First creation
			entry.TotalSupply.Set(supply)
			d.fillAssetMetadataFromTx(tx, entry)
		}

		glog.Infof("ASSET-DEBUG Phase1: writing registry ticker=%q headline=%q precision=%d assetType=%d supply=%s ctrlOut=%s",
			entry.Ticker, entry.Headline, entry.Precision, entry.AssetType, entry.TotalSupply.String(), hex.EncodeToString(ctrlOut))

		regKey := append([]byte(assetRegistryPrefix), ctrlOut...)
		packed := d.packAssetRegistryEntry(entry)
		glog.Infof("ASSET-DEBUG Phase1: regKey=%s packedLen=%d", hex.EncodeToString(regKey), len(packed))
		wb.PutCF(d.cfh[cfDefault], regKey, packed)
	}

	// ── Phase 2: v11 ASSET_TRANSFER ────────────────────────────

	for txi := range block.Txs {
		tx := &block.Txs[txi]
		if tx.Version != 11 {
			continue
		}

		btxID, err := d.chainParser.PackTxid(tx.Txid)
		if err != nil {
			return err
		}

		ta := txAddressesMap[string(btxID)]
		var assetTotal big.Int
		var controller []byte

		// Pass over inputs: find controller, sum asset values
		for i := range tx.Vin {
			vin := &tx.Vin[i]
			if vin.Txid == "" {
				continue
			}
			ci := ctrlMap[opKey(vin.Txid, vin.Vout)]
			if ci == nil {
				ci = d.lookupSpentController(vin.Txid, vin.Vout, txAddressesMap)
			}
			if ci == nil || len(ci.Controller) == 0 {
				continue
			}
			if ci.IsController {
				// Controller coins don't count toward fill amount
				if controller == nil {
					controller = ci.Controller
				}
			} else {
				// Asset supply input: sum value
				if ta != nil && i < len(ta.Inputs) {
					assetTotal.Add(&assetTotal, &ta.Inputs[i].ValueSat)
				}
				if controller == nil {
					controller = ci.Controller
				}
			}
			// Track input address
			if ta != nil && i < len(ta.Inputs) && len(ta.Inputs[i].AddrDesc) > 0 {
				affected[addrAssetKey{string(ta.Inputs[i].AddrDesc), string(controller)}] = true
			}
		}

		if controller == nil || assetTotal.Sign() == 0 {
			continue
		}

		resolved := d.ResolveCurrentController(controller)

		// Fill outputs top-to-bottom until assetTotal consumed
		var filled big.Int
		var filledIdx []int32
		for i := range tx.Vout {
			if filled.Cmp(&assetTotal) >= 0 {
				break
			}
			d.tagUtxoController(balances, txAddressesMap, btxID, int32(i), resolved, false)
			ctrlMap[opKey(tx.Txid, uint32(i))] = &controllerInfo{resolved, false}
			filledIdx = append(filledIdx, int32(i))

			if ta != nil && i < len(ta.Outputs) && len(ta.Outputs[i].AddrDesc) > 0 {
				affected[addrAssetKey{string(ta.Outputs[i].AddrDesc), string(resolved)}] = true
			}
			filled.Add(&filled, &tx.Vout[i].ValueSat)
		}

		assetTxs = append(assetTxs, assetTxEntry{resolved, btxID, filledIdx})
	}

	// ── Phase 3: Write indexes ─────────────────────────────────

	glog.Infof("ASSET-DEBUG Phase3: %d affected pairs, %d assetTxs", len(affected), len(assetTxs))

	// 3a. Per-address asset balances
	for ak := range affected {
		addrDesc := bchain.AddressDescriptor(ak.addrDesc)
		ctrl := []byte(ak.controller)

		// Compute current balance from live UTXOs (exclude controller output — it's ownership, not supply)
		var assetBal big.Int
		if bal := balances[ak.addrDesc]; bal != nil {
			for _, u := range bal.Utxos {
				if u.Vout >= 0 && bytes.Equal(u.Controller, ctrl) && !u.IsController {
					assetBal.Add(&assetBal, &u.ValueSat)
				}
			}
		}

		// Load existing to carry forward txCount + sentSat
		existing, _ := d.GetAddrAssetBalance(addrDesc, ctrl)
		aab := &AddrAssetBalance{BalanceSat: assetBal}
		if existing != nil {
			aab.Txs = existing.Txs + 1
			aab.SentSat.Set(&existing.SentSat)
		} else {
			aab.Txs = 1
		}

		key := d.makeAddrAssetKey(addrDesc, ctrl)
		wb.PutCF(d.cfh[cfDefault], key, d.packAddrAssetBalance(aab))
	}

	// 3b. Tx history (global + per-address per-asset)
	for _, ate := range assetTxs {
		val := d.packAssetTxEntry(ate.btxID, ate.indexes)

		// Global asset tx history
		gtKey := d.makeGlobalAssetTxKey(ate.controller, block.Height)
		d.appendToCF(wb, gtKey, val)

		// Per-address per-asset tx history
		ta := txAddressesMap[string(ate.btxID)]
		if ta == nil {
			continue
		}
		seen := make(map[string]bool)

		// Output addresses
		for _, idx := range ate.indexes {
			if int(idx) < len(ta.Outputs) {
				ad := string(ta.Outputs[idx].AddrDesc)
				if ad != "" && !seen[ad] {
					seen[ad] = true
					axKey := d.makeAddrAssetTxKey(bchain.AddressDescriptor(ad), ate.controller, block.Height)
					d.appendToCF(wb, axKey, val)
				}
			}
		}
		// Input addresses
		for i := range ta.Inputs {
			ad := string(ta.Inputs[i].AddrDesc)
			if ad != "" && !seen[ad] {
				seen[ad] = true
				axKey := d.makeAddrAssetTxKey(bchain.AddressDescriptor(ad), ate.controller, block.Height)
				d.appendToCF(wb, axKey, val)
			}
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func opKey(txid string, vout uint32) string {
	return txid + ":" + uitoa(vout)
}

func uitoa(v uint32) string {
	if v == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	for v > 0 {
		buf = append(buf, byte('0'+v%10))
		v /= 10
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

// adjustBalancesForAssets recomputes BalanceSat for every address that has
// asset-tagged UTXOs. After processAssetsCoordinateType tags outputs with
// Controller, the raw BalanceSat (set by processAddressesBitcoinType) still
// includes asset supply/controller values. This function corrects that by
// summing only non-asset UTXOs.
//
// Must be called AFTER processAssetsCoordinateType and BEFORE storeBalances.
func adjustBalancesForAssets(balances map[string]*AddrBalance) {
	for _, bal := range balances {
		if len(bal.Utxos) == 0 {
			continue
		}
		// Quick check: does this address have any asset-tagged UTXOs?
		hasAsset := false
		for i := range bal.Utxos {
			if bal.Utxos[i].Vout >= 0 && len(bal.Utxos[i].Controller) > 0 {
				hasAsset = true
				break
			}
		}
		if !hasAsset {
			continue
		}
		// Recompute BalanceSat from non-asset UTXOs only
		var nonAssetBalance big.Int
		for i := range bal.Utxos {
			u := &bal.Utxos[i]
			if u.Vout < 0 { // spent
				continue
			}
			if len(u.Controller) == 0 {
				nonAssetBalance.Add(&nonAssetBalance, &u.ValueSat)
			}
		}
		bal.BalanceSat.Set(&nonAssetBalance)
	}
}

// tagUtxoController sets Controller on a UTXO in the balances map.
func (d *RocksDB) tagUtxoController(
	balances map[string]*AddrBalance,
	txAddressesMap map[string]*TxAddresses,
	btxID []byte, vout int32,
	controller []byte, isController bool,
) {
	ta := txAddressesMap[string(btxID)]
	if ta == nil || int(vout) >= len(ta.Outputs) {
		glog.Warningf("ASSET-DEBUG tagUtxo: ta=%v vout=%d — ta nil or out of range", ta == nil, vout)
		return
	}
	addrDesc := string(ta.Outputs[vout].AddrDesc)
	if addrDesc == "" {
		glog.Warningf("ASSET-DEBUG tagUtxo: empty addrDesc for vout=%d", vout)
		return
	}
	bal := balances[addrDesc]
	if bal == nil {
		glog.Warningf("ASSET-DEBUG tagUtxo: no balance for vout=%d addrDescLen=%d", vout, len(addrDesc))
		return
	}
	for i := range bal.Utxos {
		u := &bal.Utxos[i]
		if u.Vout == vout && bytes.Equal(u.BtxID, btxID) {
			u.Controller = controller
			u.IsController = isController
			glog.Infof("ASSET-DEBUG tagUtxo: TAGGED vout=%d isController=%v value=%s", vout, isController, u.ValueSat.String())
			return
		}
	}
	glog.Warningf("ASSET-DEBUG tagUtxo: UTXO NOT FOUND vout=%d, bal has %d utxos", vout, len(bal.Utxos))
}

// lookupSpentController reads controller from a spent UTXO in the DB.
func (d *RocksDB) lookupSpentController(
	txid string, vout uint32,
	txAddressesMap map[string]*TxAddresses,
) *controllerInfo {
	btxID, err := d.chainParser.PackTxid(txid)
	if err != nil {
		return nil
	}
	ta := txAddressesMap[string(btxID)]
	if ta == nil {
		ta, err = d.getTxAddresses(btxID)
		if err != nil || ta == nil {
			return nil
		}
	}
	if int(vout) >= len(ta.Outputs) {
		return nil
	}
	addrDesc := ta.Outputs[vout].AddrDesc
	if len(addrDesc) == 0 {
		return nil
	}
	bal, err := d.GetAddrDescBalance(addrDesc, AddressBalanceDetailUTXO)
	if err != nil || bal == nil {
		return nil
	}
	for _, u := range bal.Utxos {
		if u.Vout == int32(vout) && bytes.Equal(u.BtxID, btxID) {
			if len(u.Controller) == 0 {
				return nil
			}
			return &controllerInfo{u.Controller, u.IsController}
		}
	}
	return nil
}

// packAssetTxEntry creates cfAddresses-compatible value for one tx.
func (d *RocksDB) packAssetTxEntry(btxID []byte, indexes []int32) []byte {
	buf := make([]byte, 0, len(btxID)+len(indexes)*2)
	bvout := make([]byte, vlq.MaxLen32)
	buf = append(buf, btxID...)
	for i, idx := range indexes {
		v := idx << 1
		if i == len(indexes)-1 {
			v |= 1
		}
		l := packVarint32(v, bvout)
		buf = append(buf, bvout[:l]...)
	}
	return buf
}


func appendVarint32(buf []byte, v int32) []byte {
	uv := uint32(v)
	for uv >= 0x80 {
		buf = append(buf, byte(uv)|0x80)
		uv >>= 7
	}
	buf = append(buf, byte(uv))
	return buf
}

// appendToCF appends data to an existing key's value (or creates it).
func (d *RocksDB) appendToCF(wb *grocksdb.WriteBatch, key, val []byte) {
	existing, err := d.db.GetCF(d.ro, d.cfh[cfDefault], key)
	if err == nil && existing.Data() != nil {
		combined := make([]byte, 0, len(existing.Data())+len(val))
		combined = append(combined, existing.Data()...)
		combined = append(combined, val...)
		wb.PutCF(d.cfh[cfDefault], key, combined)
		existing.Free()
	} else {
		wb.PutCF(d.cfh[cfDefault], key, val)
		if existing != nil {
			existing.Free()
		}
	}
}

// fillAssetMetadataFromTx extracts ticker/headline/precision/assetType from CoinSpecificData.
func (d *RocksDB) fillAssetMetadataFromTx(tx *bchain.Tx, entry *AssetRegistryEntry) {
	if tx.CoinSpecificData == nil {
		glog.Warning("ASSET-DEBUG fillMetadata: CoinSpecificData is NIL for tx ", tx.Txid)
		return
	}
	glog.Infof("ASSET-DEBUG fillMetadata: CoinSpecificData type=%T", tx.CoinSpecificData)
	raw, ok := tx.CoinSpecificData.(json.RawMessage)
	if !ok {
		if rawBytes, ok2 := tx.CoinSpecificData.([]byte); ok2 {
			raw = json.RawMessage(rawBytes)
			glog.Info("ASSET-DEBUG fillMetadata: used []byte fallback")
		} else {
			glog.Warningf("ASSET-DEBUG fillMetadata: type assertion FAILED, type=%T", tx.CoinSpecificData)
			return
		}
	}
	preview := string(raw)
	if len(preview) > 100 {
		preview = preview[:100]
	}
	glog.Infof("ASSET-DEBUG fillMetadata: raw JSON len=%d, first100=%s", len(raw), preview)
	var fields struct {
		Ticker    string `json:"ticker"`
		Headline  string `json:"headline"`
		Precision int32  `json:"precision"`
		AssetType int32  `json:"assettype"`
	}
	if err := json.Unmarshal(raw, &fields); err == nil {
		glog.Infof("ASSET-DEBUG fillMetadata: parsed ticker=%q headline=%q precision=%d assetType=%d", fields.Ticker, fields.Headline, fields.Precision, fields.AssetType)
		if fields.Ticker != "" {
			entry.Ticker = fields.Ticker
		}
		if fields.Headline != "" {
			entry.Headline = fields.Headline
		}
		if fields.Precision > 0 {
			entry.Precision = fields.Precision
		}
		entry.AssetType = fields.AssetType
	} else {
		glog.Warningf("ASSET-DEBUG fillMetadata: json.Unmarshal FAILED: %v", err)
	}
}