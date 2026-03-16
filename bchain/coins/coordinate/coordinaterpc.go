package coordinate

import (
	"encoding/json"
	"math/big"
	"strings"

	"github.com/golang/glog"
	"github.com/juju/errors"
	"github.com/trezor/blockbook/bchain"
	"github.com/trezor/blockbook/bchain/coins/btc"
	"github.com/trezor/blockbook/common"
)

// ---------------------------------------------------------------------------
// JSON response types
// ---------------------------------------------------------------------------

type FlexibleString string

func (f *FlexibleString) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*f = FlexibleString(s)
		return nil
	}
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*f = FlexibleString(strings.Join(arr, " "))
		return nil
	}
	*f = ""
	return nil
}

type ResGetBlockChainInfo struct {
	Error  *bchain.RPCError `json:"error"`
	Result struct {
		Chain         string            `json:"chain"`
		Blocks        int               `json:"blocks"`
		Headers       int               `json:"headers"`
		Bestblockhash string            `json:"bestblockhash"`
		Difficulty    common.JSONNumber `json:"difficulty"`
		SizeOnDisk    int64             `json:"size_on_disk"`
		Warnings      FlexibleString    `json:"warnings"`
	} `json:"result"`
}

type ResGetNetworkInfo struct {
	Error  *bchain.RPCError `json:"error"`
	Result struct {
		Version         common.JSONNumber `json:"version"`
		Subversion      common.JSONNumber `json:"subversion"`
		ProtocolVersion common.JSONNumber `json:"protocolversion"`
		Timeoffset      float64           `json:"timeoffset"`
		Warnings        FlexibleString    `json:"warnings"`
	} `json:"result"`
}

type ResGetCoordinateBlock struct {
	Error  *bchain.RPCError      `json:"error"`
	Result CoordinateBlockResult `json:"result"`
}

type CoordinateBlockResult struct {
	bchain.BlockHeader
	Txs           []json.RawMessage       `json:"tx"`
	PreconfBlocks []CoordinateSignedBlock `json:"preconfblocks"`
	Pegins        []json.RawMessage       `json:"pegins"`
}

type CoordinateSignedBlock struct {
	Fee    int64             `json:"fee"`
	Height uint64            `json:"height"`
	Time   uint32            `json:"time"`
	Hash   string            `json:"hash"`
	Txs    []json.RawMessage `json:"tx"`
}

// ---------------------------------------------------------------------------
// CoordinateRPC
// ---------------------------------------------------------------------------

type CoordinateRPC struct {
	*btc.BitcoinRPC
}

func NewCoordinateRPC(config json.RawMessage, pushHandler func(bchain.NotificationType)) (bchain.BlockChain, error) {
	b, err := btc.NewBitcoinRPC(config, pushHandler)
	if err != nil {
		return nil, err
	}
	s := &CoordinateRPC{b.(*btc.BitcoinRPC)}
	s.RPCMarshaler = btc.JSONMarshalerV2{}
	s.ParseBlocks = false
	return s, nil
}

func (b *CoordinateRPC) Initialize() error {
	ci, err := b.GetChainInfo()
	if err != nil {
		return err
	}
	chainName := ci.Chain
	glog.Info("Chain name ", chainName)
	params := GetChainParams(chainName)
	b.Parser = NewCoordinateParser(params, b.ChainConfig)
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

func (b *CoordinateRPC) GetChainInfo() (*bchain.ChainInfo, error) {
	chainInfoReq := btc.CmdGetBlockChainInfo{Method: "getblockchaininfo"}
	resCi := ResGetBlockChainInfo{}
	err := b.Call(&chainInfoReq, &resCi)
	if err != nil {
		return nil, err
	}
	if resCi.Error != nil {
		return nil, resCi.Error
	}
	networkInfoReq := btc.CmdGetNetworkInfo{Method: "getnetworkinfo"}
	resNi := ResGetNetworkInfo{}
	err = b.Call(&networkInfoReq, &resNi)
	if err != nil {
		return nil, err
	}
	if resNi.Error != nil {
		return nil, resNi.Error
	}
	rv := &bchain.ChainInfo{
		Bestblockhash: resCi.Result.Bestblockhash,
		Blocks:        resCi.Result.Blocks,
		Chain:         resCi.Result.Chain,
		Difficulty:    string(resCi.Result.Difficulty),
		Headers:       resCi.Result.Headers,
		SizeOnDisk:    resCi.Result.SizeOnDisk,
		Subversion:    string(resNi.Result.Subversion),
		Timeoffset:    resNi.Result.Timeoffset,
		Version:       string(resNi.Result.Version),
		ProtocolVersion: string(resNi.Result.ProtocolVersion),
	}
	if len(resCi.Result.Warnings) > 0 {
		rv.Warnings = string(resCi.Result.Warnings) + " "
	}
	if resCi.Result.Warnings != resNi.Result.Warnings {
		rv.Warnings += string(resNi.Result.Warnings)
	}
	return rv, nil
}

// GetBlock returns block with transactions from all three sources:
// vtx (mined), preconfblocks (signed blocks), pegins (Bitcoin mainchain).
// Uses verbosity=2 so full transaction JSON is returned inline,
// avoiding separate getrawtransaction calls (which fail for the genesis coinbase).
func (b *CoordinateRPC) GetBlock(hash string, height uint32) (*bchain.Block, error) {
	var err error
	if hash == "" {
		hash, err = b.GetBlockHash(height)
		if err != nil {
			return nil, err
		}
	}
	glog.V(1).Info("rpc: getblock (verbosity=2) ", hash)
	res := ResGetCoordinateBlock{}
	req := btc.CmdGetBlock{Method: "getblock"}
	req.Params.BlockHash = hash
	req.Params.Verbosity = 2
	err = b.Call(&req, &res)
	if err != nil {
		return nil, errors.Annotatef(err, "hash %v", hash)
	}
	if res.Error != nil {
		return nil, errors.Annotatef(res.Error, "hash %v", hash)
	}

	// Estimate total tx count
	totalTxs := len(res.Result.Txs) + len(res.Result.Pegins)
	for i := range res.Result.PreconfBlocks {
		totalTxs += len(res.Result.PreconfBlocks[i].Txs)
	}
	txs := make([]bchain.Tx, 0, totalTxs)

	// 1. Main mined transactions (vtx) — already full JSON from verbosity=2
	for txIdx, rawTx := range res.Result.Txs {
		tx, err := b.Parser.ParseTxFromJson(rawTx)
		if err != nil {
			glog.Warningf("rpc: failed to parse vtx %d in block %s: %v", txIdx, hash, err)
			continue
		}
		tx.CoinSpecificData = rawTx
		txs = append(txs, *tx)
	}

	// 2. Preconf (signed block) transactions
	// For v9 txs, correct vout[0] using the node's getpreconftxrefund RPC.
	// Same-block cache handles chained preconf txs within this block where
	// the RPC might not have the data yet.
	preconfRefunds := make(map[string]*big.Int)

	for sbIdx, sb := range res.Result.PreconfBlocks {
		for txIdx, rawTx := range sb.Txs {
			tx, err := b.Parser.ParseTxFromJson(rawTx)
			if err != nil {
				glog.Warningf("rpc: failed to parse preconf tx %d in signed block %d: %v", txIdx, sbIdx, err)
				continue
			}
			tx.CoinSpecificData = rawTx

			if tx.Version == TxVersionPreconf && len(tx.Vout) >= 2 {
				// Try the node RPC first
				refund, err := b.getPreconfRefund(tx.Txid)
				if err == nil && refund >= 0 {
					tx.Vout[0].ValueSat = *big.NewInt(refund)
					preconfRefunds[tx.Txid] = big.NewInt(refund)
					glog.Infof("PRECONF-DEBUG: v9 tx=%s refund=%d (from RPC)", tx.Txid, refund)
				} else if tx.VSize > 0 {
					// Fallback: compute manually using sb.Fee
					var vinTotal big.Int
					for i := range tx.Vin {
						if tx.Vin[i].Txid == "" {
							continue
						}
						if tx.Vin[i].Vout == 0 {
							if cached, ok := preconfRefunds[tx.Vin[i].Txid]; ok {
								vinTotal.Add(&vinTotal, cached)
								continue
							}
						}
						prevTx, pErr := b.GetTransaction(tx.Vin[i].Txid)
						if pErr == nil && prevTx != nil && int(tx.Vin[i].Vout) < len(prevTx.Vout) {
							vinTotal.Add(&vinTotal, &prevTx.Vout[tx.Vin[i].Vout].ValueSat)
						}
					}
					var voutSum big.Int
					for i := 1; i < len(tx.Vout); i++ {
						voutSum.Add(&voutSum, &tx.Vout[i].ValueSat)
					}
					actualFee := int64(tx.VSize) * sb.Fee
					change := new(big.Int).Sub(&vinTotal, &voutSum)
					change.Sub(change, big.NewInt(actualFee))
					if change.Sign() >= 0 {
						tx.Vout[0].ValueSat = *change
						preconfRefunds[tx.Txid] = new(big.Int).Set(change)
						glog.Infof("PRECONF-DEBUG: v9 tx=%s refund=%s (computed, sbFee=%d)", tx.Txid, change.String(), sb.Fee)
					} else {
						glog.Warningf("PRECONF-DEBUG: v9 tx=%s negative change=%s", tx.Txid, change.String())
					}
				}
			}

			txs = append(txs, *tx)
		}
	}

	// 3. Pegin transactions
	for pIdx, rawTx := range res.Result.Pegins {
		tx, err := b.Parser.ParseTxFromJson(rawTx)
		if err != nil {
			glog.Warningf("rpc: failed to parse pegin tx %d (block %s): %v", pIdx, hash, err)
			continue
		}
		tx.CoinSpecificData = rawTx
		txs = append(txs, *tx)
	}

	block := &bchain.Block{
		BlockHeader: res.Result.BlockHeader,
		Txs:         txs,
	}
	return block, nil
}

// GetTransaction overrides BitcoinRPC.GetTransaction to correct v9 preconf
// vout[0] values. The raw tx has the fee RATE in vout[0], but the actual
// UTXO value is the refund after fee settlement.
// getPreconfRefund calls the node's getpreconftxrefund RPC to get the correct
// refund value for a v9 preconf transaction's vout[0].
func (b *CoordinateRPC) getPreconfRefund(txid string) (int64, error) {
	type refundReq struct {
		Method string        `json:"method"`
		Params []interface{} `json:"params"`
	}
	type refundEntry struct {
		Tx     string `json:"tx"`
		Refund int64  `json:"refund"`
	}
	type refundRes struct {
		Error  *bchain.RPCError `json:"error"`
		Result []refundEntry    `json:"result"`
	}
	req := refundReq{
		Method: "getpreconftxrefund",
		Params: []interface{}{
			[]map[string]string{{"tx": txid}},
		},
	}
	res := refundRes{}
	err := b.Call(&req, &res)
	if err != nil {
		return 0, err
	}
	if res.Error != nil {
		return 0, res.Error
	}
	if len(res.Result) > 0 {
		return res.Result[0].Refund, nil
	}
	return 0, errors.New("empty result from getpreconftxrefund")
}

// GetTransaction overrides BitcoinRPC.GetTransaction to correct v9 preconf
// vout[0] values. The raw tx has the fee RATE in vout[0], but the actual
// UTXO value is the refund after fee settlement.
func (b *CoordinateRPC) GetTransaction(txid string) (*bchain.Tx, error) {
	tx, err := b.BitcoinRPC.GetTransaction(txid)
	if err != nil || tx == nil {
		return tx, err
	}
	// Only correct confirmed v9 txs
	if tx.Version != TxVersionPreconf || len(tx.Vout) < 2 || tx.Confirmations == 0 {
		return tx, nil
	}
	// Ask the node for the correct refund value
	refund, err := b.getPreconfRefund(txid)
	if err != nil {
		glog.Warningf("PRECONF-DEBUG: getPreconfRefund failed for tx=%s: %v", txid, err)
		return tx, nil
	}
	tx.Vout[0].ValueSat = *big.NewInt(refund)
	glog.V(1).Infof("PRECONF-DEBUG: corrected vout[0] for tx=%s refund=%d", txid, refund)
	return tx, nil
}

func (b *CoordinateRPC) GetTransactionForMempool(txid string) (*bchain.Tx, error) {
	return b.GetTransaction(txid)
}

// GetRawTransactionsForMempoolBatch overrides the parent to use JSON (verbose)
// fetching instead of raw hex, since Coordinate's wire format differs from Bitcoin.
func (b *CoordinateRPC) GetRawTransactionsForMempoolBatch(txids []string) (map[string]*bchain.Tx, error) {
	results := make(map[string]*bchain.Tx, len(txids))
	for _, txid := range txids {
		tx, err := b.GetTransaction(txid)
		if err != nil {
			if err == bchain.ErrTxNotFound {
				continue
			}
			return nil, err
		}
		results[txid] = tx
	}
	return results, nil
}

func (b *CoordinateRPC) GetMempoolEntry(txid string) (*bchain.MempoolEntry, error) {
	return nil, errors.New("GetMempoolEntry: not implemented")
}