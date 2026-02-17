package coordinate

import (
	"encoding/json"
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
	Txids         []string                `json:"tx"`
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
func (b *CoordinateRPC) GetBlock(hash string, height uint32) (*bchain.Block, error) {
	var err error
	if hash == "" {
		hash, err = b.GetBlockHash(height)
		if err != nil {
			return nil, err
		}
	}
	glog.V(1).Info("rpc: getblock (verbosity=1) ", hash)
	res := ResGetCoordinateBlock{}
	req := btc.CmdGetBlock{Method: "getblock"}
	req.Params.BlockHash = hash
	req.Params.Verbosity = 1
	err = b.Call(&req, &res)
	if err != nil {
		return nil, errors.Annotatef(err, "hash %v", hash)
	}
	if res.Error != nil {
		return nil, errors.Annotatef(res.Error, "hash %v", hash)
	}

	// Estimate total tx count
	totalTxs := len(res.Result.Txids) + len(res.Result.Pegins)
	for i := range res.Result.PreconfBlocks {
		totalTxs += len(res.Result.PreconfBlocks[i].Txs)
	}
	txs := make([]bchain.Tx, 0, totalTxs)

	// 1. Main mined transactions (vtx)
	for _, txid := range res.Result.Txids {
		tx, err := b.GetTransaction(txid)
		if err != nil {
			return nil, err
		}
		txs = append(txs, *tx)
	}

	// 2. Preconf (signed block) transactions
	for sbIdx, sb := range res.Result.PreconfBlocks {
		for txIdx, rawTx := range sb.Txs {
			tx, err := b.Parser.ParseTxFromJson(rawTx)
			if err != nil {
				glog.Warningf("rpc: failed to parse preconf tx %d in signed block %d: %v", txIdx, sbIdx, err)
				continue
			}
			tx.CoinSpecificData = rawTx
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

func (b *CoordinateRPC) GetTransactionForMempool(txid string) (*bchain.Tx, error) {
	return b.GetTransaction(txid)
}

func (b *CoordinateRPC) GetMempoolEntry(txid string) (*bchain.MempoolEntry, error) {
	return nil, errors.New("GetMempoolEntry: not implemented")
}