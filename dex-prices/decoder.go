package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"log"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type SwapDecoder struct {
	client *ethclient.Client
	cache  *Cache
	config ChainConfig
}

func NewSwapDecoder(client *ethclient.Client, cache *Cache, config ChainConfig) *SwapDecoder {
	return &SwapDecoder{
		client: client,
		cache:  cache,
		config: config,
	}
}

func (d *SwapDecoder) GetTokenInfo(ctx context.Context, address common.Address) (TokenInfo, error) {
	addrStr := strings.ToLower(address.Hex())

	// Check cache first
	if token, ok := d.cache.GetToken(addrStr); ok {
		return token, nil
	}

	// Query contract
	contract := bind.NewBoundContract(address, ERC20ABI, d.client, d.client, d.client)

	var symbol string
	var decimals uint8

	opts := &bind.CallOpts{Context: ctx}

	// Try to get symbol - some tokens return bytes32 instead of string
	symbolOut := []interface{}{&symbol}
	symbolErr := contract.Call(opts, &symbolOut, "symbol")
	if symbolErr != nil {
		// Try bytes32 format for older tokens
		var symbolBytes32 [32]byte
		symbolBytes32Out := []interface{}{&symbolBytes32}
		if err := contract.Call(opts, &symbolBytes32Out, "symbol"); err == nil {
			// Convert bytes32 to string (remove trailing zeros)
			symbol = string(bytes.TrimRight(symbolBytes32[:], "\x00"))
		} else {
			// Both failed, use unknown
			log.Printf("[%s] ⚠ Failed to get symbol for token %s (both string and bytes32): %v", d.config.Name, addrStr, err)
			symbol = "UNKNOWN"
		}
	}

	// Get decimals
	decimalsOut := []interface{}{&decimals}
	if err := contract.Call(opts, &decimalsOut, "decimals"); err != nil {
		log.Printf("[%s] ⚠ Failed to get decimals for token %s, using default 18: %v", d.config.Name, addrStr, err)
		decimals = 18
	}

	token := TokenInfo{
		Address:  addrStr,
		Symbol:   symbol,
		Decimals: decimals,
	}

	d.cache.SetToken(addrStr, token)
	return token, nil
}

func (d *SwapDecoder) GetV2PairInfo(ctx context.Context, pairAddr common.Address) (*PairInfo, error) {
	addrStr := strings.ToLower(pairAddr.Hex())

	// Check cache
	if pair, ok := d.cache.GetPair(addrStr); ok {
		return &pair, nil
	}

	contract := bind.NewBoundContract(pairAddr, UniswapV2PairABI, d.client, d.client, d.client)
	opts := &bind.CallOpts{Context: ctx}

	var token0Addr common.Address
	var token1Addr common.Address

	token0Out := []interface{}{&token0Addr}
	token1Out := []interface{}{&token1Addr}

	if err := contract.Call(opts, &token0Out, "token0"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token0 for pair %s: %v", d.config.Name, addrStr, err)
		return nil, err
	}
	if err := contract.Call(opts, &token1Out, "token1"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token1 for pair %s: %v", d.config.Name, addrStr, err)
		return nil, err
	}

	token0Info, err := d.GetTokenInfo(ctx, token0Addr)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get token0 info: %v", d.config.Name, err)
		return nil, err
	}

	token1Info, err := d.GetTokenInfo(ctx, token1Addr)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get token1 info: %v", d.config.Name, err)
		return nil, err
	}

	pair := PairInfo{
		Address:    addrStr,
		Version:    "V2",
		Token0:     strings.ToLower(token0Addr.Hex()),
		Token1:     strings.ToLower(token1Addr.Hex()),
		Token0Info: token0Info,
		Token1Info: token1Info,
	}

	d.cache.SetPair(addrStr, pair)
	return &pair, nil
}

func (d *SwapDecoder) GetV3PoolInfo(ctx context.Context, poolAddr common.Address) (*PairInfo, error) {
	addrStr := strings.ToLower(poolAddr.Hex())
	cacheKey := "v3_" + addrStr

	// Check cache
	if pair, ok := d.cache.GetPair(cacheKey); ok {
		return &pair, nil
	}

	contract := bind.NewBoundContract(poolAddr, UniswapV3PoolABI, d.client, d.client, d.client)
	opts := &bind.CallOpts{Context: ctx}

	var token0Addr common.Address
	var token1Addr common.Address
	var feeBig *big.Int // uint24 comes as *big.Int

	token0Out := []interface{}{&token0Addr}
	token1Out := []interface{}{&token1Addr}
	feeOut := []interface{}{&feeBig}

	if err := contract.Call(opts, &token0Out, "token0"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token0 for V3 pool %s: %v", d.config.Name, addrStr, err)
		return nil, err
	}
	if err := contract.Call(opts, &token1Out, "token1"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token1 for V3 pool %s: %v", d.config.Name, addrStr, err)
		return nil, err
	}
	if err := contract.Call(opts, &feeOut, "fee"); err != nil {
		log.Printf("[%s] ⚠ Failed to get fee for V3 pool %s: %v", d.config.Name, addrStr, err)
		return nil, err
	}

	// Convert big.Int to uint32
	fee := uint32(feeBig.Uint64())

	token0Info, err := d.GetTokenInfo(ctx, token0Addr)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get token0 info: %v", d.config.Name, err)
		return nil, err
	}

	token1Info, err := d.GetTokenInfo(ctx, token1Addr)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get token1 info: %v", d.config.Name, err)
		return nil, err
	}

	pair := PairInfo{
		Address:    addrStr,
		Version:    "V3",
		Token0:     strings.ToLower(token0Addr.Hex()),
		Token1:     strings.ToLower(token1Addr.Hex()),
		Token0Info: token0Info,
		Token1Info: token1Info,
		Fee:        &fee,
	}

	d.cache.SetPair(cacheKey, pair)
	return &pair, nil
}

// ProcessSingleLog processes a single log event from WebSocket subscription
func (d *SwapDecoder) ProcessSingleLog(ctx context.Context, logData map[string]interface{}) {
	// Parse the log data from WebSocket
	topicsRaw, ok := logData["topics"].([]interface{})
	if !ok || len(topicsRaw) == 0 {
		log.Printf("[%s] ⚠ Failed to parse topics from log data", d.config.Name)
		return
	}

	// Get first topic (event signature)
	topicStr, ok := topicsRaw[0].(string)
	if !ok {
		log.Printf("[%s] ⚠ Failed to parse topic string", d.config.Name)
		return
	}
	topic := common.HexToHash(topicStr)

	// Get contract address
	addressStr, ok := logData["address"].(string)
	if !ok {
		log.Printf("[%s] ⚠ Failed to parse address from log data", d.config.Name)
		return
	}
	address := common.HexToAddress(addressStr)

	// Get transaction hash and block number
	txHashStr, ok := logData["transactionHash"].(string)
	if !ok {
		log.Printf("[%s] ⚠ Failed to parse transaction hash from log data", d.config.Name)
		return
	}
	txHash := common.HexToHash(txHashStr)

	blockNumStr, ok := logData["blockNumber"].(string)
	if !ok {
		log.Printf("[%s] ⚠ Failed to parse block number from log data", d.config.Name)
		return
	}
	blockNum, err := strconv.ParseUint(blockNumStr[2:], 16, 64)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to convert block number %s: %v", d.config.Name, blockNumStr, err)
		return
	}

	// Get log data
	dataStr, ok := logData["data"].(string)
	if !ok {
		log.Printf("[%s] ⚠ Failed to parse data from log data", d.config.Name)
		return
	}
	data, err := hex.DecodeString(dataStr[2:])
	if err != nil {
		log.Printf("[%s] ⚠ Failed to decode hex data %s: %v", d.config.Name, dataStr, err)
		return
	}

	// Parse topics
	var topics []common.Hash
	for _, t := range topicsRaw {
		if tStr, ok := t.(string); ok {
			topics = append(topics, common.HexToHash(tStr))
		}
	}

	// Create a minimal receipt-like structure
	receipt := &types.Receipt{
		TxHash:      txHash,
		BlockNumber: big.NewInt(int64(blockNum)),
	}

	// Create log entry
	logEntry := &types.Log{
		Address: address,
		Topics:  topics,
		Data:    data,
	}

	// Process based on topic
	switch topic {
	case UniswapV2SwapTopic:
		d.decodeV2Swap(ctx, logEntry, receipt)
	case UniswapV3SwapTopic:
		d.decodeV3Swap(ctx, logEntry, receipt)
	case UniswapV4SwapTopic:
		d.decodeV4Swap(ctx, logEntry, receipt)
	}
}

func (d *SwapDecoder) decodeV2Swap(ctx context.Context, logEntry *types.Log, receipt *types.Receipt) {
	pairAddr := logEntry.Address

	// Apply pool filter if configured
	if !d.config.ShouldProcessPool(pairAddr.Hex()) {
		return
	}

	pairInfo, err := d.GetV2PairInfo(ctx, pairAddr)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get V2 pair info: %v", d.config.Name, err)
		return
	}

	var event V2SwapEvent
	if err := UniswapV2SwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack V2 swap event: %v", d.config.Name, err)
		return
	}

	// Parse indexed parameters
	if len(logEntry.Topics) >= 2 {
		event.Sender = common.BytesToAddress(logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.To = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}

	d.printV2Swap(pairInfo, &event, receipt)
}

func (d *SwapDecoder) decodeV3Swap(ctx context.Context, logEntry *types.Log, receipt *types.Receipt) {
	poolAddr := logEntry.Address

	// Apply pool filter if configured
	if !d.config.ShouldProcessPool(poolAddr.Hex()) {
		return
	}

	poolInfo, err := d.GetV3PoolInfo(ctx, poolAddr)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get V3 pool info: %v", d.config.Name, err)
		return
	}

	var event V3SwapEvent
	if err := UniswapV3SwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack V3 swap event: %v", d.config.Name, err)
		return
	}

	// Parse indexed parameters
	if len(logEntry.Topics) >= 2 {
		event.Sender = common.BytesToAddress(logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.Recipient = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}

	d.printV3Swap(poolInfo, &event, receipt)
}

func (d *SwapDecoder) decodeV4Swap(ctx context.Context, logEntry *types.Log, receipt *types.Receipt) {
	// V4 uses PoolManager contract, don't filter by contract address
	// Instead, we'll filter by PoolId after unpacking

	var event V4SwapEvent
	if err := UniswapV4SwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack V4 swap event: %v", d.config.Name, err)
		return
	}

	// Parse indexed parameters
	if len(logEntry.Topics) >= 2 {
		// Topic 1 is the pool ID (bytes32)
		copy(event.PoolId[:], logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.Sender = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}

	// Filter by PoolId if configured
	poolIdHex := "0x" + common.Bytes2Hex(event.PoolId[:])
	if !d.config.ShouldProcessPool(poolIdHex) {
		return
	}

	d.printV4Swap(&event, receipt)
}

func (d *SwapDecoder) printV2Swap(pair *PairInfo, event *V2SwapEvent, receipt *types.Receipt) {
	var tokenIn, tokenOut TokenInfo
	var amountIn, amountOut *big.Int

	if event.Amount0In.Cmp(big.NewInt(0)) > 0 {
		tokenIn = pair.Token0Info
		tokenOut = pair.Token1Info
		amountIn = event.Amount0In
		amountOut = event.Amount1Out
	} else {
		tokenIn = pair.Token1Info
		tokenOut = pair.Token0Info
		amountIn = event.Amount1In
		amountOut = event.Amount0Out
	}

	amountInFloat := formatAmount(amountIn, tokenIn.Decimals)
	amountOutFloat := formatAmount(amountOut, tokenOut.Decimals)

	log.Printf("[%s] Swap (V2) | %s/%s | In: %s %s -> Out: %s %s | To: %s | Tx: %s",
		d.config.Name,
		pair.Token0Info.Symbol,
		pair.Token1Info.Symbol,
		amountInFloat,
		tokenIn.Symbol,
		amountOutFloat,
		tokenOut.Symbol,
		event.To.Hex()[:10]+"...",
		receipt.TxHash.Hex()[:10]+"...")
}

func (d *SwapDecoder) printV3Swap(pool *PairInfo, event *V3SwapEvent, receipt *types.Receipt) {
	var tokenIn, tokenOut TokenInfo
	var amountIn, amountOut *big.Int

	if event.Amount0.Cmp(big.NewInt(0)) < 0 {
		tokenIn = pool.Token0Info
		tokenOut = pool.Token1Info
		amountIn = new(big.Int).Abs(event.Amount0)
		amountOut = event.Amount1
	} else {
		tokenIn = pool.Token1Info
		tokenOut = pool.Token0Info
		amountIn = new(big.Int).Abs(event.Amount1)
		amountOut = event.Amount0
	}

	amountInFloat := formatAmount(amountIn, tokenIn.Decimals)
	amountOutFloat := formatAmount(amountOut, tokenOut.Decimals)

	feePercent := float64(*pool.Fee) / 10000.0

	log.Printf("[%s] Swap (V3) Fee: %.2f%% | %s/%s | In: %s %s -> Out: %s %s | To: %s | Tx: %s",
		d.config.Name,
		feePercent,
		pool.Token0Info.Symbol,
		pool.Token1Info.Symbol,
		amountInFloat,
		tokenIn.Symbol,
		amountOutFloat,
		tokenOut.Symbol,
		event.Recipient.Hex()[:10]+"...",
		receipt.TxHash.Hex()[:10]+"...")
}

func (d *SwapDecoder) printV4Swap(event *V4SwapEvent, receipt *types.Receipt) {
	// For V4, we log with PoolId since we can't easily get token info without the PoolManager
	var tokenIn, tokenOut string
	var amountIn, amountOut *big.Int

	if event.Amount0.Cmp(big.NewInt(0)) < 0 {
		tokenIn = "Token0"
		tokenOut = "Token1"
		amountIn = new(big.Int).Abs(event.Amount0)
		amountOut = event.Amount1
	} else {
		tokenIn = "Token1"
		tokenOut = "Token0"
		amountIn = new(big.Int).Abs(event.Amount1)
		amountOut = event.Amount0
	}

	feePercent := float64(event.Fee.Uint64()) / 10000.0

	log.Printf("[%s] Swap (V4) PoolID: %x | Fee: %.2f%% | In: %s %s -> Out: %s %s | From: %s | Tx: %s",
		d.config.Name,
		event.PoolId[:8], // Show first 8 bytes of pool ID
		feePercent,
		amountIn.String(),
		tokenIn,
		amountOut.String(),
		tokenOut,
		event.Sender.Hex()[:10]+"...",
		receipt.TxHash.Hex()[:10]+"...",
		receipt.TxHash.Hex()[:10]+"...",
	)
}

func formatAmount(amount *big.Int, decimals uint8) string {
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	amountFloat := new(big.Float).SetInt(amount)
	result := new(big.Float).Quo(amountFloat, divisor)
	return result.Text('f', 2)
}
