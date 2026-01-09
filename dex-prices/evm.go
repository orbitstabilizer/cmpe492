package main

import (
	"context"
	"encoding/json"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gorilla/websocket"
)

// EVMListener handles EVM chain monitoring with all necessary dependencies
type EVMListener struct {
	config  ChainConfig
	client  *ethclient.Client
	cache   *Cache
	handler SwapHandler
}

// NewEVMListener creates a new EVM listener instance
func NewEVMListener(config ChainConfig) (*EVMListener, error) {
	cache := NewCache(config.ChainID)
	client, err := ethclient.Dial(config.RPCHTTP)
	if err != nil {
		return nil, err
	}

	return &EVMListener{
		config:  config,
		client:  client,
		cache:   cache,
		handler: NewDatabaseInsertSwapHandler(),
	}, nil
}

// startEVMListener handles EVM chain monitoring
func startEVMListener(config ChainConfig) {
	log.Printf("[%s] Starting EVM listener...", config.Name)

	// Create listener
	listener, err := NewEVMListener(config)
	if err != nil {
		log.Printf("[%s] ❌ Failed to initialize: %v", config.Name, err)
		return
	}

	// Reconnection loop
RECONNECT:
	conn, _, err := websocket.DefaultDialer.Dial(config.RPCWS, nil)
	if err != nil {
		log.Printf("[%s] ❌ Failed to connect to WebSocket: %v (reconnecting...)", config.Name, err)
		time.Sleep(time.Second * 2)
		goto RECONNECT
	}
	defer conn.Close()

	// Subscribe to swap events
	subscribeParams := map[string]interface{}{
		"topics": []interface{}{
			[]string{
				UniswapV2SwapTopic.Hex(),
				UniswapV3SwapTopic.Hex(),
				UniswapV4SwapTopic.Hex(),
				CurveTokenExchangeTopic.Hex(),
				BalancerSwapTopic.Hex(),
			},
		},
	}

	if config.HasAddressFilter() {
		subscribeParams["address"] = config.FilterAddresses
		log.Printf("[%s] 📍 Filtering %d specific address(es)", config.Name, len(config.FilterAddresses))
	}

	subscribeMsg := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params":  []interface{}{"logs", subscribeParams},
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		log.Printf("[%s] ❌ Failed to subscribe: %v", config.Name, err)
		goto RECONNECT
	}

	log.Printf("[%s] Subscribed to Uniswap V2/V3/V4, Curve, and Balancer Swap events", config.Name)

	// Message reading loop
	for {
		var msg map[string]interface{}
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("[%s] ⚠ Connection lost: %v (reconnecting...)", config.Name, err)
			goto RECONNECT
		}

		// Process logs
		if params, ok := msg["params"].(map[string]interface{}); ok {
			if result, ok := params["result"].(map[string]interface{}); ok {
				go listener.processLog(result)
			}
		}
	}
}

// processLog decodes and processes a single EVM log entry
func (l *EVMListener) processLog(logData map[string]interface{}) {
	// Parse raw log data into types.Log
	logJSON, err := json.Marshal(logData)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to marshal log data: %v", l.config.Name, err)
		return
	}

	var logEntry types.Log
	if err := json.Unmarshal(logJSON, &logEntry); err != nil {
		log.Printf("[%s] ⚠ Failed to unmarshal log entry: %v", l.config.Name, err)
		return
	}

	if len(logEntry.Topics) == 0 {
		return
	}

	eventSignature := logEntry.Topics[0]
	receipt := &types.Receipt{
		TxHash:      logEntry.TxHash,
		BlockNumber: big.NewInt(int64(logEntry.BlockNumber)),
	}

	// Route to appropriate decoder
	switch eventSignature {
	case UniswapV2SwapTopic:
		l.decodeV2Swap(&logEntry, receipt)
	case UniswapV3SwapTopic:
		l.decodeV3Swap(&logEntry, receipt)
	case UniswapV4SwapTopic:
		l.decodeV4Swap(&logEntry, receipt)
	case CurveTokenExchangeTopic:
		l.decodeCurveSwap(&logEntry, receipt)
	case BalancerSwapTopic:
		l.decodeBalancerSwap(&logEntry, receipt)
	default:
		// Log unrecognized events for debugging
		log.Printf("[%s] ⚠ Unrecognized event signature: %s (tx: %s)",
			l.config.Name, eventSignature.Hex(), logEntry.TxHash.Hex())
	}
}

// decodeV2Swap processes Uniswap V2 swap events
func (l *EVMListener) decodeV2Swap(logEntry *types.Log, receipt *types.Receipt) {
	pairInfo, err := l.getV2PairInfo(logEntry.Address)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get V2 pair info: %v", l.config.Name, err)
		return
	}

	var event V2SwapEvent
	if err := UniswapV2SwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack V2 swap event: %v", l.config.Name, err)
		return
	}

	if len(logEntry.Topics) >= 2 {
		event.Sender = common.BytesToAddress(logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.To = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}

	var tokenIn, tokenOut TokenInfo
	var amountIn, amountOut *big.Int

	if event.Amount0In.Cmp(big.NewInt(0)) > 0 {
		tokenIn, tokenOut = pairInfo.Token0Info, pairInfo.Token1Info
		amountIn, amountOut = event.Amount0In, event.Amount1Out
	} else {
		tokenIn, tokenOut = pairInfo.Token1Info, pairInfo.Token0Info
		amountIn, amountOut = event.Amount1In, event.Amount0Out
	}

	feePercent := float64(pairInfo.Fee) / 10000.0

	swapData := SwapData{
		Protocol:    "V2",
		ChainName:   l.config.Name,
		ChainType:   "evm",
		PoolAddress: logEntry.Address.Hex(),
		Token0:      pairInfo.Token0Info,
		Token1:      pairInfo.Token1Info,
		TokenIn:     tokenIn,
		TokenOut:    tokenOut,
		AmountIn:    amountIn,
		AmountOut:   amountOut,
		Fee:         &feePercent,
		Sender:      event.Sender.Hex(),
		Recipient:   event.To.Hex(),
		TxHash:      receipt.TxHash.Hex(),
		BlockNumber: receipt.BlockNumber.Uint64(),
	}

	l.handler.HandleSwap(swapData)
}

// decodeV3Swap processes Uniswap V3 swap events
func (l *EVMListener) decodeV3Swap(logEntry *types.Log, receipt *types.Receipt) {
	poolInfo, err := l.getV3PoolInfo(logEntry.Address)
	if err != nil {
		log.Printf("[%s] ⚠ Failed to get V3 pool info: %v", l.config.Name, err)
		return
	}

	var event V3SwapEvent
	if err := UniswapV3SwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack V3 swap event: %v", l.config.Name, err)
		return
	}

	if len(logEntry.Topics) >= 2 {
		event.Sender = common.BytesToAddress(logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.Recipient = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}

	var tokenIn, tokenOut TokenInfo
	var amountIn, amountOut *big.Int

	if event.Amount0.Sign() > 0 {
		tokenIn, tokenOut = poolInfo.Token0Info, poolInfo.Token1Info
		amountIn = event.Amount0
		amountOut = new(big.Int).Abs(event.Amount1)
	} else {
		tokenIn, tokenOut = poolInfo.Token1Info, poolInfo.Token0Info
		amountIn = new(big.Int).Abs(event.Amount0)
		amountOut = event.Amount1
	}

	feePercent := float64(poolInfo.Fee) / 10000.0

	swapData := SwapData{
		Protocol:    "V3",
		ChainName:   l.config.Name,
		ChainType:   "evm",
		PoolAddress: logEntry.Address.Hex(),
		Token0:      poolInfo.Token0Info,
		Token1:      poolInfo.Token1Info,
		TokenIn:     tokenIn,
		TokenOut:    tokenOut,
		AmountIn:    amountIn,
		AmountOut:   amountOut,
		Fee:         &feePercent,
		Sender:      event.Sender.Hex(),
		Recipient:   event.Recipient.Hex(),
		TxHash:      receipt.TxHash.Hex(),
		BlockNumber: receipt.BlockNumber.Uint64(),
	}

	l.handler.HandleSwap(swapData)
}

// decodeV4Swap processes Uniswap V4 swap events
func (l *EVMListener) decodeV4Swap(logEntry *types.Log, receipt *types.Receipt) {
	var event V4SwapEvent
	if err := UniswapV4SwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack V4 swap event: %v", l.config.Name, err)
		return
	}

	if len(logEntry.Topics) >= 2 {
		copy(event.PoolId[:], logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.Sender = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}

	poolIdHex := "0x" + common.Bytes2Hex(event.PoolId[:])

	var amountIn, amountOut *big.Int
	if event.Amount0.Cmp(big.NewInt(0)) < 0 {
		amountIn = new(big.Int).Abs(event.Amount0)
		amountOut = event.Amount1
	} else {
		amountIn = new(big.Int).Abs(event.Amount1)
		amountOut = event.Amount0
	}

	feePercent := float64(event.Fee.Uint64()) / 10000.0

	swapData := SwapData{
		Protocol:    "V4",
		ChainName:   l.config.Name,
		ChainType:   "evm",
		PoolID:      poolIdHex,
		PoolAddress: logEntry.Address.Hex(),
		AmountIn:    amountIn,
		AmountOut:   amountOut,
		Fee:         &feePercent,
		Sender:      event.Sender.Hex(),
		Recipient:   event.Sender.Hex(),
		TxHash:      receipt.TxHash.Hex(),
		BlockNumber: receipt.BlockNumber.Uint64(),
	}

	l.handler.HandleSwap(swapData)
}

// decodeCurveSwap processes Curve TokenExchange events
func (l *EVMListener) decodeCurveSwap(logEntry *types.Log, receipt *types.Receipt) {
	var event CurveTokenExchangeEvent
	if err := CurveTokenExchangeEventABI.UnpackIntoInterface(&event, "TokenExchange", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack Curve swap event: %v", l.config.Name, err)
		return
	}

	if len(logEntry.Topics) >= 2 {
		event.Buyer = common.BytesToAddress(logEntry.Topics[1].Bytes())
	}

	swapData := SwapData{
		Protocol:    "Curve",
		ChainName:   l.config.Name,
		ChainType:   "evm",
		PoolAddress: logEntry.Address.Hex(),
		AmountIn:    event.TokensSold,
		AmountOut:   event.TokensBought,
		Sender:      event.Buyer.Hex(),
		Recipient:   event.Buyer.Hex(),
		TxHash:      receipt.TxHash.Hex(),
		BlockNumber: receipt.BlockNumber.Uint64(),
	}

	l.handler.HandleSwap(swapData)
}

// decodeBalancerSwap processes Balancer V2 Swap events
func (l *EVMListener) decodeBalancerSwap(logEntry *types.Log, receipt *types.Receipt) {
	var event BalancerSwapEvent
	if err := BalancerSwapEventABI.UnpackIntoInterface(&event, "Swap", logEntry.Data); err != nil {
		log.Printf("[%s] ⚠ Failed to unpack Balancer swap event: %v", l.config.Name, err)
		return
	}

	if len(logEntry.Topics) >= 2 {
		copy(event.PoolId[:], logEntry.Topics[1].Bytes())
	}
	if len(logEntry.Topics) >= 3 {
		event.TokenIn = common.BytesToAddress(logEntry.Topics[2].Bytes())
	}
	if len(logEntry.Topics) >= 4 {
		event.TokenOut = common.BytesToAddress(logEntry.Topics[3].Bytes())
	}

	tokenInInfo, _ := l.getTokenInfo(event.TokenIn)
	tokenOutInfo, _ := l.getTokenInfo(event.TokenOut)

	poolIdHex := "0x" + common.Bytes2Hex(event.PoolId[:])

	swapData := SwapData{
		Protocol:    "Balancer",
		ChainName:   l.config.Name,
		ChainType:   "evm",
		PoolID:      poolIdHex,
		PoolAddress: logEntry.Address.Hex(),
		TokenIn:     tokenInInfo,
		TokenOut:    tokenOutInfo,
		AmountIn:    event.AmountIn,
		AmountOut:   event.AmountOut,
		Recipient:   logEntry.Address.Hex(),
		TxHash:      receipt.TxHash.Hex(),
		BlockNumber: receipt.BlockNumber.Uint64(),
	}

	l.handler.HandleSwap(swapData)
}

// =============================================================================
// Helper Methods for Token and Pair Metadata
// =============================================================================

// getTokenInfo retrieves token metadata (symbol, decimals) from contract or cache
func (l *EVMListener) getTokenInfo(address common.Address) (TokenInfo, error) {
	addrStr := strings.ToLower(address.Hex())

	// Check cache first
	if token, ok := l.cache.GetToken(addrStr); ok {
		return token, nil
	}

	// Create contract binding for ERC20 token
	contract := bind.NewBoundContract(address, ERC20ABI, l.client, l.client, l.client)
	opts := &bind.CallOpts{Context: context.Background()}

	// Get token symbol
	var symbol string
	symbolSuccess := false
	symbolOut := []interface{}{&symbol}
	if err := contract.Call(opts, &symbolOut, "symbol"); err != nil {
		// Some older tokens use bytes32 instead of string
		var symbolBytes32 [32]byte
		symbolBytes32Out := []interface{}{&symbolBytes32}
		if err := contract.Call(opts, &symbolBytes32Out, "symbol"); err == nil {
			symbol = strings.TrimRight(string(symbolBytes32[:]), "\x00")
			symbolSuccess = true
		} else {
			log.Printf("[%s] ⚠ Failed to get symbol for token %s: %v", l.config.Name, addrStr, err)
			symbol = "UNKNOWN"
		}
	} else {
		symbolSuccess = true
	}

	// Get token decimals
	var decimals uint8
	decimalsSuccess := false
	decimalsOut := []interface{}{&decimals}
	if err := contract.Call(opts, &decimalsOut, "decimals"); err != nil {
		log.Printf("[%s] ⚠ Failed to get decimals for token %s, using default 18: %v", l.config.Name, addrStr, err)
		decimals = 18
	} else {
		decimalsSuccess = true
	}

	token := TokenInfo{
		Address:  addrStr,
		Symbol:   symbol,
		Decimals: decimals,
	}

	// Only cache if both symbol and decimals were retrieved successfully
	if symbolSuccess && decimalsSuccess {
		l.cache.SetToken(addrStr, token)
	}

	return token, nil
}

// getV2PairInfo retrieves Uniswap V2 pair metadata (tokens and fee)
func (l *EVMListener) getV2PairInfo(pairAddr common.Address) (*PairInfo, error) {
	addrStr := strings.ToLower(pairAddr.Hex())
	cacheKey := "v2_" + addrStr

	// Check cache
	if pair, ok := l.cache.GetPair(cacheKey); ok {
		return &pair, nil
	}

	// Query pair contract for token addresses
	contract := bind.NewBoundContract(pairAddr, UniswapV2PairABI, l.client, l.client, l.client)
	opts := &bind.CallOpts{Context: context.Background()}

	var token0Addr, token1Addr common.Address
	token0Out := []interface{}{&token0Addr}
	token1Out := []interface{}{&token1Addr}

	if err := contract.Call(opts, &token0Out, "token0"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token0 for pair %s: %v", l.config.Name, addrStr, err)
		return nil, err
	}
	if err := contract.Call(opts, &token1Out, "token1"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token1 for pair %s: %v", l.config.Name, addrStr, err)
		return nil, err
	}

	// Fetch token metadata
	token0Info, err := l.getTokenInfo(token0Addr)
	if err != nil {
		return nil, err
	}
	token1Info, err := l.getTokenInfo(token1Addr)
	if err != nil {
		return nil, err
	}

	// Build and cache pair info
	pair := PairInfo{
		Address:    addrStr,
		Version:    "V2",
		Token0:     strings.ToLower(token0Addr.Hex()),
		Token1:     strings.ToLower(token1Addr.Hex()),
		Token0Info: token0Info,
		Token1Info: token1Info,
		Fee:        30, // V2 fixed fee: 0.30% = 30 basis points
	}
	l.cache.SetPair(cacheKey, pair)
	return &pair, nil
}

// getV3PoolInfo retrieves Uniswap V3 pool metadata (tokens and dynamic fee tier)
func (l *EVMListener) getV3PoolInfo(poolAddr common.Address) (*PairInfo, error) {
	addrStr := strings.ToLower(poolAddr.Hex())
	cacheKey := "v3_" + addrStr

	// Check cache
	if pair, ok := l.cache.GetPair(cacheKey); ok {
		return &pair, nil
	}

	// Query pool contract for token addresses and fee tier
	contract := bind.NewBoundContract(poolAddr, UniswapV3PoolABI, l.client, l.client, l.client)
	opts := &bind.CallOpts{Context: context.Background()}

	var token0Addr, token1Addr common.Address
	var feeBig *big.Int

	token0Out := []interface{}{&token0Addr}
	token1Out := []interface{}{&token1Addr}
	feeOut := []interface{}{&feeBig}

	if err := contract.Call(opts, &token0Out, "token0"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token0 for V3 pool %s: %v", l.config.Name, addrStr, err)
		return nil, err
	}
	if err := contract.Call(opts, &token1Out, "token1"); err != nil {
		log.Printf("[%s] ⚠ Failed to get token1 for V3 pool %s: %v", l.config.Name, addrStr, err)
		return nil, err
	}
	if err := contract.Call(opts, &feeOut, "fee"); err != nil {
		log.Printf("[%s] ⚠ Failed to get fee for V3 pool %s: %v", l.config.Name, addrStr, err)
		return nil, err
	}

	// Fetch token metadata
	token0Info, err := l.getTokenInfo(token0Addr)
	if err != nil {
		return nil, err
	}
	token1Info, err := l.getTokenInfo(token1Addr)
	if err != nil {
		return nil, err
	}

	// Build and cache pool info
	pair := PairInfo{
		Address:    addrStr,
		Version:    "V3",
		Token0:     strings.ToLower(token0Addr.Hex()),
		Token1:     strings.ToLower(token1Addr.Hex()),
		Token0Info: token0Info,
		Token1Info: token1Info,
		Fee:        uint32(feeBig.Uint64()),
	}
	l.cache.SetPair(cacheKey, pair)
	return &pair, nil
}
