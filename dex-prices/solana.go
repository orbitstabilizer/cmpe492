package main

import (
	"log"
	"math/big"
	"time"

	"github.com/gorilla/websocket"
)

// Solana program IDs for major DEXes
const (
	// Jupiter Aggregator V6
	JupiterV6ProgramID = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

	// Raydium AMM V4
	RaydiumAMMV4ProgramID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

	// Raydium CLMM (Concentrated Liquidity)
	RaydiumCLMMProgramID = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"

	// Orca Whirlpool
	OrcaWhirlpoolProgramID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"

	// Meteora DLMM (Dynamic Liquidity Market Maker)
	MeteoraDLMMProgramID = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

	// Lifinity V2
	LifinityV2ProgramID = "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c"

	// Phoenix (Order book DEX)
	PhoenixProgramID = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY"

	// Pump.fun (Bonding curve)
	PumpFunProgramID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

// SolanaListener manages Solana chain monitoring with WebSocket connection
type SolanaListener struct {
	config  ChainConfig
	cache   *Cache
	handler SwapHandler
}

// NewSolanaListener creates a new Solana listener with config, cache, and handler
func NewSolanaListener(config ChainConfig) *SolanaListener {
	return &SolanaListener{
		config:  config,
		cache:   NewCache(config.ChainID),
		handler: NewDatabaseInsertSwapHandler(),
	}
}

// startSolanaListener handles Solana chain monitoring
func startSolanaListener(config ChainConfig) {
	listener := NewSolanaListener(config)
	log.Printf("[%s] Starting Solana listener...", listener.config.Name)

	// Reconnection loop
RECONNECT:
	conn, _, err := websocket.DefaultDialer.Dial(listener.config.RPCWS, nil)
	if err != nil {
		log.Printf("[%s] ❌ Failed to connect to WebSocket: %v (reconnecting...)", listener.config.Name, err)
		time.Sleep(time.Second * 2)
		goto RECONNECT
	}
	defer conn.Close()

	// Subscribe to swap events from multiple programs
	programsToSubscribe := map[string]string{
		"Jupiter":      JupiterV6ProgramID,
		"Raydium V4":   RaydiumAMMV4ProgramID,
		"Raydium CLMM": RaydiumCLMMProgramID,
		"Orca":         OrcaWhirlpoolProgramID,
		"Meteora":      MeteoraDLMMProgramID,
		"Lifinity":     LifinityV2ProgramID,
		"Phoenix":      PhoenixProgramID,
		"Pump.fun":     PumpFunProgramID,
	}

	for dexName, programID := range programsToSubscribe {
		subscribeMsg := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "logsSubscribe",
			"params": []interface{}{
				map[string]interface{}{
					"mentions": []string{programID},
				},
				map[string]interface{}{
					"commitment": "confirmed",
				},
			},
		}

		if err := conn.WriteJSON(subscribeMsg); err != nil {
			log.Printf("[%s] ⚠ Failed to subscribe to %s: %v (reconnecting...)", listener.config.Name, dexName, err)
			goto RECONNECT
		}
		log.Printf("[%s] 📍 Subscribed to %s swaps (%s)", listener.config.Name, dexName, programID[:8]+"...")
	}

	log.Printf("[%s] Subscribed to Jupiter, Raydium, Orca, Meteora, Lifinity, Phoenix, and Pump.fun swap events", listener.config.Name)

	// Message reading loop
	for {
		var msg map[string]interface{}
		if err := conn.ReadJSON(&msg); err != nil {
			log.Printf("[%s] ⚠ Connection lost: %v (reconnecting...)", listener.config.Name, err)
			goto RECONNECT
		}

		// Process logs
		if params, ok := msg["params"].(map[string]interface{}); ok {
			if result, ok := params["result"].(map[string]interface{}); ok {
				if value, ok := result["value"].(map[string]interface{}); ok {
					go listener.processLog(value)
				}
			}
		}
	}
}

// processLog decodes and processes a single Solana log entry
func (l *SolanaListener) processLog(logData map[string]interface{}) {
	// Extract signature
	signature, ok := logData["signature"].(string)
	if !ok {
		return
	}

	// Extract logs
	logs, ok := logData["logs"].([]interface{})
	if !ok || len(logs) == 0 {
		return
	}

	// Determine which DEX based on program mentioned in logs
	var protocol string
	for _, logEntry := range logs {
		if s, ok := logEntry.(string); ok {
			if containsProgram(s, JupiterV6ProgramID) {
				protocol = "Jupiter"
				break
			} else if containsProgram(s, RaydiumAMMV4ProgramID) {
				protocol = "Raydium V4"
				break
			} else if containsProgram(s, RaydiumCLMMProgramID) {
				protocol = "Raydium CLMM"
				break
			} else if containsProgram(s, OrcaWhirlpoolProgramID) {
				protocol = "Orca"
				break
			} else if containsProgram(s, MeteoraDLMMProgramID) {
				protocol = "Meteora"
				break
			} else if containsProgram(s, LifinityV2ProgramID) {
				protocol = "Lifinity"
				break
			} else if containsProgram(s, PhoenixProgramID) {
				protocol = "Phoenix"
				break
			} else if containsProgram(s, PumpFunProgramID) {
				protocol = "Pump.fun"
				break
			}
		}
	}

	if protocol == "" {
		log.Printf("No recognized DEX program found in logs for signature %s", signature)
		return
	}

	// Extract slot
	slot := uint64(0)
	if s, ok := logData["slot"].(float64); ok {
		slot = uint64(s)
	}

	log.Printf("Detected %s swap in transaction %s (slot %d)", protocol, signature, slot)

	// Route to appropriate decoder based on protocol
	switch protocol {
	case "Jupiter":
		l.decodeJupiterSwap(signature, slot)
	case "Raydium V4":
		l.decodeRaydiumSwap(signature, slot)
	case "Raydium CLMM":
		l.decodeRaydiumCLMMSwap(signature, slot)
	case "Orca":
		l.decodeOrcaSwap(signature, slot)
	case "Meteora":
		l.decodeMeteoraSwap(signature, slot)
	case "Lifinity":
		l.decodeLifinitySwap(signature, slot)
	case "Phoenix":
		l.decodePhoenixSwap(signature, slot)
	case "Pump.fun":
		l.decodePumpFunSwap(signature, slot)
	default:
		log.Printf("Unknown protocol %s for signature %s", protocol, signature)
	}
}

// decodeJupiterSwap processes Jupiter swap logs
func (l *SolanaListener) decodeJupiterSwap(signature string, slot uint64) {
	// Note: Full implementation would parse instruction data from the transaction
	// For now, we create a basic SwapData structure
	swapData := SwapData{
		ChainType:   "solana",
		Protocol:    "Jupiter",
		ChainName:   l.config.Name,
		PoolAddress: "TBD",                                                 // Would parse from instruction
		Token0:      TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9}, // Placeholder
		Token1:      TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9}, // Placeholder
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9}, // Would parse from instruction
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9}, // Would parse from instruction
		AmountIn:    big.NewInt(0),                                         // Would parse from instruction
		AmountOut:   big.NewInt(0),                                         // Would parse from instruction
		Fee:         nil,
		Recipient:   "TBD",
		TxHash:      signature,
		BlockNumber: slot,
	}

	l.handler.HandleSwap(swapData)
}

// decodeRaydiumSwap processes Raydium swap logs
func (l *SolanaListener) decodeRaydiumSwap(signature string, slot uint64) {
	// Note: Full implementation would parse instruction data from the transaction
	swapData := SwapData{
		ChainType:   "solana",
		Protocol:    "Raydium",
		ChainName:   l.config.Name,
		PoolAddress: "TBD",
		Token0:      TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		Token1:      TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Fee:         nil,
		Recipient:   "TBD",
		TxHash:      signature,
		BlockNumber: slot,
	}

	l.handler.HandleSwap(swapData)
}

// decodeOrcaSwap processes Orca swap logs
func (l *SolanaListener) decodeOrcaSwap(signature string, slot uint64) {
	// Note: Full implementation would parse instruction data from the transaction
	swapData := SwapData{
		ChainType:   "solana",
		Protocol:    "Orca",
		ChainName:   l.config.Name,
		PoolAddress: "TBD",
		Token0:      TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		Token1:      TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 9},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Fee:         nil,
		Recipient:   "TBD",
		TxHash:      signature,
		BlockNumber: slot,
	}

	l.handler.HandleSwap(swapData)
}

// containsProgram checks if a log line mentions a program ID
func containsProgram(logLine, programID string) bool {
	return len(logLine) > 0 && len(programID) > 0 &&
		(logLine == programID ||
			(len(logLine) > len(programID) && logLine[:len(programID)] == programID))
}

func (l *SolanaListener) decodeRaydiumCLMMSwap(signature string, slot uint64) {
	// TBD: Fetch transaction and parse Raydium CLMM instruction
	// Accounts: [payer, amm_config, pool_state, input_token_account, output_token_account, input_vault, output_vault, observation_state, token_program, tick_array_state, ...]
	// Parse instruction data for amount_in, amount_out_minimum, sqrt_price_limit_x64

	swapData := SwapData{
		TxHash:      signature,
		BlockNumber: slot,
		ChainName:   l.config.Name,
		ChainType:   "Solana",
		Protocol:    "Raydium CLMM",
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Sender:      "TBD",
		Recipient:   "TBD",
	}
	l.handler.HandleSwap(swapData)
}

func (l *SolanaListener) decodeMeteoraSwap(signature string, slot uint64) {
	// TBD: Fetch transaction and parse Meteora DLMM instruction
	// Accounts: [lb_pair, bin_array_bitmap_extension, reserve_x, reserve_y, user_token_x, user_token_y, token_x_mint, token_y_mint, oracle, host_fee_in, ...]
	// Parse instruction data for amount_in, min_amount_out

	swapData := SwapData{
		TxHash:      signature,
		BlockNumber: slot,
		ChainName:   l.config.Name,
		ChainType:   "Solana",
		Protocol:    "Meteora",
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Sender:      "TBD",
		Recipient:   "TBD",
	}
	l.handler.HandleSwap(swapData)
}

func (l *SolanaListener) decodeLifinitySwap(signature string, slot uint64) {
	// TBD: Fetch transaction and parse Lifinity V2 instruction
	// Accounts: [amm, amm_authority, source_info, destination_info, swap_source, swap_destination, pool_mint, fee_account, token_program, pyth_account, pyth_price_account]
	// Parse instruction data for amount_in, minimum_amount_out

	swapData := SwapData{
		TxHash:      signature,
		BlockNumber: slot,
		ChainName:   l.config.Name,
		ChainType:   "Solana",
		Protocol:    "Lifinity",
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Sender:      "TBD",
		Recipient:   "TBD",
	}
	l.handler.HandleSwap(swapData)
}

func (l *SolanaListener) decodePhoenixSwap(signature string, slot uint64) {
	// TBD: Fetch transaction and parse Phoenix order book instruction
	// Phoenix uses order book model, need to parse PlaceLimitOrder or Swap instruction
	// Accounts: [phoenix_program, log_authority, market, trader, base_account, quote_account, base_vault, quote_vault, token_program]
	// Parse instruction data for side, price, size

	swapData := SwapData{
		TxHash:      signature,
		BlockNumber: slot,
		ChainName:   l.config.Name,
		ChainType:   "Solana",
		Protocol:    "Phoenix",
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Sender:      "TBD",
		Recipient:   "TBD",
	}
	l.handler.HandleSwap(swapData)
}

func (l *SolanaListener) decodePumpFunSwap(signature string, slot uint64) {
	// TBD: Fetch transaction and parse Pump.fun bonding curve instruction
	// Accounts: [global, fee_recipient, mint, bonding_curve, associated_bonding_curve, associated_user, user, system_program, token_program, rent, event_authority, program]
	// Parse instruction data for amount, sol_amount, is_buy

	swapData := SwapData{
		TxHash:      signature,
		BlockNumber: slot,
		ChainName:   l.config.Name,
		ChainType:   "Solana",
		Protocol:    "Pump.fun",
		TokenIn:     TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		TokenOut:    TokenInfo{Address: "TBD", Symbol: "TBD", Decimals: 0},
		AmountIn:    big.NewInt(0),
		AmountOut:   big.NewInt(0),
		Sender:      "TBD",
		Recipient:   "TBD",
	}
	l.handler.HandleSwap(swapData)
}
