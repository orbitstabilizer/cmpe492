package main

import (
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
)

// SwapData represents a decoded swap event from any chain (EVM or Solana)
type SwapData struct {
	// Common fields
	Protocol  string // "V2", "V3", "V4", "Curve", "Balancer", "Jupiter", "Raydium", "Orca"
	ChainName string // Chain name from config
	ChainType string // "evm" or "solana"

	// Pool/Pair information
	PoolAddress string // Pool/pair contract address (EVM) or program address (Solana)
	PoolID      string // For V4 and Balancer (bytes32 pool ID), empty for others

	// Token information (may be empty for some protocols)
	Token0   TokenInfo // First token info (EVM pairs)
	Token1   TokenInfo // Second token info (EVM pairs)
	TokenIn  TokenInfo // Input token
	TokenOut TokenInfo // Output token

	// Amounts
	AmountIn  *big.Int // Raw input amount
	AmountOut *big.Int // Raw output amount

	// Optional fields
	Fee *float64 // Fee percentage (optional, mainly for EVM)

	// Transaction details
	Sender      string // Transaction sender/signer
	Recipient   string // Swap recipient
	TxHash      string // Transaction hash (EVM) or signature (Solana)
	BlockNumber uint64 // Block number (EVM) or slot (Solana)

	// Pool State (Snapshot at swap time)
	Reserve0     *big.Int // Reserve0 (V2)
	Reserve1     *big.Int // Reserve1 (V2)
	SqrtPriceX96 *big.Int // Uniswap V3/V4
	Tick         *big.Int // Uniswap V3/V4
	Liquidity    *big.Int // Uniswap V3/V4

	// Calculated fields
	TradeSizeUSD *float64
}

// SwapHandler defines the interface for handling decoded swap events
type SwapHandler interface {
	HandleSwap(swap SwapData)
	HandlePoolState(poolAddress, chain, dex string, blockNumber uint64, txHash string, reserve0, reserve1 *big.Int)
}

// DatabaseInsertSwapHandler logs swap events to console and writes to DB
type DatabaseInsertSwapHandler struct {
	dbWriter *DexDatabaseWriter
}

// NewDatabaseInsertSwapHandler creates a new log-based swap handler with DB connection
func NewDatabaseInsertSwapHandler() *DatabaseInsertSwapHandler {
	connStr := os.Getenv("DB_CONN_STR")
	if connStr == "" {
		user := os.Getenv("POSTGRES_USER")
		if user == "" {
			panic("Required environment variable missing: POSTGRES_USER")
		}
		password := os.Getenv("POSTGRES_PASSWORD")
		if password == "" {
			panic("Required environment variable missing: POSTGRES_PASSWORD")
		}
		dbname := os.Getenv("POSTGRES_DB")
		if dbname == "" {
			panic("Required environment variable missing: POSTGRES_DB")
		}
		host := os.Getenv("POSTGRES_HOST")
		if host == "" {
			panic("Required environment variable missing: POSTGRES_HOST")
		}
		port := os.Getenv("POSTGRES_PORT")
		if port == "" {
			panic("Required environment variable missing: POSTGRES_PORT")
		}
		connStr = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbname)
	}
	dbWriter, err := NewDexDatabaseWriter(connStr)
	if err != nil {
		panic(err)
	}
	return &DatabaseInsertSwapHandler{dbWriter: dbWriter}
}

// HandleSwap logs the swap event to console
func (h *DatabaseInsertSwapHandler) HandleSwap(swap SwapData) {
	// Convert big.Int to float64 with decimal adjustment
	var amountIn, amountOut float64

	// Helper to convert raw amount to human amount
	toHuman := func(raw *big.Int, decimals uint8) float64 {
		if raw == nil {
			return 0
		}
		f, _ := new(big.Float).SetInt(raw).Float64()
		if decimals > 0 {
			return f / math.Pow(10, float64(decimals))
		}
		return f
	}

	amountIn = toHuman(swap.AmountIn, swap.TokenIn.Decimals)
	amountOut = toHuman(swap.AmountOut, swap.TokenOut.Decimals)

	// Calculate price as exchange rate (how many output tokens per input token)
	var price float64
	if amountIn > amountOut {
		price = amountIn / amountOut
	} else {
		price = amountOut / amountIn
	}

	// Debug logging for suspicious prices
	if price > 1e7 {
		log.Printf("⚠️ HIGH PRICE DETECTED: %.18f", price)
		log.Printf("   TokenIn: %s (decimals: %d)", swap.TokenIn.Symbol, swap.TokenIn.Decimals)
		log.Printf("   TokenOut: %s (decimals: %d)", swap.TokenOut.Symbol, swap.TokenOut.Decimals)
		log.Printf("   AmountIn (raw): %s", swap.AmountIn.String())
		log.Printf("   AmountOut (raw): %s", swap.AmountOut.String())
		log.Printf("   AmountIn (human): %.18f", amountIn)
		log.Printf("   AmountOut (human): %.18f", amountOut)
		log.Printf("   Pool: %s", swap.PoolAddress)
		log.Printf("   TxHash: %s", swap.TxHash)

		// Sanity check: if AmountIn raw is huge but decimals is small (like USDT=6),
		// the raw amount might actually be in the other token's units
		log.Printf("   ⚠️ POSSIBLE ISSUE: Raw amounts may be using wrong token decimals!")
		log.Printf("   Suggestion: Check if AmountIn/AmountOut are swapped in the EVM parser")
	}

	// Insert swap
	dbSwap := SwapEvent{
		Chain:       swap.ChainName,
		Dex:         swap.Protocol,
		PoolAddress: swap.PoolAddress,
		TokenIn:     swap.TokenIn.Address,
		TokenOut:    swap.TokenOut.Address,
		AmountIn:    amountIn,
		AmountOut:   amountOut,
		Price:       price,
		TxHash:      swap.TxHash,
		BlockNumber: int64(swap.BlockNumber),
	}
	err := h.dbWriter.InsertSwap(dbSwap)
	if err != nil {
		log.Printf("⚠ Failed to write swap to DB: %v", err)
	}

	// Upsert Tokens
	if swap.TokenIn.Address != "" {
		_ = h.dbWriter.UpsertToken(swap.TokenIn.Address, swap.TokenIn.Symbol, int(swap.TokenIn.Decimals), swap.ChainName)
	}
	if swap.TokenOut.Address != "" {
		_ = h.dbWriter.UpsertToken(swap.TokenOut.Address, swap.TokenOut.Symbol, int(swap.TokenOut.Decimals), swap.ChainName)
	}

	// Prepare Pool Info
	token0Addr := swap.Token0.Address
	token1Addr := swap.Token1.Address
	// If Token0/1 not explicitly set (e.g. some parsers), derive from In/Out by sorting
	if token0Addr == "" {
		if swap.TokenIn.Address < swap.TokenOut.Address {
			token0Addr = swap.TokenIn.Address
			token1Addr = swap.TokenOut.Address
		} else {
			token0Addr = swap.TokenOut.Address
			token1Addr = swap.TokenIn.Address
		}
	}

	// Upsert Pool
	// Fee tier is optional, defaulting to 0.0 if not present
	var fee float64
	if swap.Fee != nil {
		fee = *swap.Fee // Access pointer value directly
	}
	err = h.dbWriter.UpsertPool(swap.PoolAddress, swap.ChainName, swap.Protocol, token0Addr, token1Addr, fee)
	if err != nil {
		log.Printf("⚠ Failed to upsert pool: %v", err)
	}

	// Insert Pool State (Snapshot)
	// Skip for V2
	if swap.Protocol != "V2" {
		err = h.dbWriter.InsertPoolState(
			swap.PoolAddress,
			swap.ChainName,
			swap.Protocol,
			int64(swap.BlockNumber),
			swap.TxHash,
			swap.Reserve0,
			swap.Reserve1,
			swap.SqrtPriceX96,
			swap.Liquidity,
			swap.Tick,
		)
		if err != nil {
			log.Printf("⚠ Failed to insert pool state: %v", err)
		}
	}
}

// HandlePoolState processes and stores pool reserve updates (e.g. Uniswap V2 Sync)
func (h *DatabaseInsertSwapHandler) HandlePoolState(poolAddress, chain, dex string, blockNumber uint64, txHash string, reserve0, reserve1 *big.Int) {
	// Insert simple pool state update (reserves only)
	// We pass nil for V3 fields
	err := h.dbWriter.InsertPoolState(
		poolAddress,
		chain,
		dex,
		int64(blockNumber),
		txHash,
		reserve0,
		reserve1,
		nil, // SqrtPriceX96
		nil, // Liquidity (V3)
		nil, // Tick
	)
	if err != nil {
		log.Printf("⚠ Failed to insert pool state (Sync): %v", err)
	}
}
