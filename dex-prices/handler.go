package main

import (
	"fmt"
	"log"
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
}

// SwapHandler defines the interface for handling decoded swap events
type SwapHandler interface {
	HandleSwap(swap SwapData)
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
	defer dbWriter.Close()
	return &DatabaseInsertSwapHandler{dbWriter: dbWriter}
}

// HandleSwap logs the swap event to console
func (h *DatabaseInsertSwapHandler) HandleSwap(swap SwapData) {
	// Convert big.Int to float64 (naive conversion for now)
	amountIn, _ := new(big.Float).SetInt(swap.AmountIn).Float64()
	amountOut, _ := new(big.Float).SetInt(swap.AmountOut).Float64()

	// Calculate price (simple division)
	var price float64
	if amountIn > 0 {
		price = amountOut / amountIn
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
		// Name might be missing in TokenInfo, using empty string or Symbol as fallback if needed.
		// Casting Decimals to int.
		_ = h.dbWriter.UpsertToken(swap.TokenIn.Address, swap.TokenIn.Symbol, "", int(swap.TokenIn.Decimals), swap.ChainName)
	}
	if swap.TokenOut.Address != "" {
		_ = h.dbWriter.UpsertToken(swap.TokenOut.Address, swap.TokenOut.Symbol, "", int(swap.TokenOut.Decimals), swap.ChainName)
	}
}
