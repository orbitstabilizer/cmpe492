package main

import (
	"fmt"
	"log"
	"math/big"
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

// LogSwapHandler is a simple handler that logs swap events to console
type LogSwapHandler struct{}

// NewLogSwapHandler creates a new log-based swap handler
func NewLogSwapHandler() *LogSwapHandler {
	return &LogSwapHandler{}
}

// HandleSwap logs the swap event to console
func (h *LogSwapHandler) HandleSwap(swap SwapData) {
	// Format fee string if provided
	var feeStr string
	if swap.Fee != nil {
		feeStr = fmt.Sprintf(" | Fee: %.2f%%", *swap.Fee)
	}

	// Format pair/pool identifier
	var pairStr string
	if swap.Token1.Symbol != "" {
		pairStr = swap.Token0.Symbol + "/" + swap.Token1.Symbol
	} else if swap.PoolID != "" {
		pairStr = swap.PoolID // Full pool ID
	} else {
		pairStr = swap.PoolAddress // Full pool address
	}

	// Format swap direction
	var swapStr string
	if swap.TokenIn.Symbol != "" && swap.TokenOut.Symbol != "" {
		swapStr = fmt.Sprintf("In: %s %s -> Out: %s %s",
			swap.AmountIn.String(), swap.TokenIn.Symbol,
			swap.AmountOut.String(), swap.TokenOut.Symbol)
	} else {
		swapStr = fmt.Sprintf("In: %s -> Out: %s", swap.AmountIn.String(), swap.AmountOut.String())
	}

	// Log the formatted swap with full addresses and hashes
	log.Printf("[%s] Swap (%s)%s | %s | %s | To: %s | Tx: %s",
		swap.ChainName,
		swap.Protocol,
		feeStr,
		pairStr,
		swapStr,
		swap.Recipient,
		swap.TxHash)
}
