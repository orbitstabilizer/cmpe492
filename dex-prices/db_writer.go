package main

import (
	"database/sql"
	"log"
	"math/big"
	"time"

	_ "github.com/lib/pq"
)

type SwapEvent struct {
	Chain       string
	Dex         string
	PoolAddress string
	TokenIn     string
	TokenOut    string
	AmountIn    float64
	AmountOut   float64
	Price       float64
	TxHash      string
	BlockNumber int64
}

type DexDatabaseWriter struct {
	db *sql.DB
}

func NewDexDatabaseWriter(connStr string) (*DexDatabaseWriter, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	log.Println("DEX Database writer connected")
	return &DexDatabaseWriter{db: db}, nil
}

func (w *DexDatabaseWriter) InsertSwap(swap SwapEvent) error {
	query := `
        INSERT INTO dex_swaps (time, chain, dex, pool_address, token_in, token_out,
                               amount_in, amount_out, price, tx_hash, block_number)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `
	_, err := w.db.Exec(query,
		time.Now(),
		swap.Chain,
		swap.Dex,
		swap.PoolAddress,
		swap.TokenIn,
		swap.TokenOut,
		swap.AmountIn,
		swap.AmountOut,
		swap.Price,
		swap.TxHash,
		swap.BlockNumber,
	)

	if err != nil {
		return err
	}

	return nil
}

func (w *DexDatabaseWriter) GetLatestPrice(symbol string) (float64, error) {
	// Normalize symbol to match price_index format (assuming lowercase + usdt usually)
	// We try exact match first
	query := `
        SELECT price_index 
        FROM price_index 
        WHERE symbol = $1 
        ORDER BY time DESC 
        LIMIT 1
    `
	var price float64
	err := w.db.QueryRow(query, symbol).Scan(&price)
	if err != nil {
		return 0, err
	}
	return price, nil
}

func (w *DexDatabaseWriter) UpsertToken(address, symbol string, decimals int, chain string) error {
	query := `
        INSERT INTO tokens (address, symbol, decimals, chain)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (address) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            decimals = EXCLUDED.decimals
    `
	_, err := w.db.Exec(query, address, symbol, decimals, chain)
	return err
}

func (w *DexDatabaseWriter) UpsertPool(poolAddress, chain, dex, token0, token1 string, feeTier float64) error {
	query := `
        INSERT INTO pools (pool_address, chain, dex, token0_address, token1_address, fee_tier)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (pool_address) DO UPDATE SET
            token0_address = EXCLUDED.token0_address,
            token1_address = EXCLUDED.token1_address,
            fee_tier = EXCLUDED.fee_tier,
            last_updated = NOW()
    `
	_, err := w.db.Exec(query, poolAddress, chain, dex, token0, token1, feeTier)
	return err
}

func (w *DexDatabaseWriter) InsertPoolState(
	poolAddress, chain, dex string,
	blockNumber int64,
	txHash string,
	reserve0, reserve1, sqrtPriceX96, liquidity, tick *big.Int,
) error {
	query := `
        INSERT INTO dex_pool_state (
            time, pool_address, chain, dex, block_number, triggered_by_tx,
            reserve0, reserve1, sqrt_price_x96, liquidity, tick
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `

	// Helper to convert *big.Int to something driver handles (string for numeric)
	toNum := func(b *big.Int) interface{} {
		if b == nil {
			return nil
		}
		return b.String()
	}

	// Helper for tick (int)
	toInt := func(b *big.Int) interface{} {
		if b == nil {
			return nil
		}
		return b.Int64()
	}

	_, err := w.db.Exec(query,
		time.Now(),
		poolAddress,
		chain,
		dex,
		blockNumber,
		txHash,
		toNum(reserve0),
		toNum(reserve1),
		toNum(sqrtPriceX96),
		toNum(liquidity),
		toInt(tick),
	)
	return err
}

func (w *DexDatabaseWriter) Close() error {
	return w.db.Close()
}
