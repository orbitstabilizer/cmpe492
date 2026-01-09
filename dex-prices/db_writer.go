package main

import (
	"database/sql"
	"log"
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

	// log.Printf("Inserted swap: %s on %s", swap.TxHash, swap.Chain)
	return nil
}

func (w *DexDatabaseWriter) UpsertToken(address, symbol, name string, decimals int, chain string) error {
	query := `
        INSERT INTO tokens (address, symbol, name, decimals, chain)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (address) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            decimals = EXCLUDED.decimals
    `
	_, err := w.db.Exec(query, address, symbol, name, decimals, chain)
	return err
}

func (w *DexDatabaseWriter) UpsertPool(poolAddress, chain, dex, token0, token1 string, feeTier int) error {
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

func (w *DexDatabaseWriter) Close() error {
	return w.db.Close()
}
