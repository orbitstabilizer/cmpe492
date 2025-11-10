package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type TokenInfo struct {
	Address  string `json:"address"`
	Symbol   string `json:"symbol"`
	Decimals uint8  `json:"decimals"`
}

type PairInfo struct {
	Address    string    `json:"address"`
	Version    string    `json:"version"`
	Token0     string    `json:"token0"`
	Token1     string    `json:"token1"`
	Token0Info TokenInfo `json:"token0_info"`
	Token1Info TokenInfo `json:"token1_info"`
	Fee        uint32    `json:"fee"`
}

type Cache struct {
	chainID    int64
	tokens     map[string]TokenInfo
	pairs      map[string]PairInfo
	tokenMutex sync.RWMutex
	pairMutex  sync.RWMutex
	cacheDir   string // Absolute path to cache directory
}

func NewCache(chainID int64) *Cache {
	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		log.Printf("⚠ Failed to get working directory: %v", err)
		cwd = "."
	}

	// Create absolute cache directory path
	cacheDir := filepath.Join(cwd, "cache")

	c := &Cache{
		chainID:  chainID,
		tokens:   make(map[string]TokenInfo),
		pairs:    make(map[string]PairInfo),
		cacheDir: cacheDir,
	}

	// Create cache directory if it doesn't exist
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		log.Printf("⚠ Failed to create cache directory: %v", err)
	} else {
		log.Printf("📁 Cache directory: %s", cacheDir)
	}

	c.load()
	return c
}

func (c *Cache) tokenCacheFile() string {
	return filepath.Join(c.cacheDir, fmt.Sprintf("token_cache_%d.json", c.chainID))
}

func (c *Cache) pairCacheFile() string {
	return filepath.Join(c.cacheDir, fmt.Sprintf("pair_cache_%d.json", c.chainID))
}

func (c *Cache) load() {
	// Load token cache
	if data, err := os.ReadFile(c.tokenCacheFile()); err == nil {
		if err := json.Unmarshal(data, &c.tokens); err != nil {
			log.Printf("⚠ Failed to unmarshal token cache: %v", err)
		}
	} else if !os.IsNotExist(err) {
		// Only log if it's not a "file doesn't exist" error
		log.Printf("⚠ Failed to read token cache file: %v", err)
	}

	// Load pair cache
	if data, err := os.ReadFile(c.pairCacheFile()); err == nil {
		if err := json.Unmarshal(data, &c.pairs); err != nil {
			log.Printf("⚠ Failed to unmarshal pair cache: %v", err)
		}
	} else if !os.IsNotExist(err) {
		// Only log if it's not a "file doesn't exist" error
		log.Printf("⚠ Failed to read pair cache file: %v", err)
	}

	log.Printf("✓ Loaded %d tokens and %d pairs from cache\n", len(c.tokens), len(c.pairs))
}

func (c *Cache) saveTokens() {
	c.tokenMutex.RLock()
	defer c.tokenMutex.RUnlock()

	data, err := json.MarshalIndent(c.tokens, "", "  ")
	if err != nil {
		log.Printf("⚠ Failed to marshal tokens cache: %v", err)
		return
	}
	if err := os.WriteFile(c.tokenCacheFile(), data, 0644); err != nil {
		log.Printf("⚠ Failed to write tokens cache file: %v", err)
	}
}

func (c *Cache) savePairs() {
	c.pairMutex.RLock()
	defer c.pairMutex.RUnlock()

	data, err := json.MarshalIndent(c.pairs, "", "  ")
	if err != nil {
		log.Printf("⚠ Failed to marshal pairs cache: %v", err)
		return
	}
	if err := os.WriteFile(c.pairCacheFile(), data, 0644); err != nil {
		log.Printf("⚠ Failed to write pairs cache file: %v", err)
	}
}

func (c *Cache) GetToken(address string) (TokenInfo, bool) {
	c.tokenMutex.RLock()
	defer c.tokenMutex.RUnlock()

	token, ok := c.tokens[address]
	return token, ok
}

func (c *Cache) SetToken(address string, token TokenInfo) {
	c.tokenMutex.Lock()
	c.tokens[address] = token
	c.tokenMutex.Unlock()

	go c.saveTokens()
}

func (c *Cache) GetPair(address string) (PairInfo, bool) {
	c.pairMutex.RLock()
	defer c.pairMutex.RUnlock()

	pair, ok := c.pairs[address]
	return pair, ok
}

func (c *Cache) SetPair(address string, pair PairInfo) {
	c.pairMutex.Lock()
	c.pairs[address] = pair
	c.pairMutex.Unlock()

	go c.savePairs()
}
