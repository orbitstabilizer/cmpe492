package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

type ChainConfig struct {
	ChainID         int64     `json:"chainId"`
	Name            string    `json:"name"`
	ChainType       string    `json:"chainType"` // "evm" or "solana"
	RPCWS           string    `json:"rpcWs"`
	RPCHTTP         string    `json:"rpcHttp"`
	NativeToken     TokenInfo `json:"nativeToken"`
	Enabled         bool      `json:"enabled"`
	FilterAddresses []string  `json:"filterAddresses,omitempty"` // Optional: filter by specific contract addresses
}

type Config struct {
	Chains []ChainConfig `json:"chains"`
}

// LoadConfig loads configuration from config.json
func LoadConfig() (*Config, error) {
	file, err := os.ReadFile("config.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read config.json: %w", err)
	}

	var config Config
	if err := json.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config.json: %w", err)
	}

	// Normalize addresses to lowercase
	for i := range config.Chains {
		for j := range config.Chains[i].FilterAddresses {
			config.Chains[i].FilterAddresses[j] = strings.ToLower(config.Chains[i].FilterAddresses[j])
		}
	}

	return &config, nil
}

// GetEnabledChains returns all chains that are enabled
func GetEnabledChains(config *Config) []ChainConfig {
	var enabled []ChainConfig
	for _, chain := range config.Chains {
		if chain.Enabled {
			enabled = append(enabled, chain)
		}
	}
	return enabled
}

// HasAddressFilter checks if a chain has address filtering enabled
func (c *ChainConfig) HasAddressFilter() bool {
	return len(c.FilterAddresses) > 0
}

// CreateDefaultConfig creates a default config.json file with example configuration
func CreateDefaultConfig() error {
	defaultConfig := Config{
		Chains: []ChainConfig{
			{
				ChainID:   56,
				Name:      "BSC",
				ChainType: "evm",
				RPCWS:     "wss://bsc-rpc.publicnode.com",
				RPCHTTP:   "https://bsc-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
					Symbol:   "WBNB",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
			{
				ChainID:   1,
				Name:      "Ethereum",
				ChainType: "evm",
				RPCWS:     "wss://ethereum-rpc.publicnode.com",
				RPCHTTP:   "https://ethereum-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
					Symbol:   "WETH",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
			{
				ChainID:   137,
				Name:      "Polygon",
				ChainType: "evm",
				RPCWS:     "wss://polygon-bor-rpc.publicnode.com",
				RPCHTTP:   "https://polygon-bor-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
					Symbol:   "WMATIC",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
			{
				ChainID:   42161,
				Name:      "Arbitrum",
				ChainType: "evm",
				RPCWS:     "wss://arbitrum-one-rpc.publicnode.com",
				RPCHTTP:   "https://arbitrum-one-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
					Symbol:   "WETH",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
			{
				ChainID:   10,
				Name:      "Optimism",
				ChainType: "evm",
				RPCWS:     "wss://optimism-rpc.publicnode.com",
				RPCHTTP:   "https://optimism-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0x4200000000000000000000000000000000000006",
					Symbol:   "WETH",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
			{
				ChainID:   8453,
				Name:      "Base",
				ChainType: "evm",
				RPCWS:     "wss://base-rpc.publicnode.com",
				RPCHTTP:   "https://base-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0x4200000000000000000000000000000000000006",
					Symbol:   "WETH",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
			{
				ChainID:   43114,
				Name:      "Avalanche",
				ChainType: "evm",
				RPCWS:     "wss://avalanche-c-chain-rpc.publicnode.com",
				RPCHTTP:   "https://avalanche-c-chain-rpc.publicnode.com",
				NativeToken: TokenInfo{
					Address:  "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
					Symbol:   "WAVAX",
					Decimals: 18,
				},
				Enabled:         true,
				FilterAddresses: []string{},
			},
		},
	}

	data, err := json.MarshalIndent(defaultConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal default config: %w", err)
	}

	if err := os.WriteFile("config.json", data, 0644); err != nil {
		return fmt.Errorf("failed to write config.json: %w", err)
	}

	log.Println("Created default config.json with 7 EVM chains")
	return nil
}
