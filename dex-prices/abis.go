package main

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	// Uniswap V2/V3/V4 and all forks (PancakeSwap, SushiSwap, TraderJoe, QuickSwap, etc.)
	UniswapV2SwapTopic = crypto.Keccak256Hash([]byte("Swap(address,uint256,uint256,uint256,uint256,address)"))
	UniswapV3SwapTopic = crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)"))
	UniswapV4SwapTopic = crypto.Keccak256Hash([]byte("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)"))

	// Curve Finance - StableSwap pools
	CurveTokenExchangeTopic = crypto.Keccak256Hash([]byte("TokenExchange(address,int128,uint256,int128,uint256)"))

	// Balancer V2 - Weighted/Stable/MetaStable pools
	BalancerSwapTopic = crypto.Keccak256Hash([]byte("Swap(bytes32,address,address,uint256,uint256)"))
)

// Universal V2 Swap Event ABI (works for Uniswap, PancakeSwap, SushiSwap, etc.)
const uniswapV2SwapABIJSON = `[{
	"anonymous": false,
	"inputs": [
		{"indexed": true, "name": "sender", "type": "address"},
		{"indexed": false, "name": "amount0In", "type": "uint256"},
		{"indexed": false, "name": "amount1In", "type": "uint256"},
		{"indexed": false, "name": "amount0Out", "type": "uint256"},
		{"indexed": false, "name": "amount1Out", "type": "uint256"},
		{"indexed": true, "name": "to", "type": "address"}
	],
	"name": "Swap",
	"type": "event"
}]`

// Universal V3 Swap Event ABI (works for Uniswap V3, PancakeSwap V3, etc.)
const uniswapV3SwapABIJSON = `[{
	"anonymous": false,
	"inputs": [
		{"indexed": true, "name": "sender", "type": "address"},
		{"indexed": true, "name": "recipient", "type": "address"},
		{"indexed": false, "name": "amount0", "type": "int256"},
		{"indexed": false, "name": "amount1", "type": "int256"},
		{"indexed": false, "name": "sqrtPriceX96", "type": "uint160"},
		{"indexed": false, "name": "liquidity", "type": "uint128"},
		{"indexed": false, "name": "tick", "type": "int24"}
	],
	"name": "Swap",
	"type": "event"
}]`

// Uniswap V4 Swap Event ABI (PoolManager emits these)
const uniswapV4SwapABIJSON = `[{
	"anonymous": false,
	"inputs": [
		{"indexed": true, "name": "id", "type": "bytes32"},
		{"indexed": true, "name": "sender", "type": "address"},
		{"indexed": false, "name": "amount0", "type": "int128"},
		{"indexed": false, "name": "amount1", "type": "int128"},
		{"indexed": false, "name": "sqrtPriceX96", "type": "uint160"},
		{"indexed": false, "name": "liquidity", "type": "uint128"},
		{"indexed": false, "name": "tick", "type": "int24"},
		{"indexed": false, "name": "fee", "type": "uint24"}
	],
	"name": "Swap",
	"type": "event"
}]`

// Curve Finance TokenExchange Event ABI
const curveTokenExchangeABIJSON = `[{
	"anonymous": false,
	"inputs": [
		{"indexed": true, "name": "buyer", "type": "address"},
		{"indexed": false, "name": "sold_id", "type": "int128"},
		{"indexed": false, "name": "tokens_sold", "type": "uint256"},
		{"indexed": false, "name": "bought_id", "type": "int128"},
		{"indexed": false, "name": "tokens_bought", "type": "uint256"}
	],
	"name": "TokenExchange",
	"type": "event"
}]`

// Balancer V2 Swap Event ABI
const balancerSwapABIJSON = `[{
	"anonymous": false,
	"inputs": [
		{"indexed": true, "name": "poolId", "type": "bytes32"},
		{"indexed": true, "name": "tokenIn", "type": "address"},
		{"indexed": true, "name": "tokenOut", "type": "address"},
		{"indexed": false, "name": "amountIn", "type": "uint256"},
		{"indexed": false, "name": "amountOut", "type": "uint256"}
	],
	"name": "Swap",
	"type": "event"
}]`

// Universal V2 Pair Contract ABI (minimal, for token info)
const uniswapV2PairABIJSON = `[
	{
		"constant": true,
		"inputs": [],
		"name": "token0",
		"outputs": [{"name": "", "type": "address"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "token1",
		"outputs": [{"name": "", "type": "address"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "getReserves",
		"outputs": [
			{"name": "_reserve0", "type": "uint112"},
			{"name": "_reserve1", "type": "uint112"},
			{"name": "_blockTimestampLast", "type": "uint32"}
		],
		"type": "function"
	}
]`

// Universal V3 Pool Contract ABI (minimal, for token info)
const uniswapV3PoolABIJSON = `[
	{
		"constant": true,
		"inputs": [],
		"name": "token0",
		"outputs": [{"name": "", "type": "address"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "token1",
		"outputs": [{"name": "", "type": "address"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "fee",
		"outputs": [{"name": "", "type": "uint24"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "slot0",
		"outputs": [
			{"name": "sqrtPriceX96", "type": "uint160"},
			{"name": "tick", "type": "int24"},
			{"name": "observationIndex", "type": "uint16"},
			{"name": "observationCardinality", "type": "uint16"},
			{"name": "observationCardinalityNext", "type": "uint16"},
			{"name": "feeProtocol", "type": "uint8"},
			{"name": "unlocked", "type": "bool"}
		],
		"type": "function"
	}
]`

// ERC20 ABI for token metadata
const erc20ABIJSON = `[
	{
		"constant": true,
		"inputs": [],
		"name": "name",
		"outputs": [{"name": "", "type": "string"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "symbol",
		"outputs": [{"name": "", "type": "string"}],
		"type": "function"
	},
	{
		"constant": true,
		"inputs": [],
		"name": "decimals",
		"outputs": [{"name": "", "type": "uint8"}],
		"type": "function"
	}
]`

var (
	UniswapV2SwapEventABI abi.ABI
	UniswapV3SwapEventABI abi.ABI
	UniswapV4SwapEventABI abi.ABI
	UniswapV2PairABI      abi.ABI
	UniswapV3PoolABI      abi.ABI
	ERC20ABI              abi.ABI

	// Curve and Balancer ABIs
	CurveTokenExchangeEventABI abi.ABI
	BalancerSwapEventABI       abi.ABI
)

func init() {
	var err error

	UniswapV2SwapEventABI, err = abi.JSON(strings.NewReader(uniswapV2SwapABIJSON))
	if err != nil {
		panic(err)
	}

	UniswapV3SwapEventABI, err = abi.JSON(strings.NewReader(uniswapV3SwapABIJSON))
	if err != nil {
		panic(err)
	}

	UniswapV4SwapEventABI, err = abi.JSON(strings.NewReader(uniswapV4SwapABIJSON))
	if err != nil {
		panic(err)
	}

	UniswapV2PairABI, err = abi.JSON(strings.NewReader(uniswapV2PairABIJSON))
	if err != nil {
		panic(err)
	}

	UniswapV3PoolABI, err = abi.JSON(strings.NewReader(uniswapV3PoolABIJSON))
	if err != nil {
		panic(err)
	}

	ERC20ABI, err = abi.JSON(strings.NewReader(erc20ABIJSON))
	if err != nil {
		panic(err)
	}

	CurveTokenExchangeEventABI, err = abi.JSON(strings.NewReader(curveTokenExchangeABIJSON))
	if err != nil {
		panic(err)
	}

	BalancerSwapEventABI, err = abi.JSON(strings.NewReader(balancerSwapABIJSON))
	if err != nil {
		panic(err)
	}
}

// V2SwapEvent represents a Uniswap V2 (and forks) swap event
type V2SwapEvent struct {
	Sender     common.Address
	Amount0In  *big.Int
	Amount1In  *big.Int
	Amount0Out *big.Int
	Amount1Out *big.Int
	To         common.Address
}

// V3SwapEvent represents a Uniswap V3 (and forks) swap event
type V3SwapEvent struct {
	Sender       common.Address
	Recipient    common.Address
	Amount0      *big.Int
	Amount1      *big.Int
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         *big.Int
}

// V4SwapEvent represents a Uniswap V4 swap event
type V4SwapEvent struct {
	PoolId       [32]byte // bytes32 pool ID
	Sender       common.Address
	Amount0      *big.Int // int128
	Amount1      *big.Int // int128
	SqrtPriceX96 *big.Int
	Liquidity    *big.Int
	Tick         *big.Int
	Fee          *big.Int // uint24 comes as *big.Int
}

// CurveTokenExchangeEvent represents a Curve Finance TokenExchange event
type CurveTokenExchangeEvent struct {
	Buyer        common.Address
	SoldId       *big.Int // int128
	TokensSold   *big.Int
	BoughtId     *big.Int // int128
	TokensBought *big.Int
}

// BalancerSwapEvent represents a Balancer V2 Swap event
type BalancerSwapEvent struct {
	PoolId    [32]byte // bytes32
	TokenIn   common.Address
	TokenOut  common.Address
	AmountIn  *big.Int
	AmountOut *big.Int
}
