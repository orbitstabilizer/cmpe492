from dotenv import load_dotenv
import os
import json
import asyncio
import time
import picows
from web3 import Web3
from web3.exceptions import TransactionNotFound
from pathlib import Path
from typing import Optional, Dict, List
from eth_utils import to_checksum_address

load_dotenv()

ALCHEMY_WS_URL = os.getenv('ALCHEMY_WS_URL')
# Default HTTP URL: convert WebSocket URL to HTTP (wss:// -> https://, ws:// -> http://)
# You can also set ALCHEMY_HTTP_URL explicitly in .env
_default_http_url = ALCHEMY_WS_URL.replace('wss://', 'https://').replace('ws://', 'http://') if ALCHEMY_WS_URL else None
ALCHEMY_HTTP_URL = os.getenv('ALCHEMY_HTTP_URL', _default_http_url)

# Filter addresses (set to empty list to listen to all transactions)
# You can filter by specific addresses, or leave empty to monitor all Uniswap swaps
FILTER_ADDRESSES = os.getenv('FILTER_ADDRESSES', '').split(',')
FILTER_ADDRESSES = [addr.strip() for addr in FILTER_ADDRESSES if addr.strip()]

FILTER_POOLS = os.getenv('FILTER_POOLS', '').split(',')
FILTER_POOLS = [addr.strip().lower() for addr in FILTER_POOLS if addr.strip()]

def create_subscription_message():
    """Create subscription message with optional address filtering"""
    params = {
        "includeRemoved": True,
        "hashesOnly": True
    }
    
    if FILTER_ADDRESSES:
        params["addresses"] = [{"to": addr} for addr in FILTER_ADDRESSES]
    
    return {
        "jsonrpc": "2.0",
        "method": "eth_subscribe",
        "params": [
            "alchemy_minedTransactions",
            params
        ],
        "id": 1
    }

# Cache directories
CACHE_DIR = Path(__file__).parent
TOKEN_CACHE_FILE = CACHE_DIR / 'token_cache.json'
PAIR_CACHE_FILE = CACHE_DIR / 'pair_cache.json'

# Uniswap V2 Pair Swap event ABI
UNISWAP_V2_SWAP_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "sender", "type": "address"},
        {"indexed": False, "name": "amount0In", "type": "uint256"},
        {"indexed": False, "name": "amount1In", "type": "uint256"},
        {"indexed": False, "name": "amount0Out", "type": "uint256"},
        {"indexed": False, "name": "amount1Out", "type": "uint256"},
        {"indexed": True, "name": "to", "type": "address"}
    ],
    "name": "Swap",
    "type": "event"
}

# Uniswap V3 Pool Swap event ABI
UNISWAP_V3_SWAP_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "sender", "type": "address"},
        {"indexed": True, "name": "recipient", "type": "address"},
        {"indexed": False, "name": "amount0", "type": "int256"},
        {"indexed": False, "name": "amount1", "type": "int256"},
        {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
        {"indexed": False, "name": "liquidity", "type": "uint128"},
        {"indexed": False, "name": "tick", "type": "int24"}
    ],
    "name": "Swap",
    "type": "event"
}

# Uniswap V2 Pair ABI (minimal for token info)
UNISWAP_V2_PAIR_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "getReserves",
        "outputs": [
            {"name": "_reserve0", "type": "uint112"},
            {"name": "_reserve1", "type": "uint112"},
            {"name": "_blockTimestampLast", "type": "uint32"}
        ],
        "type": "function"
    },
    UNISWAP_V2_SWAP_ABI
]

# Uniswap V3 Pool ABI (minimal for token info)
UNISWAP_V3_POOL_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "fee",
        "outputs": [{"name": "", "type": "uint24"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"name": "", "type": "uint128"}],
        "type": "function"
    },
    {
        "constant": True,
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
    },
    UNISWAP_V3_SWAP_ABI
]

# ERC20 ABI for token metadata
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    }
]

# Event signatures - computed from event signature hash
# V2: Swap(address,uint256,uint256,uint256,uint256,address)
# V3: Swap(address,address,int256,int256,uint160,uint128,int24)
V2_SWAP_TOPIC = Web3.keccak(text="Swap(address,uint256,uint256,uint256,uint256,address)")
V3_SWAP_TOPIC = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)")


class SwapDecoder:
    """Decodes Uniswap swap events from transaction receipts"""
    
    def __init__(self, rpc_url: str):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to RPC at {rpc_url}")
        
        self.token_cache: Dict[str, Dict] = {}
        self.pair_cache: Dict[str, Dict] = {}
        
        self._load_caches()
        print(f"✓ Loaded {len(self.pair_cache)} pools and {len(self.token_cache)} tokens from cache")
    
    def _load_caches(self):
        """Load token and pair caches from disk"""
        if TOKEN_CACHE_FILE.exists():
            try:
                with open(TOKEN_CACHE_FILE, 'r') as f:
                    self.token_cache = json.load(f)
            except Exception as e:
                print(f"⚠ Could not load token cache: {e}")
        
        if PAIR_CACHE_FILE.exists():
            try:
                with open(PAIR_CACHE_FILE, 'r') as f:
                    self.pair_cache = json.load(f)
            except Exception as e:
                print(f"⚠ Could not load pair cache: {e}")
    
    def _save_token_cache(self):
        """Save token cache to disk"""
        try:
            with open(TOKEN_CACHE_FILE, 'w') as f:
                json.dump(self.token_cache, f, indent=2)
        except Exception as e:
            print(f"⚠ Could not save token cache: {e}")
    
    def _save_pair_cache(self):
        """Save pair cache to disk"""
        try:
            with open(PAIR_CACHE_FILE, 'w') as f:
                json.dump(self.pair_cache, f, indent=2)
        except Exception as e:
            print(f"⚠ Could not save pair cache: {e}")
    
    def get_token_info(self, token_address: str) -> Dict:
        """Get token metadata with caching"""
        if token_address == "0x0000000000000000000000000000000000000000":
            return {
                "address": token_address,
                "symbol": "ETH",
                "name": "Ethereum",
                "decimals": 18
            }
        
        token_address = to_checksum_address(token_address)
        if token_address in self.token_cache:
            return self.token_cache[token_address]
        
        try:
            token_contract = self.w3.eth.contract(
                address=token_address,
                abi=ERC20_ABI
            )
            symbol = token_contract.functions.symbol().call()
            name = token_contract.functions.name().call()
            decimals = token_contract.functions.decimals().call()
            
            token_info = {
                "address": token_address,
                "symbol": symbol,
                "name": name,
                "decimals": decimals
            }
            
            self.token_cache[token_address] = token_info
            self._save_token_cache()
            return token_info
        except Exception as e:
            return {
                "address": token_address,
                "symbol": "UNKNOWN",
                "name": "Unknown Token",
                "decimals": 18
            }
    
    def get_v2_pair_info(self, pair_address: str) -> Optional[Dict]:
        """Get V2 pair information (static data only - reserves are not cached)"""
        pair_address = to_checksum_address(pair_address)
        if pair_address in self.pair_cache:
            return self.pair_cache[pair_address]
        
        try:
            pair_contract = self.w3.eth.contract(
                address=pair_address,
                abi=UNISWAP_V2_PAIR_ABI
            )
            token0_address = pair_contract.functions.token0().call()
            token1_address = pair_contract.functions.token1().call()
            
            token0_info = self.get_token_info(token0_address)
            token1_info = self.get_token_info(token1_address)
            
            pair_info = {
                "address": pair_address,
                "version": "V2",
                "token0": token0_address,
                "token1": token1_address,
                "token0_info": token0_info,
                "token1_info": token1_info
            }
            
            self.pair_cache[pair_address] = pair_info
            self._save_pair_cache()
            return pair_info
        except Exception as e:
            print(f"⚠ Error fetching V2 pair info for {pair_address}: {e}")
            return None
    
    def get_v3_pool_info(self, pool_address: str) -> Optional[Dict]:
        """Get V3 pool information (static data only - dynamic state is not cached)"""
        pool_address = to_checksum_address(pool_address)
        cache_key = f"v3_{pool_address}"
        if cache_key in self.pair_cache:
            return self.pair_cache[cache_key]
        
        try:
            pool_contract = self.w3.eth.contract(
                address=pool_address,
                abi=UNISWAP_V3_POOL_ABI
            )
            token0_address = pool_contract.functions.token0().call()
            token1_address = pool_contract.functions.token1().call()
            fee = pool_contract.functions.fee().call()
            
            token0_info = self.get_token_info(token0_address)
            token1_info = self.get_token_info(token1_address)
            
            pool_info = {
                "address": pool_address,
                "version": "V3",
                "token0": token0_address,
                "token1": token1_address,
                "token0_info": token0_info,
                "token1_info": token1_info,
                "fee": fee,
                "fee_percent": fee / 10000
            }
            
            self.pair_cache[cache_key] = pool_info
            self._save_pair_cache()
            return pool_info
        except Exception as e:
            print(f"⚠ Error fetching V3 pool info for {pool_address}: {e}")
            return None
    
    def format_amount(self, amount: int, decimals: int) -> str:
        """Format token amount with decimals"""
        amount_float = abs(amount) / (10 ** decimals)
        return f"{amount_float:,.6f}".rstrip('0').rstrip('.')
    
    def decode_v2_swap(self, log: Dict, tx_receipt: Dict) -> Optional[Dict]:
        """Decode V2 swap event"""
        try:
            pair_address = to_checksum_address(log['address'])
            pair_info = self.get_v2_pair_info(pair_address)
            if not pair_info:
                return None
            
            # Decode the event
            pair_contract = self.w3.eth.contract(
                address=pair_address,
                abi=UNISWAP_V2_PAIR_ABI
            )
            event = pair_contract.events.Swap().process_log(log)
            args = event['args']
            
            # Fetch fresh reserves at the transaction block (they change with every swap)
            block_number = tx_receipt['blockNumber']
            reserves = pair_contract.functions.getReserves().call(block_identifier=block_number)
            reserve0 = reserves[0]
            reserve1 = reserves[1]
            
            # Determine swap direction
            amount0_in = args['amount0In']
            amount1_in = args['amount1In']
            amount0_out = args['amount0Out']
            amount1_out = args['amount1Out']
            
            # Calculate net amounts
            if amount0_in > 0:
                # Token0 in, Token1 out
                amount_in = amount0_in
                amount_out = amount1_out
                token_in = pair_info['token0_info']
                token_out = pair_info['token1_info']
                direction = "token0 → token1"
            else:
                # Token1 in, Token0 out
                amount_in = amount1_in
                amount_out = amount0_out
                token_in = pair_info['token1_info']
                token_out = pair_info['token0_info']
                direction = "token1 → token0"
            
            # Calculate price from fresh reserves
            if reserve0 > 0 and reserve1 > 0:
                price = (reserve1 / (10 ** pair_info['token1_info']['decimals'])) / (reserve0 / (10 ** pair_info['token0_info']['decimals']))
            else:
                price = 0
            
            return {
                "version": "V2",
                "pair_address": pair_address,
                "pair_info": pair_info,
                "tx_hash": tx_receipt['transactionHash'].hex(),
                "block_number": tx_receipt['blockNumber'],
                "sender": args['sender'],
                "to": args['to'],
                "amount_in": amount_in,
                "amount_out": amount_out,
                "token_in": token_in,
                "token_out": token_out,
                "direction": direction,
                "reserve0": reserve0,
                "reserve1": reserve1,
                "price": price
            }
        except Exception as e:
            print(f"⚠ Error decoding V2 swap: {e}")
            return None
    
    def decode_v3_swap(self, log: Dict, tx_receipt: Dict) -> Optional[Dict]:
        """Decode V3 swap event"""
        try:
            pool_address = to_checksum_address(log['address'])
            pool_info = self.get_v3_pool_info(pool_address)
            if not pool_info:
                return None
            
            # Decode the event
            pool_contract = self.w3.eth.contract(
                address=pool_address,
                abi=UNISWAP_V3_POOL_ABI
            )
            event = pool_contract.events.Swap().process_log(log)
            args = event['args']
            
            amount0 = args['amount0']
            amount1 = args['amount1']
            
            # Determine swap direction
            if amount0 < 0:
                # Token0 in, Token1 out
                amount_in = abs(amount0)
                amount_out = amount1
                token_in = pool_info['token0_info']
                token_out = pool_info['token1_info']
                direction = "token0 → token1"
            else:
                # Token1 in, Token0 out
                amount_in = abs(amount1)
                amount_out = amount0
                token_in = pool_info['token1_info']
                token_out = pool_info['token0_info']
                direction = "token1 → token0"
            
            # Calculate price from sqrtPriceX96, adjusted for token decimals
            # sqrtPriceX96 = sqrt(amount1/amount0) * 2^96 where amounts are in raw token units
            # price_raw = (sqrtPriceX96 / 2^96)^2 = amount1/amount0 (in raw units)
            # price_adjusted = price_raw * 10^(token0_decimals - token1_decimals)
            sqrt_price = args['sqrtPriceX96']
            price_raw = (sqrt_price / (2**96)) ** 2
            token0_decimals = pool_info['token0_info']['decimals']
            token1_decimals = pool_info['token1_info']['decimals']
            price = price_raw * (10 ** (token0_decimals - token1_decimals))
            
            return {
                "version": "V3",
                "pool_address": pool_address,
                "pool_info": pool_info,
                "tx_hash": tx_receipt['transactionHash'].hex(),
                "block_number": tx_receipt['blockNumber'],
                "sender": args['sender'],
                "recipient": args['recipient'],
                "amount_in": amount_in,
                "amount_out": amount_out,
                "token_in": token_in,
                "token_out": token_out,
                "direction": direction,
                "sqrtPriceX96": sqrt_price,
                "price": price,
                "liquidity": args['liquidity'],
                "tick": args['tick']
            }
        except Exception as e:
            print(f"⚠ Error decoding V3 swap: {e}")
            return None
    
    def process_transaction(self, tx_hash: str, max_retries: int = 3) -> List[Dict]:
        """Process a transaction and return all swap events"""
        for attempt in range(max_retries):
            try:
                tx_receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                swaps = []
                
                for log in tx_receipt['logs']:
                    # Check if it's a swap event
                    if len(log['topics']) > 0:
                        topic = log['topics'][0]
                        
                        # Compare topics (both are HexBytes)
                        if topic == V2_SWAP_TOPIC:
                            swap = self.decode_v2_swap(log, tx_receipt)
                            if swap:
                                swaps.append(swap)
                        elif topic == V3_SWAP_TOPIC:
                            swap = self.decode_v3_swap(log, tx_receipt)
                            if swap:
                                swaps.append(swap)
                
                return swaps
            except TransactionNotFound:
                if attempt < max_retries - 1:
                    # Wait a bit and retry (transaction might not be indexed yet)
                    time.sleep(0.5)
                    continue
                print(f"⚠ Transaction not found after {max_retries} attempts: {tx_hash}")
                return []
            except Exception as e:
                print(f"⚠ Error processing transaction {tx_hash}: {e}")
                return []
        
        return []
    
    def format_swap(self, swap: Dict) -> str:
        """Format a swap for display"""
        if swap['version'] == "V2":
            pair = swap['pair_info']
            pair_name = f"{pair['token0_info']['symbol']}/{pair['token1_info']['symbol']}"
            
            amount_in_formatted = self.format_amount(swap['amount_in'], swap['token_in']['decimals'])
            amount_out_formatted = self.format_amount(swap['amount_out'], swap['token_out']['decimals'])
            
            reserve0_formatted = self.format_amount(swap['reserve0'], pair['token0_info']['decimals'])
            reserve1_formatted = self.format_amount(swap['reserve1'], pair['token1_info']['decimals'])
            
            output = f"""
{'='*80}
Block: {swap['block_number']} | Tx: 0x{swap['tx_hash']}
Pair: {pair_name} (V2)
Address: {swap['pair_address']}
Sender: {swap['sender']}
To: {swap['to']}

SWAP: {amount_in_formatted} {swap['token_in']['symbol']} → {amount_out_formatted} {swap['token_out']['symbol']}

Reserves:
  {pair['token0_info']['symbol']}: {reserve0_formatted}
  {pair['token1_info']['symbol']}: {reserve1_formatted}
  Price: {swap['price']:.6f} {pair['token1_info']['symbol']}/{pair['token0_info']['symbol']}
{'='*80}
"""
        else:  # V3
            pool = swap['pool_info']
            pool_name = f"{pool['token0_info']['symbol']}/{pool['token1_info']['symbol']}"
            
            amount_in_formatted = self.format_amount(swap['amount_in'], swap['token_in']['decimals'])
            amount_out_formatted = self.format_amount(swap['amount_out'], swap['token_out']['decimals'])
            
            liquidity_formatted = f"{swap['liquidity']:,}"
            
            output = f"""
{'='*80}
Block: {swap['block_number']} | Tx: 0x{swap['tx_hash']}
Pool: {pool_name} (V3) Fee: {pool['fee_percent']:.2f}%
Address: {swap['pool_address']}
Sender: {swap['sender']}
Recipient: {swap['recipient']}

SWAP: {amount_in_formatted} {swap['token_in']['symbol']} → {amount_out_formatted} {swap['token_out']['symbol']}

Pool State:
  Sqrt Price X96: {swap['sqrtPriceX96']}
  Price: {swap['price']:.18f} {pool['token1_info']['symbol']}/{pool['token0_info']['symbol']}
  Liquidity: {liquidity_formatted}
  Tick: {swap['tick']}
{'='*80}
"""
        
        return output


class AlchemyListener(picows.WSListener):
    def __init__(self, decoder: SwapDecoder):
        self.decoder = decoder
        super().__init__()
    
    def on_ws_frame(self, transport: picows.WSTransport, frame: picows.WSFrame) -> None:
        try:
            message = json.loads(frame.get_payload_as_utf8_text())
            
            # Check if it's a subscription notification
            if message.get('method') == 'eth_subscription' and 'params' in message:
                params = message['params']
                if 'result' in params:
                    result = params['result']
                    
                    # Check if it's a transaction hash notification
                    if 'hash' in result:
                        tx_hash = result['hash']
                        # Process the transaction
                        swaps = self.decoder.process_transaction(tx_hash)
                        
                        if swaps:
                            for swap in swaps:
                                # Apply pool filter if specified
                                if FILTER_POOLS:
                                    pool_addr = swap.get('pool_address', '').lower()
                                    pair_addr = swap.get('pair_address', '').lower()
                                    if pool_addr not in FILTER_POOLS and pair_addr not in FILTER_POOLS:
                                        continue
                                print(self.decoder.format_swap(swap))

        except json.JSONDecodeError:
            print(f"⚠ Invalid JSON received: {frame.get_payload_as_utf8_text()[:100]}")
        except Exception as e:
            print(f"⚠ Error processing message: {e}")


async def main():
    print("="*80)
    print("Uniswap Swap Event Listener - Ethereum Mainnet")
    print("="*80)
    print()
    
    # Initialize decoder
    try:
        decoder = SwapDecoder(ALCHEMY_HTTP_URL)
    except Exception as e:
        print(f"❌ Error initializing decoder: {e}")
        return
    
    print(f"✓ Connected to Ethereum (Chain ID: {decoder.w3.eth.chain_id})")
    print(f"✓ Current block: {decoder.w3.eth.block_number}")
    print()
    print("Listening for transactions...")
    print("Press Ctrl+C to stop\n")
    
    # Connect to WebSocket
    try:
        sub_msg = create_subscription_message()
        if FILTER_ADDRESSES:
            print(f"✓ Filtering transactions to addresses: {', '.join(FILTER_ADDRESSES)}")
        else:
            print("✓ Listening to all transactions (no address filter)")
        print()
        
        transport, listener = await picows.ws_connect(
            lambda: AlchemyListener(decoder),
            ALCHEMY_WS_URL
        )
        transport.send(picows.WSMsgType.TEXT, json.dumps(sub_msg).encode("utf-8"))
        await asyncio.sleep(float("inf"))
    except KeyboardInterrupt:
        print("\n\nStopped listening.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())