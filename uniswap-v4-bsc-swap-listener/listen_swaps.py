#!/usr/bin/env python3
"""
Uniswap V4 Swap Event Listener for BSC Mainnet

This script listens to Swap events from the Uniswap V4 PoolManager contract
on BNB Smart Chain and displays swap details in real-time.
"""

import os
import sys
import json
import time
from pathlib import Path
from web3 import Web3
from web3.exceptions import BlockNotFound
from dotenv import load_dotenv
from typing import Optional, Dict, Tuple
from functools import lru_cache

# Load environment variables
load_dotenv()

# BSC Mainnet Configuration
BSC_RPC_URL = os.getenv('BSC_RPC_URL', 'https://rpc.ankr.com/bsc')
POOL_MANAGER_ADDRESS = '0x28e2ea090877bf75740558f6bfb36a5ffee9e9df'

# Cache file path
CACHE_DIR = Path(__file__).parent
POOL_CACHE_FILE = CACHE_DIR / 'pool_cache.json'
TOKEN_CACHE_FILE = CACHE_DIR / 'token_cache.json'

# Swap event signature from IPoolManager.sol
SWAP_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "id", "type": "bytes32"},
        {"indexed": True, "name": "sender", "type": "address"},
        {"indexed": False, "name": "amount0", "type": "int128"},
        {"indexed": False, "name": "amount1", "type": "int128"},
        {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
        {"indexed": False, "name": "liquidity", "type": "uint128"},
        {"indexed": False, "name": "tick", "type": "int24"},
        {"indexed": False, "name": "fee", "type": "uint24"}
    ],
    "name": "Swap",
    "type": "event"
}

# Initialize event ABI for PoolManager
INITIALIZE_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "id", "type": "bytes32"},
        {"indexed": True, "name": "currency0", "type": "address"},
        {"indexed": True, "name": "currency1", "type": "address"},
        {"indexed": False, "name": "fee", "type": "uint24"},
        {"indexed": False, "name": "tickSpacing", "type": "int24"},
        {"indexed": False, "name": "hooks", "type": "address"},
        {"indexed": False, "name": "sqrtPriceX96", "type": "uint160"},
        {"indexed": False, "name": "tick", "type": "int24"}
    ],
    "name": "Initialize",
    "type": "event"
}

# Minimal ABI for the PoolManager
POOL_MANAGER_ABI = [SWAP_EVENT_ABI, INITIALIZE_EVENT_ABI]

# Standard ERC20 ABI for token metadata
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


class SwapEventListener:
    """Listener for Uniswap V4 Swap events on BSC"""

    def __init__(self, rpc_url: str, pool_manager_address: str):
        """
        Initialize the swap event listener

        Args:
            rpc_url: BSC RPC endpoint URL
            pool_manager_address: Address of the PoolManager contract
        """
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))

        # Check connection
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to BSC RPC at {rpc_url}")

        print(f"✓ Connected to BSC (Chain ID: {self.w3.eth.chain_id})")
        print(f"✓ Current block: {self.w3.eth.block_number}")

        # Setup contract
        self.pool_manager_address = Web3.to_checksum_address(pool_manager_address)
        self.contract = self.w3.eth.contract(
            address=self.pool_manager_address,
            abi=POOL_MANAGER_ABI
        )

        # Cache for token metadata and pool information
        self.token_cache: Dict[str, Dict] = {}
        self.pool_cache: Dict[str, Dict] = {}

        # Load caches from disk
        self._load_caches()

        print(f"✓ Monitoring PoolManager at: {self.pool_manager_address}")
        print(f"✓ Loaded {len(self.pool_cache)} pools and {len(self.token_cache)} tokens from cache\n")

    def _load_caches(self):
        """Load token and pool caches from disk"""
        # Load token cache
        if TOKEN_CACHE_FILE.exists():
            try:
                with open(TOKEN_CACHE_FILE, 'r') as f:
                    self.token_cache = json.load(f)
            except Exception as e:
                print(f"⚠ Could not load token cache: {e}")
                self.token_cache = {}

        # Load pool cache
        if POOL_CACHE_FILE.exists():
            try:
                with open(POOL_CACHE_FILE, 'r') as f:
                    self.pool_cache = json.load(f)
            except Exception as e:
                print(f"⚠ Could not load pool cache: {e}")
                self.pool_cache = {}

    def _save_token_cache(self):
        """Save token cache to disk"""
        try:
            with open(TOKEN_CACHE_FILE, 'w') as f:
                json.dump(self.token_cache, f, indent=2)
        except Exception as e:
            print(f"⚠ Could not save token cache: {e}")

    def _save_pool_cache(self):
        """Save pool cache to disk"""
        try:
            with open(POOL_CACHE_FILE, 'w') as f:
                json.dump(self.pool_cache, f, indent=2)
        except Exception as e:
            print(f"⚠ Could not save pool cache: {e}")

    def get_token_info(self, token_address: str) -> Dict:
        """
        Get token metadata (symbol, name, decimals) with caching

        Args:
            token_address: The token contract address

        Returns:
            Dict with token info (symbol, name, decimals)
        """
        # Check if native currency (address(0))
        if token_address == "0x0000000000000000000000000000000000000000":
            return {
                "address": token_address,
                "symbol": "BNB",
                "name": "Binance Coin",
                "decimals": 18
            }

        # Check cache
        if token_address in self.token_cache:
            return self.token_cache[token_address]

        try:
            # Create token contract
            token_address_checksum = Web3.to_checksum_address(token_address)
            token_contract = self.w3.eth.contract(
                address=token_address_checksum,
                abi=ERC20_ABI
            )

            # Fetch token info
            symbol = token_contract.functions.symbol().call()
            name = token_contract.functions.name().call()
            decimals = token_contract.functions.decimals().call()

            token_info = {
                "address": token_address,
                "symbol": symbol,
                "name": name,
                "decimals": decimals
            }

            # Cache it
            self.token_cache[token_address] = token_info
            self._save_token_cache()
            return token_info

        except Exception as e:
            # Return unknown token info
            return {
                "address": token_address,
                "symbol": f"UNKNOWN",
                "name": "Unknown Token",
                "decimals": 18
            }

    def get_pool_info(self, pool_id: str) -> Optional[Dict]:
        """
        Get pool information by looking up Initialize event

        Args:
            pool_id: The pool ID (bytes32 as hex string)

        Returns:
            Dict with pool info (currency0, currency1, fee, etc.) or None
        """
        # Check cache
        if pool_id in self.pool_cache:
            return self.pool_cache[pool_id]

        print(f"🔍 Fetching pool info for {pool_id[:16]}... (first time, may take a few seconds)")

        try:
            # Search for Initialize event with this pool ID
            # Start from a reasonable block (adjust based on when V4 was deployed)
            from_block = 43000000  # Approximate V4 deployment on BSC
            to_block = self.w3.eth.block_number

            # Search in chunks to avoid timeout
            chunk_size = 50000

            for start_block in range(from_block, to_block + 1, chunk_size):
                end_block = min(start_block + chunk_size - 1, to_block)

                try:
                    event_filter = self.contract.events.Initialize.create_filter(
                        from_block=start_block,
                        to_block=end_block,
                        argument_filters={'id': bytes.fromhex(pool_id)}
                    )

                    events = event_filter.get_all_entries()

                    if events:
                        event = events[0]  # Take first match
                        print(f"✓ Found pool initialization at block {event['blockNumber']}")

                        pool_info = {
                            "pool_id": pool_id,
                            "currency0": event['args']['currency0'],
                            "currency1": event['args']['currency1'],
                            "fee": event['args']['fee'],
                            "tickSpacing": event['args']['tickSpacing'],
                            "hooks": event['args']['hooks'],
                            "token0_info": self.get_token_info(event['args']['currency0']),
                            "token1_info": self.get_token_info(event['args']['currency1'])
                        }

                        # Cache it
                        self.pool_cache[pool_id] = pool_info
                        print(f"✓ Pool: {pool_info['token0_info']['symbol']}/{pool_info['token1_info']['symbol']}\n")
                        return pool_info

                except Exception as e:
                    # Continue to next chunk
                    print(f"  Searching blocks {start_block}-{end_block}...")
                    continue

            print(f"⚠ Could not find Initialize event for pool {pool_id[:16]}...")
            return None

        except Exception as e:
            print(f"⚠ Error fetching pool info for {pool_id}: {e}")
            return None

    def format_amount(self, amount: int, decimals: int) -> str:
        """
        Format token amount with decimals

        Args:
            amount: Raw token amount
            decimals: Token decimals

        Returns:
            Formatted string with proper decimals
        """
        amount_float = abs(amount) / (10 ** decimals)
        return f"{amount_float:,.6f}".rstrip('0').rstrip('.')

    def format_swap_event(self, event: dict) -> str:
        """
        Format a swap event for display

        Args:
            event: The event dictionary from Web3

        Returns:
            Formatted string representation of the swap
        """
        args = event['args']

        # Extract values
        pool_id = args['id'].hex()
        sender = args['sender']
        amount0 = args['amount0']
        amount1 = args['amount1']
        sqrt_price = args['sqrtPriceX96']
        liquidity = args['liquidity']
        tick = args['tick']
        fee = args['fee']

        # Calculate approximate price from sqrtPriceX96
        price_float = (sqrt_price / (2**96)) ** 2

        # Determine swap direction
        if amount0 < 0:
            direction = "token0 → token1"
        else:
            direction = "token1 → token0"

        output = f"""
{'='*80}
Block: {event['blockNumber']} | Tx: {event['transactionHash'].hex()}
Pool ID: {pool_id}
Sender: {sender}
Direction: {direction}
Amount0: {amount0:,} | Amount1: {amount1:,}
Price (approx): {price_float:.18f}
Liquidity: {liquidity:,}
Tick: {tick}
Fee: {fee} bips ({fee/10000:.2f}%)
{'='*80}
"""

        return output

    def listen_historical(self, from_block: int, to_block: Optional[int] = None):
        """
        Fetch historical swap events

        Args:
            from_block: Starting block number
            to_block: Ending block number (None for latest)
        """
        if to_block is None:
            to_block = self.w3.eth.block_number

        print(f"Fetching historical Swap events from block {from_block} to {to_block}...\n")

        # Create event filter for historical events
        event_filter = self.contract.events.Swap.create_filter(
            from_block=from_block,
            to_block=to_block
        )

        # Get all events
        events = event_filter.get_all_entries()

        print(f"Found {len(events)} swap events\n")

        for event in events:
            print(self.format_swap_event(event))

    def listen_realtime(self, poll_interval: int = 3):
        """
        Listen to swap events in real-time

        Args:
            poll_interval: Seconds between polls for new events (default: 3)
        """
        print(f"Starting real-time monitoring (polling every {poll_interval}s)")
        print("Press Ctrl+C to stop\n")

        last_processed_block = self.w3.eth.block_number - 1
        consecutive_errors = 0
        max_errors = 3

        try:
            while True:
                try:
                    # Get current block
                    current_block = self.w3.eth.block_number

                    # Check for new blocks
                    if current_block > last_processed_block:
                        # Fetch events from the last processed block to current
                        from_block = last_processed_block + 1
                        to_block = current_block

                        # Get swap events in this range
                        events = self.contract.events.Swap.get_logs(
                            from_block=from_block,
                            to_block=to_block
                        )

                        # Process and display events
                        for event in events:
                            print(self.format_swap_event(event))

                        # Update last processed block
                        last_processed_block = current_block

                        # Reset error counter on success
                        consecutive_errors = 0

                except Exception as e:
                    consecutive_errors += 1
                    error_msg = str(e)

                    # Check if it's a rate limit error
                    if 'limit exceeded' in error_msg.lower() or 'rate limit' in error_msg.lower():
                        wait_time = poll_interval * (2 ** consecutive_errors)  # Exponential backoff
                        wait_time = min(wait_time, 30)  # Cap at 30 seconds
                        print(f"\n⚠ Rate limit exceeded. Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)

                        if consecutive_errors >= max_errors:
                            print("\n⚠ Too many rate limit errors. Consider:")
                            print("   1. Using a different RPC endpoint (edit .env)")
                            print("   2. Getting a dedicated RPC provider (NodeReal, Ankr, etc.)")
                            print("   3. Increasing poll_interval")
                            consecutive_errors = 0  # Reset to continue trying
                        continue
                    else:
                        print(f"\n⚠ Error: {e}")
                        print(f"Retrying in {poll_interval} seconds...")
                        time.sleep(poll_interval)
                        continue

                # Wait before checking again
                time.sleep(poll_interval)

        except KeyboardInterrupt:
            print("\n\nStopped monitoring.")

    def listen_specific_pool(self, pool_id: str, from_block: int, realtime: bool = False):
        """
        Listen to swaps for a specific pool

        Args:
            pool_id: The pool ID (bytes32 as hex string)
            from_block: Starting block number
            realtime: Continue listening in real-time after historical events
        """
        pool_id_bytes = bytes.fromhex(pool_id.replace('0x', ''))

        print(f"Monitoring swaps for Pool ID: {pool_id}\n")

        # Create event filter with pool ID filter
        event_filter = self.contract.events.Swap.create_filter(
            from_block=from_block,
            argument_filters={'id': pool_id_bytes}
        )

        # Get historical events
        events = event_filter.get_all_entries()
        print(f"Found {len(events)} historical swap events\n")

        for event in events:
            print(self.format_swap_event(event))

        if realtime:
            print("\nSwitching to real-time monitoring...")
            print("Press Ctrl+C to stop\n")

            last_processed_block = self.w3.eth.block_number - 1

            try:
                while True:
                    try:
                        # Get current block
                        current_block = self.w3.eth.block_number

                        # Check for new blocks
                        if current_block > last_processed_block:
                            # Fetch events from the last processed block to current
                            from_block = last_processed_block + 1
                            to_block = current_block

                            # Get swap events for this pool in this range
                            events = self.contract.events.Swap.get_logs(
                                from_block=from_block,
                                to_block=to_block,
                                argument_filters={'id': pool_id_bytes}
                            )

                            # Process and display events
                            for event in events:
                                print(self.format_swap_event(event))

                            # Update last processed block
                            last_processed_block = current_block

                    except Exception as e:
                        print(f"\n⚠ Error: {e}")
                        print(f"Retrying in 2 seconds...")
                        time.sleep(2)
                        continue

                    time.sleep(2)

            except KeyboardInterrupt:
                print("\n\nStopped monitoring.")


def main():
    """Main entry point"""

    print("=" * 80)
    print("Uniswap V4 Swap Event Listener - BSC Mainnet")
    print("=" * 80)
    print()

    # Initialize listener
    try:
        listener = SwapEventListener(BSC_RPC_URL, POOL_MANAGER_ADDRESS)
    except Exception as e:
        print(f"Error initializing listener: {e}")
        sys.exit(1)

    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "historical":
            # Fetch historical events
            from_block = int(sys.argv[2]) if len(sys.argv) > 2 else listener.w3.eth.block_number - 1000
            to_block = int(sys.argv[3]) if len(sys.argv) > 3 else None
            listener.listen_historical(from_block, to_block)

        elif command == "pool":
            # Listen to specific pool
            if len(sys.argv) < 3:
                print("Usage: python listen_swaps.py pool <pool_id> [from_block] [--realtime]")
                sys.exit(1)

            pool_id = sys.argv[2]
            from_block = int(sys.argv[3]) if len(sys.argv) > 3 else listener.w3.eth.block_number - 1000
            realtime = '--realtime' in sys.argv

            listener.listen_specific_pool(pool_id, from_block, realtime)

        elif command == "realtime":
            # Real-time monitoring
            listener.listen_realtime()

        else:
            print(f"Unknown command: {command}")
            print("Usage: python listen_swaps.py [realtime|historical|pool]")
            sys.exit(1)
    else:
        # Default: real-time monitoring
        listener.listen_realtime()


if __name__ == "__main__":
    main()
