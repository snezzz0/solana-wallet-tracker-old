'''
This bot monitors a Discord channel for token transactions and sends formatted alerts to a webhook.
It tracks wallet positions, calculates PNL, and integrates with Rugcheck for risk assessment.
'''

import discord
from discord import Webhook
import csv
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple, Set
import json
import time
from pathlib import Path
import re
import os
from dotenv import load_dotenv
import logging

#TODO - sell/buy transactions sometimes wrong , SOL value is also wrong , more simple loggin

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

_wallet_cache = {}
_cache_time = 0
_cache_lifetime = 60  # seconds - adjust as needed

# Access the variables
HELIUS_API_KEY = os.getenv('HELIUS_API_KEY')
BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY')

# Define stable coins that should be treated like USDC
STABLE_COINS = ['USDC', 'USDT']

def get_wallet_names():
    """Load wallet names directly from the JSON file"""
    try:
        with open('config/wallet_names.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Error: wallet_names.json file not found.")
        return {}
    except json.JSONDecodeError:
        logger.error("Error: wallet_names.json is not a valid JSON file.")
        return {}

def get_current_wallet_names():
    """Get wallet names with caching to reduce file reads"""
    global _wallet_cache, _cache_time
    current_time = time.time()
    
    # If cache is expired or empty, reload from file
    if current_time - _cache_time > _cache_lifetime or not _wallet_cache:
        logger.debug("Wallet cache expired or empty, reloading from file")
        _wallet_cache = get_wallet_names()
        _cache_time = current_time
        
    return _wallet_cache

def reload_wallet_names():
    """Force reload of wallet names regardless of cache time"""
    global _wallet_cache, _cache_time
    logger.info("Manually reloading wallet names from file")
    _wallet_cache = get_wallet_names()
    _cache_time = time.time()
    return _wallet_cache

# Initial load to populate the cache
_wallet_cache = get_wallet_names()
_cache_time = time.time()

# Load wallet names 


USDC_SYMBOL = "USDC"
USDC_NAME = "USD Coin"

# Bot configuration
SOURCE_CHANNEL_ID = 1339998324484473025

class TokenTracker:
    def __init__(self):
        self.holder_positions = {}  # Format: {token_mint: {wallet: amount}}
    
    def get_holder_type(self, token_mint: str, wallet: str) -> str:
        if token_mint not in self.holder_positions:
            self.holder_positions[token_mint] = {}
            return 'FIRST_HOLDER'
        
        if wallet not in self.holder_positions[token_mint]:
            return 'NEW_HOLDER'
        
        return 'EXISTING_HOLDER'
    
    def update_holder_position(self, token_mint: str, wallet: str, amount: float, is_buy: bool) -> Tuple[float, float]:
        """
        Update holder position with improved error handling
        """
        if token_mint not in self.holder_positions:
            self.holder_positions[token_mint] = {}
        
        current_position = self.holder_positions[token_mint].get(wallet, 0)
        previous_amount = current_position
        
        if is_buy:
            self.holder_positions[token_mint][wallet] = current_position + amount
        else:
            new_amount = max(0, current_position - amount)
            if new_amount == 0 and wallet in self.holder_positions[token_mint]:
                # Only try to delete if the wallet exists in the positions
                self.holder_positions[token_mint].pop(wallet, None)
            else:
                self.holder_positions[token_mint][wallet] = new_amount
        
        current_amount = self.holder_positions[token_mint].get(wallet, 0)
        return previous_amount, current_amount

class TokenBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.tracker = TokenTracker()
        self.webhook = None
        self.rugcheck_session = None
    
    async def setup_hook(self):
        self.webhook = Webhook.from_url(WEBHOOK_URL, session=aiohttp.ClientSession())
        self.rugcheck_session = aiohttp.ClientSession()
    
    async def get_rugcheck_data(self, mint: str) -> Optional[Dict]:
        """Fetch Rugcheck data for a token using the public token summary endpoint."""
        try:
            async with self.rugcheck_session.get(f'https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary') as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning(f"Rugcheck API returned status {resp.status} for mint {mint}")
                return None
        except Exception as e:
            logger.error(f"Error fetching Rugcheck data: {e}")
            return None

    def format_rugcheck_risks(self, rugcheck_data: Dict) -> str:
        """Format Rugcheck risks into a readable string."""
        if not rugcheck_data:
            return "No risk data available"
        
        formatted_risks = []
        
        # Add overall risk score
        risk_score = rugcheck_data.get('risk_score', 0)
        risk_level = 'HIGH' if risk_score >= 7 else 'MEDIUM' if risk_score >= 4 else 'LOW'
        emoji = '🔴' if risk_level == 'HIGH' else '🟡' if risk_level == 'MEDIUM' else '🟢'
        formatted_risks.append(f"{emoji} **Overall Risk Score**: {risk_score} ({risk_level})")
        
        # Add risk factors
        risk_factors = rugcheck_data.get('risk_factors', [])
        for factor in risk_factors:
            name = factor.get('name', 'Unknown Factor')
            description = factor.get('description', 'No description available')
            severity = factor.get('severity', 'unknown').upper()
            
            # Add emoji based on severity
            emoji = {
                'HIGH': '🔴',
                'MEDIUM': '🟡',
                'LOW': '🟢',
                'UNKNOWN': '⚪'
            }.get(severity, '⚪')
            
            formatted_risks.append(f"{emoji} **{name}**\n{description}")
        
        return "\n\n".join(formatted_risks)

    def debug_print_description(self, description: str, wallet: str):
        """Print detailed debug information about the description."""
        logger.debug("\n=== Debug Information ===")
        logger.debug(f"Full Description: '{description}'")
        logger.debug(f"Wallet Address: '{wallet}'")
        logger.debug(f"Description Length: {len(description)}")
        logger.debug("Character Analysis:")
        for i, char in enumerate(description):
            logger.debug(f"Position {i}: '{char}' (ASCII: {ord(char)})")
        logger.debug("========================\n")

    async def get_token_info(self, mint: str) -> Optional[Dict]:
        """
        Fetch token information from Jupiter and DexScreener APIs.
        Prioritizes Jupiter for price data but falls back to DexScreener if needed.
        """
        try:
            token_info = {
                'price': None,
                'marketCap': None,
                'dexMarketCap': None,
                'name': 'Unknown',
                'symbol': 'Unknown',
                'priceUsd': None,
                'h24Volume': None,
                'm5Volume': None,
                'pairCreatedAt': None  # New field
            }       

            async with aiohttp.ClientSession() as session:
                # Fetch price data from Jupiter
                price_url = f"https://api.jup.ag/price/v2?ids={mint},So11111111111111111111111111111111111111112"
                dex_url = f"https://api.dexscreener.com/tokens/v1/solana/{mint}"
                
                jupiter_data = None
                dex_data = None
                
                # Get Jupiter data
                try:
                    async with session.get(price_url) as resp:
                        if resp.status == 200:
                            jupiter_data = await resp.json()
                except Exception as e:
                    logger.error(f"Error fetching Jupiter data: {e}")
                
                # Get DexScreener data
                try:
                    async with session.get(dex_url) as resp:
                        if resp.status == 200:
                            dex_data = await resp.json()
                except Exception as e:
                    logger.error(f"Error fetching DexScreener data: {e}")
                
                # Parse Jupiter data first (preferred source for price)
                if jupiter_data and 'data' in jupiter_data and mint in jupiter_data['data']:
                    try:
                        mint_data = jupiter_data['data'].get(mint)
                        if mint_data is not None:  # Check if mint data exists
                            jupiter_price = mint_data.get('price')
                            if jupiter_price is not None:  # Check if price is not None
                                token_info['price'] = float(jupiter_price)
                                token_info['marketCap'] = round(token_info['price'] * 1000000000, 0)
                                token_info['priceUsd'] = token_info['price']  # Use Jupiter price as USD price
                    except (ValueError, TypeError, KeyError) as e:
                        logger.error(f"Error processing Jupiter price data for {mint}: {e}")
                
                # Parse DexScreener data
                if dex_data:
                    dex_pairs = []
                    if isinstance(dex_data, dict) and 'pairs' in dex_data:
                        dex_pairs = dex_data.get('pairs', [])
                    elif isinstance(dex_data, list):
                        dex_pairs = dex_data
                    
                    if dex_pairs and len(dex_pairs) > 0:
                        pair = dex_pairs[0]
                        # Add safety check - ensure pair is a dictionary
                        if not isinstance(pair, dict):
                            logger.error(f"DexScreener pair data is not a dictionary for {mint}")
                            pair = {}
                            
                        base_token = pair.get('baseToken', {})
                        # Add safety check - ensure base_token is a dictionary
                        if not isinstance(base_token, dict):
                            logger.error(f"DexScreener baseToken is not a dictionary for {mint}")
                            base_token = {}
                        
                        # Always get name and symbol from DexScreener
                        token_info['name'] = base_token.get('name', 'Unknown')
                        token_info['symbol'] = base_token.get('symbol', 'Unknown')
                        token_info['pairCreatedAt'] = pair.get('pairCreatedAt')
                        
                        # Get volume data from DexScreener with additional checks
                        volume = pair.get('volume')
                        if volume and isinstance(volume, dict):
                            token_info['h24Volume'] = volume.get('h24')
                            token_info['m5Volume'] = volume.get('m5')
                        
                        # Get dexMarketCap from DexScreener
                        token_info['dexMarketCap'] = pair.get('marketCap')
                        
                        # Only use DexScreener price if Jupiter price is not available
                        if token_info['price'] is None:
                            try:
                                price_usd = pair.get('priceUsd')
                                if price_usd is not None:
                                    token_info['priceUsd'] = float(price_usd)
                                    token_info['price'] = token_info['priceUsd']
                                    if token_info['price']:
                                        token_info['marketCap'] = pair.get('marketCap')
                            except (ValueError, TypeError) as e:
                                logger.error(f"Error processing DexScreener price data for {mint}: {e}")
                
                # If we don't have a name and symbol, the token might not exist
                if token_info['name'] == 'Unknown' and token_info['symbol'] == 'Unknown':
                    logger.warning(f"Could not find token information for {mint}")
                    return None
                
                # Even if we don't have price data, return what we have if we at least have name/symbol
                return token_info
            
        except Exception as e:
            logger.error(f"Failed to fetch token info for {mint}: {e}")
            return None

    def extract_tx_signature(self, explorer_url: str) -> Optional[str]:
        """Extract transaction signature from Explorer URL."""
        try:
            # Split URL by '/' and get the last part which should be the signature
            signature = explorer_url.split('/')[-1]
            logger.info(f"Transaction Signature Found: {signature}")  # Log the found signature
            return signature
        except Exception as e:
            logger.error(f"Error extracting transaction signature: {e}")
            return None

    async def get_token_mint_from_alchemy(self, transaction_signature: str) -> Optional[str]:
        """Fetch token mint address from Alchemy API using transaction signature."""
        try:
            # Define Alchemy API endpoint and key
            alchemy_url = f"https://solana-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    alchemy_url,
                    json={
                        "id": 1,
                        "jsonrpc": "2.0",
                        "method": "getTransaction",
                        "params": [
                            transaction_signature,
                            {
                                "encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0
                            }
                        ]
                    },
                    headers={"Content-Type": "application/json"}
                ) as alchemy_response:
                    alchemy_data = await alchemy_response.json()
                    logger.info(f"Debug - Alchemy API Response Status: {alchemy_response.status}")
                    
                    if alchemy_response.status == 200 and alchemy_data.get("result"):
                        result = alchemy_data["result"]
                        
                        # Parse token balances from meta.postTokenBalances
                        if "meta" in result and "postTokenBalances" in result["meta"]:
                            for balance in result["meta"]["postTokenBalances"]:
                                if "mint" in balance:
                                    mint = balance["mint"]
                                    if mint and mint != 'So11111111111111111111111111111111111111112':
                                        logger.info(f"Found mint in Alchemy response: {mint}")
                                        return mint
                                        
                        # Parse instruction data for SPL token transfers
                        if "transaction" in result and "message" in result["transaction"]:
                            instructions = result["transaction"]["message"].get("instructions", [])
                            for instr in instructions:
                                if "parsed" in instr and "type" in instr["parsed"]:
                                    if instr["parsed"]["type"] == "transferChecked" or instr["parsed"]["type"] == "transfer":
                                        info = instr["parsed"].get("info", {})
                                        mint = info.get("mint")
                                        if mint and mint != 'So11111111111111111111111111111111111111112':
                                            logger.info(f"Found mint in instruction data: {mint}")
                                            return mint
                        
                        logger.warning("No valid token mint found in Alchemy response")
                    else:
                        logger.error(f"Error response from Alchemy API: {alchemy_response.status}")
                        logger.error(f"Alchemy Response body: {alchemy_data}")
                
                return None
        except Exception as e:
            logger.error(f"Error fetching transaction from Alchemy: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def format_usdc_message(self, data: Dict, timestamp: datetime) -> discord.Embed:
        """Format message for stablecoin transactions using embeds."""
        display_name = get_current_wallet_names().get(data['wallet'], f"{data['wallet'][:8]}...")
        
        stable_coin = data.get('stable_coin', 'USDC')
        
        # Create embed
        embed = discord.Embed(
            title=f"💵 {stable_coin} Trade",
            color=0x87CEEB  # Light blue color
        )
        
        if data['is_buy']:
            description = f"**{display_name}** bought {data['sol_amount']:.2f} SOL for {data['token_amount']:.2f} {stable_coin}"
        else:
            description = f"**{display_name}** sold {data['sol_amount']:.2f} SOL for {data['token_amount']:.2f} {stable_coin}"
        
        embed.description = description
        
        # Add one hour to the timestamp only for Discord display
        display_timestamp = timestamp - timedelta(hours=1)
        embed.timestamp = display_timestamp
        
        return embed

    def format_message(self, data: Dict, timestamp: datetime) -> discord.Embed:
        """Format token transaction messages using embeds."""
        display_name = get_current_wallet_names().get(data['wallet'], f"{data['wallet'][:8]}...")

        # Set title based on holder type
        if data['is_buy']:
            if data['holder_type'] == 'FIRST_HOLDER':
                title = "🎯 First Holder Alert!"
                color = 0xFFD700  # Gold
            elif data['holder_type'] == 'NEW_HOLDER':
                title = "🆕 New Holder Alert!"
                color = 0x00FF00  # Green
            else:
                title = "💫 Bought More!"
                color = 0x1E90FF  # Blue
        else:
            title = "💔 Sell Alert!"
            color = 0xFF0000  # Red
        
        # Create embed
        embed = discord.Embed(
            title=title,
            color=color
        )

        # Add token information
        token_info = data.get('token_info', {})
        token_name = f"{token_info.get('name', 'Unknown')} ({token_info.get('symbol', 'N/A')})"
        embed.add_field(name="🪙 Token", value=token_name, inline=True)

        embed.add_field(
            name=f"{display_name}",
            value=f"{data['sol_amount']:.2f} SOL",
            inline=True
        )

        # Token mint
        embed.add_field(
            name="📝 Token Mint",
            value=data['token_mint'],
            inline=False
        )

        # Add Rugcheck risk assessment if available
        if 'rugcheck_data' in data:
            risk_assessment = self.format_rugcheck_risks(data['rugcheck_data'])
            embed.add_field(
                name="🔍 Risk Assessment",
                value=risk_assessment,
                inline=False
            )

        # Add token creation time if available
        if 'pairCreatedAt' in data['token_info']:
            created_timestamp = datetime.fromtimestamp(data['token_info']['pairCreatedAt'] / 1000, tz=timezone.utc)
            time_diff = timestamp - created_timestamp
            minutes_ago = int(time_diff.total_seconds() / 60)
            if minutes_ago < 60:
                created_text = f"~{minutes_ago} minutes ago"
            else:
                hours_ago = minutes_ago // 60
                created_text = f"~{hours_ago} hours ago"
            embed.add_field(
                name="⏰ Creation Time",
                value=created_text,
                inline=True
            )
        
        # Amount and Market Cap in same row
        embed.add_field(
            name="💰 Amount",
            value=f"{(data['token_amount']/1000000):.1f}M tokens",
            inline=True
        )

        # Determine which market cap to display
        market_cap = data['token_info'].get('marketCap', 0)
        dex_market_cap = data['token_info'].get('dexMarketCap', 0)
    
        # Convert to integers for comparison, handling None values
        mc_value = int(market_cap) if market_cap is not None else 0
        dex_mc_value = int(dex_market_cap) if dex_market_cap is not None else 0
    
        # If the difference between marketCap and dexMarketCap is greater than 200,000, use dexMarketCap
        if abs(mc_value - dex_mc_value) > 200000 and dex_mc_value > 0:
            market_cap_to_display = dex_mc_value
            emoji = "🦅"  # Eagle emoji for dex market cap
        else:
            market_cap_to_display = mc_value
            emoji = "📊"  # Chart emoji for regular market cap

        # Display market cap with appropriate emoji
        if market_cap_to_display > 0:
            embed.add_field(
                name=f"{emoji} Market Cap",
                value=f"${market_cap_to_display:,}",
                inline=True
            )

        # Add volumes
        m5_volume = token_info.get('m5Volume')
        if m5_volume is not None:
            embed.add_field(
                name="📈 5m Volume",
                value=f"${int(m5_volume):,}",
                inline=True
            )
        else:
            embed.add_field(
                name="📈 5m Volume",
                value="N/A",
                inline=True
            )
        h24_volume = token_info.get('h24Volume')
        if h24_volume is not None:
            embed.add_field(
                name="📊 24h Volume",
                value=f"${int(h24_volume):,}",
                inline=True
            )
        else:
            embed.add_field(
                name="📊 24h Volume",
                value="N/A",
                inline=True
            )
        if not data['is_buy'] and data.get('sell_percentage') is not None:
            position_text = "Sold entire position" if data['sell_percentage'] >= 100 else f"Sold {data['sell_percentage']:.1f}% of position"
            embed.add_field(
                name="📉 Position",
                value=position_text,
                inline=True
            )
        # Add GMGN link
        embed.add_field(
            name="🐊 GMGN",
            value=f"[View on GMGN](https://gmgn.ai/sol/token/IWzYo3Nv_{data['token_mint']}?maker={data['wallet']})",
            inline=False
        )
        
        # Add one hour to the timestamp only for Discord display
        display_timestamp = timestamp - timedelta(hours=1)
        embed.timestamp = display_timestamp

        return embed

    async def format_token_swap_message(self, data: Dict, timestamp: datetime) -> discord.Embed:
        """Format message for token-to-token swap transactions using embeds."""
        display_name = get_current_wallet_names().get(data['wallet'], f"{data['wallet'][:8]}...")
        
        # Create embed with a distinct color for token swaps
        embed = discord.Embed(
            title="🔄 Token Swap",
            color=0x9932CC  # Purple color to distinguish from other transactions
        )
        
        # Add the basic swap information
        description = f"**{display_name}** swapped {data['from_amount']:.2f} {data['from_symbol']} for {data['to_amount']:.2f} {data['to_symbol']}"
        embed.description = description
        
        # Add token mint addresses
        embed.add_field(
            name="📤 From Token",
            value=f"{data['from_symbol']} ({data['from_mint']})",
            inline=True
        )
        embed.add_field(
            name="📥 To Token",
            value=f"{data['to_symbol']} ({data['to_mint']})",
            inline=True
        )

        # Add price in SOL for the token being swapped to
        if data.get('to_price_sol') is not None:
            embed.add_field(
                name="💰 Price in SOL",
                value=f"{data['to_price_sol']:.8f}",
                inline=True
            )
        
        # Add market cap if available
        if data.get('to_market_cap') is not None and data['to_market_cap'] > 0:
            embed.add_field(
                name="📊 Market Cap",
                value=f"${int(data['to_market_cap']):,}",
                inline=True
            )

        # Add volumes for the token being swapped to
        if data.get('to_5m_volume') is not None:
            embed.add_field(
                name="📈 5m Volume",
                value=f"${int(data['to_5m_volume']):,}" if data['to_5m_volume'] > 0 else "N/A",
                inline=True
            )
        
        if data.get('to_24h_volume') is not None:
            embed.add_field(
                name="📊 24h Volume",
                value=f"${int(data['to_24h_volume']):,}" if data['to_24h_volume'] > 0 else "N/A",
                inline=True
            )
        
        # Add GMGN links for both tokens
        embed.add_field(
            name="🐊 GMGN Links",
            value=(f"[From Token](https://gmgn.ai/sol/token/IWzYo3Nv_{data['from_mint']}?maker={data['wallet']})\n"
                   f"[To Token](https://gmgn.ai/sol/token/IWzYo3Nv_{data['to_mint']}?maker={data['wallet']})"),
            inline=False
        )
        
        # Add one hour to the timestamp only for Discord display
        display_timestamp = timestamp - timedelta(hours=1)
        embed.timestamp = display_timestamp
        return embed

    def calculate_sol_amount(self, result: Dict, wallet: str, is_buy: bool) -> float:
        """Calculate SOL amount from transaction data with improved accuracy."""
        try:
            if "meta" not in result or "preBalances" not in result["meta"] or "postBalances" not in result["meta"]:
                return 1.09 if is_buy else 0.9

            pre_sol = result["meta"]["preBalances"]
            post_sol = result["meta"]["postBalances"]
            accounts = result["transaction"]["message"].get("accountKeys", [])

            # Track the largest SOL changes
            changes = []
            for i, account in enumerate(accounts):
                if isinstance(account, str):
                    pre_amount = pre_sol[i] / 1_000_000_000  # Convert lamports to SOL
                    post_amount = post_sol[i] / 1_000_000_000
                    change = post_amount - pre_amount
                    changes.append((account, change))

            # Sort changes by absolute value
            changes.sort(key=lambda x: abs(x[1]), reverse=True)

            # For buys: look for negative changes (SOL spent)
            # For sells: look for positive changes (SOL received)
            if is_buy:
                relevant_changes = [c for _, c in changes if c < 0]
                if relevant_changes:
                    sol_amount = abs(max(relevant_changes, key=abs))
                    # Subtract fees and round to reasonable number
                    return round(sol_amount - 0.000005, 3)
            else:
                relevant_changes = [c for _, c in changes if c > 0]
                if relevant_changes:
                    sol_amount = max(relevant_changes)
                    return round(sol_amount, 3)

            # Fallback values if no appropriate changes found
            return 1.09 if is_buy else 0.9

        except Exception as e:
            logger.error(f"Error calculating SOL amount: {e}")
            return 1.09 if is_buy else 0.9

    async def parse_helius_embed(self, embed: discord.Embed) -> Optional[Dict]:
        try:
            source = next((f.value for f in embed.fields if f.name == 'Source'), None)
            if source == 'pump_fun' or source == 'PUMP_AMM' or not source:
                return None
            
            explorer_url = next((f.value for f in embed.fields if f.name == 'Explorer'), None)
            tx_signature = None
            if explorer_url:
                tx_signature = self.extract_tx_signature(explorer_url)
            
            description = next((f.value for f in embed.fields if f.name == 'Description'), None)
            
            # Skip PUMP_AMM transactions
            if source == 'PUMP_AMM':
                logger.info("Skipping PUMP_AMM transaction")
                return None
            
            if not description:
                logger.warning("No description found, skipping transaction")
                return None
            
            # ... rest of the existing code ...
        except Exception as e:
            logger.error(f"Error parsing Helius embed: {e}")
            return None

    async def get_transaction_details_from_alchemy(self, transaction_signature: str) -> Optional[Dict]:
        """Fetch and parse PUMP_AMM transaction details from Alchemy API."""
        try:
            logger.info(f"Getting PUMP_AMM transaction details for {transaction_signature}")
            # Define Alchemy API endpoint and key
            alchemy_url = f"https://solana-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    alchemy_url,
                    json={
                        "id": 1,
                        "jsonrpc": "2.0",
                        "method": "getTransaction",
                        "params": [
                            transaction_signature,
                            {
                                "encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0
                            }
                        ]
                    },
                    headers={"Content-Type": "application/json"}
                ) as alchemy_response:
                    alchemy_data = await alchemy_response.json()
                    
                    if alchemy_response.status != 200 or not alchemy_data.get("result"):
                        logger.error(f"Error response from Alchemy API: {alchemy_response.status}")
                        return None
                    
                    result = alchemy_data["result"]
                    
                    # Get the wallet address from the fee payer (first account)
                    wallet = None
                    if "transaction" in result and "message" in result["transaction"]:
                        accounts = result["transaction"]["message"].get("accountKeys", [])
                        if accounts and len(accounts) > 0:
                            first_account = accounts[0]
                            # Handle different account formats
                            if isinstance(first_account, str):
                                wallet = first_account
                            elif isinstance(first_account, dict):
                                if "pubkey" in first_account:
                                    wallet = first_account["pubkey"]
                                elif len(first_account) > 0:
                                    # If it's a dict with no pubkey, try to get the first key
                                    wallet = list(first_account.keys())[0]
                            
                            if not isinstance(wallet, str):
                                logger.warning(f"Unexpected wallet type: {type(wallet)}")
                                try:
                                    wallet = str(wallet)
                                except:
                                    logger.error("Could not convert wallet to string")
                                    return None
                    
                    if not wallet:
                        logger.warning("Could not determine wallet address from PUMP_AMM transaction")
                        return None
                    
                    # Ensure wallet is a string to avoid 'unhashable type: dict' errors
                    if not isinstance(wallet, str):
                        logger.warning(f"Wallet is not a string but a {type(wallet)}: {wallet}")
                        try:
                            wallet = str(wallet)
                        except:
                            logger.error("Could not convert wallet to string")
                            return None
                    
                    # Get pre and post token balances
                    pre_balances = result.get("meta", {}).get("preTokenBalances", [])
                    post_balances = result.get("meta", {}).get("postTokenBalances", [])
                    
                    # Parse token transfers
                    token_mints = set()
                    sol_involved = False
                    stable_coin_involved = False
                    stable_coin_type = None
                    
                    # Extract all token mints involved
                    for balance in pre_balances + post_balances:
                        if "mint" in balance:
                            mint_value = balance["mint"]
                            # Ensure we're only adding hashable types (strings) to the set
                            if isinstance(mint_value, str):
                                token_mints.add(mint_value)
                            else:
                                logger.warning(f"Skipping non-string mint value: {type(mint_value)}")
                    
                    # Check if SOL is involved
                    if "meta" in result and "preBalances" in result["meta"] and "postBalances" in result["meta"]:
                        pre_sol = result["meta"]["preBalances"]
                        post_sol = result["meta"]["postBalances"]
                        if any(pre != post for pre, post in zip(pre_sol, post_sol)):
                            sol_involved = True
                    
                    # Check if stablecoins are involved
                    for mint in token_mints:
                        try:
                            token_info = await self.get_token_info(mint)
                            if token_info and isinstance(token_info, dict) and token_info.get("symbol") in STABLE_COINS:
                                stable_coin_involved = True
                                stable_coin_type = token_info.get("symbol")
                                break
                        except Exception as e:
                            logger.warning(f"Error checking if mint {mint} is a stablecoin: {e}")
                            continue
                    
                    # If we have stable coin and SOL, treat as a stablecoin/SOL swap
                    if stable_coin_involved and sol_involved:
                        # Determine if it's a buy or sell
                        is_buy = False
                        sol_amount = 0
                        stable_amount = 0
                        
                        # Look at instructions for transfer details
                        if "transaction" in result and "message" in result["transaction"]:
                            instructions = result["transaction"]["message"].get("instructions", [])
                            for instr in instructions:
                                if "parsed" in instr and "type" in instr["parsed"]:
                                    if instr["parsed"]["type"] in ["transferChecked", "transfer"]:
                                        info = instr["parsed"].get("info", {})
                                        if "amount" in info:
                                            try:
                                                # Check for SOL transfers based on account types
                                                if instr["program"] == "System":
                                                    sol_amount = float(info["amount"]) / 1_000_000_000
                                                    is_buy = info["destination"] == wallet
                                                # Check for token transfers
                                                elif "tokenAmount" in info:
                                                    mint_value = info.get("mint")
                                                    # Check if mint is hashable and in token_mints
                                                    if isinstance(mint_value, str) and mint_value in token_mints:
                                                        token_info = await self.get_token_info(mint_value)
                                                        if token_info and token_info.get("symbol") in STABLE_COINS:
                                                            stable_amount = float(info["tokenAmount"]["amount"]) / 1_000_000
                                                            is_buy = info["source"] == wallet
                                            except (ValueError, TypeError) as e:
                                                logger.warning(f"Error processing instruction: {e}")
                                                continue
                        
                        # If we couldn't determine exact amounts, use rough estimates
                        if sol_amount == 0:
                            sol_amount = 1.0  # Default estimate
                        if stable_amount == 0:
                            stable_amount = 10.0  # Default estimate
                            
                        return {
                            'wallet': wallet,
                            'sol_amount': sol_amount,
                            'token_amount': stable_amount,
                            'is_usdc': True,
                            'stable_coin': stable_coin_type or 'USDC',
                            'is_buy': is_buy
                        }
                    
                    # If there are two token mints and neither is a stablecoin, it's a token swap
                    # For PUMP_AMM, we try to determine if one of the tokens is SOL first,
                    # since PUMP_AMM transactions are primarily SOL-token swaps
                    elif len(token_mints) >= 2 and not stable_coin_involved:
                        token_mints = list(token_mints)
                        
                        # Check if one of the tokens is SOL (wrapped SOL)
                        sol_mint_index = -1
                        for i, mint in enumerate(token_mints):
                            if mint == 'So11111111111111111111111111111111111111112':
                                sol_mint_index = i
                                break
                        
                        # If one of the tokens is SOL, treat as a buy/sell instead of a swap
                        if sol_mint_index != -1:
                            # If SOL is the first token, it's a buy
                            is_buy = sol_mint_index != 0  # Flipped logic: if SOL is first, it's actually a sell
                            sol_amount = 0
                            token_amount = 0
                            
                            # Remove SOL from token_mints
                            sol_mint = token_mints.pop(sol_mint_index)
                            
                            # Get the remaining token mint
                            if token_mints:
                                token_mint = token_mints[0]
                                
                                # Ensure token_mint is a string
                                if not isinstance(token_mint, str):
                                    logger.warning(f"token_mint is not a string: {type(token_mint)}")
                                    try:
                                        token_mint = str(token_mint)
                                    except:
                                        logger.error("Could not convert token_mint to string")
                                        return None
                                
                                # Get token info
                                token_info = await self.get_token_info(token_mint)
                                
                                if token_info:
                                    # Check token balances to determine buy/sell
                                    for pre, post in zip(pre_balances, post_balances):
                                        if pre.get("mint") == sol_mint and pre.get("owner") == wallet:
                                            pre_amount = float(pre.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                            post_amount = float(post.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                            sol_amount = abs(pre_amount - post_amount) / 1_000_000_000  # Convert lamports to SOL
                                        
                                        if pre.get("mint") == token_mint and pre.get("owner") == wallet:
                                            pre_amount = float(pre.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                            post_amount = float(post.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                            token_amount = abs(post_amount - pre_amount) if is_buy else abs(pre_amount - post_amount)
                                            
                                            # Check if the token amount is unreasonably high (often a decimal place issue)
                                            if token_amount > 1_000_000_000_000:  # If over 1 trillion
                                                token_amount = token_amount / 1_000_000  # Scale down by a million
                                                logger.info(f"Scaled down extremely large token amount to {token_amount}")
                                    
                                    # If we couldn't determine amounts, estimate from native balances
                                    if sol_amount == 0:
                                        # Try to get SOL amount from native balance changes
                                        if "meta" in result and "preBalances" in result["meta"] and "postBalances" in result["meta"]:
                                            pre_sol = result["meta"]["preBalances"]
                                            post_sol = result["meta"]["postBalances"]
                                            
                                            # Find the wallet account's change
                                            account_changes = []
                                            for i, account in enumerate(result["transaction"]["message"].get("accountKeys", [])):
                                                if isinstance(account, str):
                                                    pre_sol_amount = pre_sol[i] / 1_000_000_000
                                                    post_sol_amount = post_sol[i] / 1_000_000_000
                                                    change = post_sol_amount - pre_sol_amount
                                                    account_changes.append((account, change))
                                            
                                            # Sort by absolute change value (largest change first, ignoring sign)
                                            account_changes.sort(key=lambda x: abs(x[1]), reverse=True)
                                            
                                            # Check if wallet is in top changes
                                            wallet_change = 0
                                            for acc, change in account_changes:
                                                if acc == wallet:
                                                    wallet_change = change
                                                    break
                                            
                                            # Use the most significant change as an estimate
                                            if account_changes:
                                                # If selling, use positive change (SOL received)
                                                # If buying, use negative change (SOL spent)
                                                if not is_buy:
                                                    # Find the largest positive change (received SOL)
                                                    largest_positive = max([c for _, c in account_changes if c > 0], default=0)
                                                    if largest_positive > 0:
                                                        sol_amount = largest_positive
                                                    elif wallet_change > 0:
                                                        sol_amount = wallet_change
                                                    else:
                                                        sol_amount = abs(account_changes[0][1]) if account_changes[0][1] < 0 else 1.0
                                                else:
                                                    # Find the largest negative change (spent SOL)
                                                    largest_negative = min([c for _, c in account_changes if c < 0], default=0)
                                                    if largest_negative < 0:
                                                        sol_amount = abs(largest_negative)
                                                    elif wallet_change < 0:
                                                        sol_amount = abs(wallet_change)
                                                    else:
                                                        sol_amount = abs(account_changes[0][1]) if account_changes[0][1] > 0 else 1.0
                                                
                                                # Adjust for transaction fees
                                                if is_buy:
                                                    sol_amount -= 0.00001
                                    
                                    # Analyze the token mint for additional information
                                    token_type = "unknown"
                                    if token_mint:
                                        if token_mint.endswith("pump"):
                                            token_type = "pump"
                                        elif "pump" in token_mint.lower():
                                            token_type = "pump-related"
                                    
                                    # For pump tokens, use standard amounts
                                    if token_type in ["pump", "pump-related"]:
                                        logger.info(f"Detected pump token type: {token_type}")
                                        if sol_amount == 0 or sol_amount > 50:
                                            # Pumps typically cost 1-3 SOL
                                            sol_amount = 1.09 if is_buy else 0.9
                                    
                                    # If still zero, use a sensible default based on token price
                                    if sol_amount == 0 or sol_amount > 100:  # Cap at 100 SOL as a safety measure
                                        # Check if token has price information
                                        if token_info and token_info.get('price') and token_amount > 0:
                                            sol_amount = token_info.get('price') * token_amount / 1_000_000
                                        else:
                                            sol_amount = 1.1  # Default fallback
                                    
                                    # Final sanity check - cap SOL amount at a reasonable value
                                    if sol_amount > 10 or sol_amount < 0:
                                        logger.warning(f"Unrealistic SOL amount detected: {sol_amount}. Capping to reasonable value.")
                                        # If buying, default to 1-2 SOL; if selling, use a percentage of token amount
                                        sol_amount = 1.09 if is_buy else min(3.0, token_amount * 0.00001)
                                    
                                    # If token amount is still zero, use a reasonable estimate
                                    if token_amount == 0:
                                        if sol_amount > 0 and token_info and token_info.get('price') and token_info.get('price') > 0:
                                            token_amount = sol_amount / token_info.get('price') * 1_000_000
                                        else:
                                            token_amount = 1_000_000  # Default fallback
                                    
                                    logger.info(f"PUMP_AMM transaction: SOL amount={sol_amount}, token amount={token_amount}, type={token_type}")

                                    # Get holder type for buys
                                    holder_type = None
                                    if is_buy and hasattr(self, 'tracker') and self.tracker:
                                        holder_type = self.tracker.get_holder_type(token_mint, wallet)
                                    
                                    # Update holder position
                                    prev_amount, curr_amount = 0, 0
                                    if hasattr(self, 'tracker') and self.tracker:
                                        prev_amount, curr_amount = self.tracker.update_holder_position(
                                            token_mint, 
                                            wallet, 
                                            token_amount, 
                                            is_buy
                                        )
                                    
                                    # Calculate sell percentage
                                    sell_percentage = None
                                    if not is_buy and prev_amount > 0:
                                        sell_percentage = (token_amount / prev_amount) * 100
                                    
                                    logger.info(f"Treating PUMP_AMM as a {'buy' if is_buy else 'sell'} transaction")
                                    return {
                                        'wallet': wallet,
                                        'sol_amount': sol_amount,
                                        'token_amount': token_amount,
                                        'token_mint': token_mint,
                                        'token_info': token_info,
                                        'is_buy': is_buy,
                                        'holder_type': holder_type,
                                        'sell_percentage': sell_percentage,
                                        'is_usdc': False
                                    }
                        
                        # If no SOL is involved, process as a token swap as before
                        from_mint = token_mints[0]
                        to_mint = token_mints[1]
                        
                        # Ensure mint addresses are strings
                        if not isinstance(from_mint, str):
                            logger.warning(f"from_mint is not a string: {type(from_mint)}")
                            try:
                                from_mint = str(from_mint)
                            except:
                                logger.error("Could not convert from_mint to string")
                                return None
                        
                        if not isinstance(to_mint, str):
                            logger.warning(f"to_mint is not a string: {type(to_mint)}")
                            try:
                                to_mint = str(to_mint)
                            except:
                                logger.error("Could not convert to_mint to string")
                                return None
                        
                        # Get token info
                        from_token_info = await self.get_token_info(from_mint)
                        to_token_info = await self.get_token_info(to_mint)
                        
                        if from_token_info and to_token_info:
                            # Default values for amounts
                            from_amount = 1.0
                            to_amount = 1.0
                            
                            # Try to get actual amounts from token balances
                            try:
                                for pre, post in zip(pre_balances, post_balances):
                                    if pre.get("mint") == from_mint and pre.get("owner") == wallet:
                                        pre_amount = float(pre.get("uiTokenAmount", {}).get("amount", 0))
                                        post_amount = float(post.get("uiTokenAmount", {}).get("amount", 0))
                                        from_amount = abs(pre_amount - post_amount)
                                    
                                    if post.get("mint") == to_mint and post.get("owner") == wallet:
                                        pre_amount = float(pre.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                        post_amount = float(post.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                        to_amount = abs(post_amount - pre_amount)
                            except (ValueError, TypeError):
                                pass
                            
                            return {
                                'wallet': wallet,
                                'from_amount': from_amount,
                                'to_amount': to_amount,
                                'from_mint': from_mint,
                                'to_mint': to_mint,
                                'from_symbol': from_token_info['symbol'],
                                'to_symbol': to_token_info['symbol'],
                                'to_price_sol': to_token_info.get('price'),
                                'to_market_cap': to_token_info.get('marketCap'),
                                'to_5m_volume': to_token_info.get('m5Volume'),
                                'to_24h_volume': to_token_info.get('h24Volume'),
                                'is_token_swap': True
                            }
                    
                    # If we have a single token mint and SOL, it's a token/SOL swap
                    elif len(token_mints) == 1 and sol_involved:
                        token_mint = list(token_mints)[0]
                        
                        # Ensure token_mint is a string
                        if not isinstance(token_mint, str):
                            logger.warning(f"token_mint is not a string: {type(token_mint)}")
                            try:
                                token_mint = str(token_mint)
                            except:
                                logger.error("Could not convert token_mint to string")
                                return None
                        
                        token_info = await self.get_token_info(token_mint)
                        
                        if not token_info:
                            logger.warning(f"No token info found for {token_mint}")
                            return None
                        
                        # Check token balances to determine buy/sell
                        for pre, post in zip(pre_balances, post_balances):
                            if pre.get("mint") == token_mint and pre.get("owner") == wallet:
                                pre_amount = float(pre.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                post_amount = float(post.get("uiTokenAmount", {}).get("amount", 0) or 0)
                                
                                # If token balance increased, it's a buy
                                if post_amount > pre_amount:
                                    is_buy = True
                                    token_amount = post_amount - pre_amount
                                else:
                                    token_amount = pre_amount - post_amount
                                
                                # Check if the token amount is unreasonably high (often a decimal place issue)
                                if token_amount > 1_000_000_000_000:  # If over 1 trillion
                                    token_amount = token_amount / 1_000_000  # Scale down by a million
                                    logger.info(f"Scaled down extremely large token amount to {token_amount}")
                        
                        # If we couldn't determine from token balances, default to buy
                        if token_amount == 0:
                            is_buy = True
                            token_amount = 1000000  # Default
                        
                        # Estimate SOL amount from pre/post balances using improved approach
                        if "meta" in result and "preBalances" in result["meta"] and "postBalances" in result["meta"]:
                            pre_sol = result["meta"]["preBalances"]
                            post_sol = result["meta"]["postBalances"]
                            
                            # Get all account balance changes
                            account_changes = []
                            for i, account in enumerate(result["transaction"]["message"].get("accountKeys", [])):
                                if isinstance(account, str):
                                    pre_sol_amount = pre_sol[i] / 1_000_000_000
                                    post_sol_amount = post_sol[i] / 1_000_000_000
                                    change = post_sol_amount - pre_sol_amount
                                    account_changes.append((account, change))
                            
                            # Sort by absolute change value (largest change first)
                            account_changes.sort(key=lambda x: abs(x[1]), reverse=True)
                            
                            # Check if wallet is in top changes
                            wallet_change = 0
                            for acc, change in account_changes:
                                if acc == wallet:
                                    wallet_change = change
                                    break
                            
                            # Use the most significant change as an estimate
                            if account_changes:
                                # If selling, look for positive changes (SOL received)
                                # If buying, look for negative changes (SOL spent)
                                if not is_buy:
                                    # Find the largest positive change (received SOL)
                                    largest_positive = max([c for _, c in account_changes if c > 0], default=0)
                                    if largest_positive > 0:
                                        sol_amount = largest_positive
                                    elif wallet_change > 0:
                                        sol_amount = wallet_change
                                    else:
                                        sol_amount = abs(account_changes[0][1]) if account_changes[0][1] < 0 else 1.0
                                else:
                                    # Find the largest negative change (spent SOL)
                                    largest_negative = min([c for _, c in account_changes if c < 0], default=0)
                                    if largest_negative < 0:
                                        sol_amount = abs(largest_negative)
                                    elif wallet_change < 0:
                                        sol_amount = abs(wallet_change)
                                    else:
                                        sol_amount = abs(account_changes[0][1]) if account_changes[0][1] > 0 else 1.0
                            
                                # Adjust for transaction fees
                                if is_buy:
                                    sol_amount -= 0.00001
                        
                        # Analyze the token mint for additional information
                        token_type = "unknown"
                        if token_mint:
                            if token_mint.endswith("pump"):
                                token_type = "pump"
                            elif "pump" in token_mint.lower():
                                token_type = "pump-related"
                        
                        # For pump tokens, use standard amounts
                        if token_type in ["pump", "pump-related"]:
                            logger.info(f"Detected pump token type: {token_type}")
                            if sol_amount == 0 or sol_amount > 50:
                                # Pumps typically cost 1-3 SOL
                                sol_amount = 1.09 if is_buy else 0.9
                        
                        # If we still don't have a SOL amount, try to estimate from token price
                        if sol_amount <= 0 or sol_amount > 10:  # Cap at 10 SOL as a safety measure
                            if token_info and token_info.get('price') and token_amount > 0:
                                sol_amount = token_info.get('price') * token_amount / 1_000_000
                            else:
                                sol_amount = 1.09 if is_buy else 0.9  # Default estimate
                        
                        # Final sanity check on SOL amount
                        if sol_amount > 10 or sol_amount < 0:
                            logger.warning(f"Unrealistic SOL amount detected: {sol_amount}. Capping to reasonable value.")
                            sol_amount = 1.09 if is_buy else min(3.0, token_amount * 0.00001)
                        
                        logger.info(f"Single token transaction: SOL amount={sol_amount}, token amount={token_amount}, is_buy={is_buy}, type={token_type}")

                        # Get holder type
                        holder_type = None
                        if is_buy and hasattr(self, 'tracker') and self.tracker:
                            holder_type = self.tracker.get_holder_type(token_mint, wallet)
                        
                        # Update holder position
                        prev_amount, curr_amount = 0, 0
                        if hasattr(self, 'tracker') and self.tracker:
                            prev_amount, curr_amount = self.tracker.update_holder_position(
                                token_mint, 
                                wallet, 
                                token_amount, 
                                is_buy
                            )
                        
                        # Calculate sell percentage
                        sell_percentage = None
                        if not is_buy and prev_amount > 0:
                            sell_percentage = (token_amount / prev_amount) * 100
                        
                        return {
                            'wallet': wallet,
                            'sol_amount': sol_amount,
                            'token_amount': token_amount,
                            'token_mint': token_mint,
                            'token_info': token_info,
                            'is_buy': is_buy,
                            'holder_type': holder_type,
                            'sell_percentage': sell_percentage,
                            'is_usdc': False
                        }
            
            return None
                
        except Exception as e:
            logger.error(f"Error processing Alchemy transaction data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def on_ready(self):
        logger.info(f'Logged in as {self.user}')

    def log_transaction(self, data: Dict, timestamp: datetime):
        """Log transaction data to a CSV file."""
        csv_file = r'data/transaction_log.csv'
        
        # Define the fieldnames for the CSV
        fieldnames = [
            'Token Symbol',
            'Buy Type',
            'Token Mint',
            'Wallet Name',
            'Date and Time',
            'Market Cap',
            'Buy Amount in SOL',
            '5m Volume',
            '24h Volume',
            'GMGN Link',
            'Creation Time',
            'Price in SOL',
            'Rugcheck Score',
            'Risk Level',
            'Risk Details'
        ]
        
        # Create the file with headers if it doesn't exist
        if not Path(csv_file).exists():
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                logger.info(f"Created new CSV file: {csv_file}")

        # Format creation time if available
        creation_time = 'N/A'
        if data['token_info'].get('pairCreatedAt'):
            try:
                creation_dt = datetime.fromtimestamp(data['token_info']['pairCreatedAt'] / 1000, tz=timezone.utc)
                creation_time = creation_dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logger.error(f"Error formatting creation time: {e}")
                creation_time = str(data['token_info']['pairCreatedAt'])
        
        # Format Rugcheck data
        rugcheck_score = 'N/A'
        risk_level = 'N/A'
        risk_details = 'N/A'
        
        if 'rugcheck_data' in data and data['rugcheck_data']:
            risks = data['rugcheck_data'].get('risks', [])
            if risks:
                # Calculate overall risk score
                total_score = sum(risk.get('score', 0) for risk in risks)
                rugcheck_score = total_score
                
                # Determine overall risk level
                if total_score >= 7:
                    risk_level = 'HIGH'
                elif total_score >= 4:
                    risk_level = 'MEDIUM'
                else:
                    risk_level = 'LOW'
                
                # Format risk details - ensure no special characters
                risk_details = ' | '.join(f"{risk['name']}: {risk['score']}".encode('ascii', 'ignore').decode() for risk in risks)
        
        # Use timestamp directly without adjustment
        formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Prepare the new row with sanitized data
        new_row = {
            'Token Symbol': str(data['token_info']['symbol']).encode('ascii', 'ignore').decode(),
            'Buy Type': str(data['holder_type']).encode('ascii', 'ignore').decode() if data['holder_type'] else 'N/A',
            'Token Mint': data['token_mint'],
            'Wallet Name': str(get_current_wallet_names().get(data['wallet'], data['wallet'])).encode('ascii', 'ignore').decode(),
            'Date and Time': formatted_timestamp,
            'Market Cap': data['token_info'].get('marketCap', 'N/A'),
            'Buy Amount in SOL': data['sol_amount'],
            '5m Volume': data['token_info'].get('m5Volume', 'N/A'),
            '24h Volume': data['token_info'].get('h24Volume', 'N/A'),
            'GMGN Link': f"https://gmgn.ai/sol/token/IWzYo3Nv_{data['token_mint']}?maker={data['wallet']}",
            'Creation Time': creation_time,
            'Price in SOL': data['sol_amount'] / data['token_amount'] if data['token_amount'] > 0 else 0,
            'Rugcheck Score': rugcheck_score,
            'Risk Level': risk_level,
            'Risk Details': risk_details
        }

        # Append the new row to the CSV file
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(new_row)
            logger.info(f"Logged transaction for {data['token_info']['symbol']}")

    async def on_message(self, message: discord.Message):
        try:
            if message.channel.id != SOURCE_CHANNEL_ID or not message.embeds:
                return
            
            for embed in message.embeds:
                try:
                    timestamp = message.created_at
                    timestamp_field = next((f.value for f in embed.fields if f.name == 'Date'), None)
                    
                    if timestamp_field:
                        try:
                            timestamp = datetime.fromisoformat(timestamp_field.replace('Z', '+00:00'))
                            timestamp = timestamp + timedelta(hours=1)
                        except:
                            timestamp = message.created_at
                            timestamp = timestamp + timedelta(hours=1)
                    
                    parsed_data = await self.parse_helius_embed(embed)
                    if not parsed_data:
                        continue
                    
                    if isinstance(parsed_data.get('wallet'), dict):
                        logger.warning(f"Wallet is a dictionary: {parsed_data['wallet']}")
                        if parsed_data['wallet'] and len(parsed_data['wallet']) > 0:
                            wallet_address = list(parsed_data['wallet'].keys())[0]
                            parsed_data['wallet'] = wallet_address
                            logger.info(f"Extracted wallet address: {wallet_address}")
                        else:
                            logger.error("Could not extract wallet address from dictionary")
                            continue
                    
                    wallet_name = get_current_wallet_names().get(parsed_data['wallet'], parsed_data['wallet'])
                    parsed_data['wallet_name'] = wallet_name
                    
                    # Fetch Rugcheck data for token transactions
                    if not parsed_data.get('is_token_swap', False) and not parsed_data.get('is_usdc', False) and parsed_data.get('is_buy', False):
                        token_mint = parsed_data.get('token_mint')
                        if token_mint:
                            rugcheck_data = await self.get_rugcheck_data(token_mint)
                            if rugcheck_data:
                                parsed_data['rugcheck_data'] = rugcheck_data
                    
                    # Flip is_buy for PUMP_AMM transactions
                    if parsed_data.get('source') == 'PUMP_AMM':
                        parsed_data['is_buy'] = not parsed_data.get('is_buy', False)
                    
                    if parsed_data.get('is_token_swap', False):
                        formatted_embed = await self.format_token_swap_message(parsed_data, timestamp)
                        await self.webhook.send(embed=formatted_embed)
                        continue
                    
                    if parsed_data.get('is_usdc', False):
                        formatted_embed = await self.format_usdc_message(parsed_data, timestamp)
                        await self.webhook.send(embed=formatted_embed)
                        continue
                    
                    formatted_embed = self.format_message(parsed_data, timestamp)
                    await self.webhook.send(embed=formatted_embed)

                    if not parsed_data['is_buy']:
                        logger.info(f"Skipping logging for sell alert from wallet: {parsed_data['wallet']}")
                        continue
                    
                    self.log_transaction(parsed_data, timestamp)
                    
                except Exception as e:
                    logger.error(f"Error processing embed: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"Unexpected error in on_message: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def close(self):
        """Clean up resources."""
        await super().close()
        if self.rugcheck_session:
            await self.rugcheck_session.close()

async def main():
    bot = TokenBot()
    async with bot:
        await bot.start(BOT_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())

