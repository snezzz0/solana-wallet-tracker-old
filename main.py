'''
Bot Manager - Centralized bot orchestration system

This script manages multiple trading and monitoring bots for Solana tokens:
- discordbot: Monitors token transactions and sends alerts to Discord
- ohlcv_collector: Tracks OHLCV data for tokens and generates performance reports
- wallet_swap: Manages wallet recommendations and Helius webhook updates

The manager reads configuration from config.yaml to determine which bots to run,
handles their lifecycle (startup/shutdown), and provides centralized logging.
'''

import asyncio
import logging
import yaml
import sys
import os
from pathlib import Path
from typing import Dict, List
from bots.discordbot import TokenBot
from bots.ohlcv_collector import OHLCVMonitor
from bots.wallet_swap import main as wallet_swap_main
from dotenv import load_dotenv
import schedule
import time
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/main.log')
    ]
)
logger = logging.getLogger(__name__)

class BotManager:
    def __init__(self):
        self.config = self.load_config()
        self.active_bots: Dict[str, asyncio.Task] = {}
        self.bot_classes = {
            'discordbot': TokenBot,
            'ohlcv_collector': OHLCVMonitor,
            'wallet_swap': wallet_swap_main
        }
        self.bot_params = {
            'discordbot': {'token': os.getenv('DISCORD_BOT_TOKEN')},
            'ohlcv_collector': {'api_key': os.getenv('BITQUERY_API_KEY')}
        }
        # Map bot names to their start methods
        self.bot_start_methods = {
            'discordbot': 'start',
            'ohlcv_collector': 'monitor_loop',
            'wallet_swap': None  # Function, not a class
        }

    def load_config(self) -> dict:
        try:
            with open('config/config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            sys.exit(1)

    async def start_bot(self, bot_name: str):
        if bot_name not in self.bot_classes:
            logger.error(f"Unknown bot: {bot_name}")
            return

        if self.config['bots'].get(bot_name, {}).get('enabled', False):
            try:
                logger.info(f"Starting {bot_name}...")
                
                # Check for required parameters
                if bot_name in self.bot_params:
                    missing_params = [k for k, v in self.bot_params[bot_name].items() if not v]
                    if missing_params:
                        logger.error(f"Missing required parameters for {bot_name}: {', '.join(missing_params)}")
                        return
                
                if bot_name == 'wallet_swap':
                    # wallet_swap is a function, not a class
                    task = asyncio.create_task(self.bot_classes[bot_name]())
                else:
                    # Other bots are classes
                    params = self.bot_params.get(bot_name, {})
                    start_method = self.bot_start_methods[bot_name]
                    
                    if bot_name == 'discordbot':
                        # For discordbot, pass token to start() instead of constructor
                        bot_instance = self.bot_classes[bot_name]()
                        task = asyncio.create_task(getattr(bot_instance, start_method)(params['token']))
                    elif bot_name == 'ohlcv_collector':
                        # For ohlcv_collector, pass api_key to constructor
                        bot_instance = self.bot_classes[bot_name](api_key=params['api_key'])
                        task = asyncio.create_task(getattr(bot_instance, start_method)())
                    else:
                        bot_instance = self.bot_classes[bot_name]()
                        task = asyncio.create_task(getattr(bot_instance, start_method)())
                
                self.active_bots[bot_name] = task
                logger.info(f"{bot_name} started successfully")
            except Exception as e:
                logger.error(f"Failed to start {bot_name}: {e}")

    async def stop_bot(self, bot_name: str):
        if bot_name in self.active_bots:
            try:
                logger.info(f"Stopping {bot_name}...")
                self.active_bots[bot_name].cancel()
                await self.active_bots[bot_name]
                del self.active_bots[bot_name]
                logger.info(f"{bot_name} stopped successfully")
            except asyncio.CancelledError:
                logger.info(f"{bot_name} cancelled")
            except Exception as e:
                logger.error(f"Error stopping {bot_name}: {e}")

    async def start_all(self):
        for bot_name in self.bot_classes.keys():
            await self.start_bot(bot_name)

    async def stop_all(self):
        for bot_name in list(self.active_bots.keys()):
            await self.stop_bot(bot_name)

async def run_wallet_swap():
    await wallet_swap_main()  # Call the wallet_swap function

# Schedule the wallet_swap to run every 48 hours
schedule.every(48).hours.do(run_wallet_swap)

async def main():
    # Create logs directory if it doesn't exist
    Path('logs').mkdir(exist_ok=True)

    manager = BotManager()
    try:
        # Only start the 24/7 bots immediately
        await manager.start_bot('discordbot')
        await manager.start_bot('ohlcv_collector')
        
        # Schedule wallet_swap to run every 48 hours, starting 48 hours from now
        schedule.every(48).hours.do(lambda: asyncio.create_task(run_wallet_swap()))
        
        # Keep the main task running
        while True:
            schedule.run_pending()
            await asyncio.sleep(1)  # Sleep to prevent busy-waiting
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await manager.stop_all()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await manager.stop_all()

if __name__ == "__main__":
    asyncio.run(main())
