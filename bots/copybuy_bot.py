import discord
from discord.ext import commands
import asyncio
from telethon.sync import TelegramClient
from telethon import errors
from telethon.tl.types import InputPeerChannel, InputPeerUser, InputPeerChat
import re
from decimal import Decimal

# Configuration
MIN_MARKET_CAP = 90000
MAX_MARKET_CAP = 2000000

class DiscordTelegramBot:
    def __init__(self, discord_token, tg_api_id, tg_api_hash, tg_phone, destination_channel_id):
        # Discord setup
        intents = discord.Intents.default()
        intents.message_content = True
        self.bot = commands.Bot(command_prefix='!', intents=intents)
        self.discord_token = discord_token
        
        # Update Telegram setup to use config directory
        self.config_dir = "config"  # Set config directory path
        self.tg_api_id = tg_api_id
        self.tg_api_hash = tg_api_hash
        self.tg_phone = tg_phone
        self.destination_channel_id = destination_channel_id
        self.tg_client = None
        self.entity = None
        
        # Set up Discord event handlers
        @self.bot.event
        async def on_ready():
            print(f'Discord bot is ready: {self.bot.user.name}')
            
        @self.bot.event
        async def on_message(message):
            if message.webhook_id is not None and "Swap Transaction Alert" in message.content:
                try:
                    # Check if it's a buy order
                    is_buy = "bought" in message.content.lower()
                    
                    # Parse token mint
                    mint_match = re.search(r'Token Mint: ([A-Za-z0-9]+)', message.content)
                    token_mint = mint_match.group(1) if mint_match else None
                    
                    # Parse market cap
                    market_cap_match = re.search(r'Market Cap: \$([0-9,.]+)', message.content)
                    if market_cap_match:
                        market_cap = float(market_cap_match.group(1).replace(',', ''))
                    else:
                        market_cap = 0  # Set to 0 if not found
                        
                    print(f"Detected webhook message - Buy: {is_buy}, Token: {token_mint}, Market Cap: ${market_cap:,.2f}")
                    
                    # Check all conditions: buy order, valid mint, and market cap in range
                    if (token_mint not in self.processed_tokens and 
                        MIN_MARKET_CAP <= market_cap <= MAX_MARKET_CAP):
                        await self.forward_to_telegram(token_mint)
                        print(f"Message forwarded - Market cap ${market_cap:,.2f} within range")
                    else:
                        if not (MIN_MARKET_CAP <= market_cap <= MAX_MARKET_CAP):
                            print(f"Skipped - Market cap ${market_cap:,.2f} outside target range")
                        
                except Exception as e:
                    print(f"Error processing webhook message: {e}")
                    print(f"Message content: {message.content}")

            await self.bot.process_commands(message)

    async def resolve_entity(self):
        try:
            dialogs = await self.tg_client.get_dialogs()
            for dialog in dialogs:
                if dialog.id == self.destination_channel_id:
                    self.entity = dialog.entity
                    print(f"Found entity: {dialog.name}")
                    return
                
            if not self.entity:
                print("Could not find the channel in your dialogs. Please check the channel ID.")
                print("Available channels:")
                for dialog in dialogs:
                    print(f"ID: {dialog.id}, Name: {dialog.name}")
                raise Exception("Channel not found in dialogs")
                
        except Exception as e:
            print(f"Error resolving entity: {e}")
            raise

    async def init_telegram(self):
        # Update session path to use config directory
        session_path = f"{self.config_dir}/session_{self.tg_phone}"
        self.tg_client = TelegramClient(session_path, self.tg_api_id, self.tg_api_hash)
        await self.tg_client.connect()

        if not await self.tg_client.is_user_authorized():
            await self.tg_client.send_code_request(self.tg_phone)
            try:
                code = input('Enter the Telegram code: ')
                await self.tg_client.sign_in(self.tg_phone, code)
            except errors.rpcerrorlist.SessionPasswordNeededError:
                password = input('Two-step verification is enabled. Enter your password: ')
                await self.tg_client.sign_in(password=password)

        await self.resolve_entity()
        print("Telegram client initialized successfully!")

    async def forward_to_telegram(self, mint_address):
        if self.tg_client and self.entity:
            try:
                await self.tg_client.send_message(self.entity, mint_address)
                print(f"Mint address forwarded to Telegram: {mint_address}")
            except Exception as e:
                print(f"Error forwarding to Telegram: {e}")
        else:
            print("Telegram client not initialized or entity not found!")

    async def start(self):
        await self.init_telegram()
        try:
            await self.bot.start(self.discord_token)
        except Exception as e:
            print(f"Error starting Discord bot: {e}")
        finally:
            if self.tg_client:
                await self.tg_client.disconnect()

def read_credentials():
    try:
        # Update credentials path to use config directory
        credentials_path = "config/credentials.txt"
        with open(credentials_path, "r") as file:
            lines = file.readlines()
            return {
                'tg_api_id': lines[0].strip(),
                'tg_api_hash': lines[1].strip(),
                'tg_phone': lines[2].strip(),
                'discord_token': lines[3].strip(),
                'destination_channel_id': int(lines[4].strip())
            }
    except FileNotFoundError:
        print(f"Credentials file not found at {credentials_path}")
        return None

def write_credentials(creds):
    # Update credentials path to use config directory
    credentials_path = "config/credentials.txt"
    with open(credentials_path, "w") as file:
        file.write(f"{creds['tg_api_id']}\n")
        file.write(f"{creds['tg_api_hash']}\n")
        file.write(f"{creds['tg_phone']}\n")
        file.write(f"{creds['discord_token']}\n")
        file.write(f"{creds['destination_channel_id']}\n")

async def main():
    creds = read_credentials()
    
    if not creds:
        creds = {
            'tg_api_id': input("Enter Telegram API ID: "),
            'tg_api_hash': input("Enter Telegram API Hash: "),
            'tg_phone': input("Enter your phone number: "),
            'discord_token': input("Enter Discord bot token: "),
            'destination_channel_id': int(input("Enter destination Telegram channel ID: "))
        }
        write_credentials(creds)

    bot = DiscordTelegramBot(
        creds['discord_token'],
        creds['tg_api_id'],
        creds['tg_api_hash'],
        creds['tg_phone'],
        creds['destination_channel_id']
    )
    
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())