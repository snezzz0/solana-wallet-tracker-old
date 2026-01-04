'''
This bot collects OHLCV (Open, High, Low, Close, Volume) data for Solana tokens using Bitquery.
It processes transaction logs to identify new tokens and generates performance reports with PNL calculations.
'''

import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
import requests
import csv
import os
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path
import pytz

load_dotenv()

BITQUERY_API_KEY = os.getenv('BITQUERY_API_KEY')

# Set your local timezone
local_tz = pytz.timezone('Europe/Berlin')  # Change to your local timezone

# Define summary CSV path
SUMMARY_CSV_PATH = "data/token_summaries.csv"

# Define summary CSV columns
SUMMARY_CSV_COLUMNS = [
    'token_name', 'mint_address', 'first_holder', 'first_holder_buy_time',
    'market_cap', 'base_price', 'highest_price', 'highest_price_percentage',
    'highest_price_time', 'lowest_price', 'lowest_price_percentage',
    'latest_price', 'latest_price_percentage', 'data_period_from', 'data_period_to',
    'token_buyers', 'gmgn_link'
]

# Create data directory if it doesn't exist
os.makedirs(os.path.dirname(SUMMARY_CSV_PATH), exist_ok=True)

# Initialize the summary CSV file if it doesn't exist
if not os.path.exists(SUMMARY_CSV_PATH):
    with open(SUMMARY_CSV_PATH, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_CSV_COLUMNS)
        writer.writeheader()

class SolanaOHLCVFetcher:
    def __init__(self, api_key, output_dir="data/ohlcv_data"):
        """
        Initialize the OHLCV fetcher with your Bitquery API key
        
        Args:
            api_key: Your Bitquery API key
            output_dir: Directory to save CSV files (default: data/ohlcv_data)
        """
        self.api_key = api_key
        self.bitquery_endpoint = "https://streaming.bitquery.io/eap"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    async def fetch_ohlcv_data(self, mint_address, start_datetime, end_datetime, quote_token="So11111111111111111111111111111111111111112"):
        """
        Fetch OHLCV data from Bitquery for a specific token and timeframe
        
        Args:
            mint_address: The Solana token mint address
            start_datetime: The start date and time (ISO format: YYYY-MM-DDTHH:MM:SS)
            end_datetime: The end date and time (ISO format: YYYY-MM-DDTHH:MM:SS)
            quote_token: The quote token mint address (default is SOL)
            
        Returns:
            Dictionary with OHLCV data
        """
        try:
            # Parse the datetimes
            start_dt = datetime.fromisoformat(start_datetime)
            end_dt = datetime.fromisoformat(end_datetime)
            
            # Format datetime with 'Z' suffix for UTC timezone
            start_time = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_time = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Log the time range for debugging
            logger.info(f"Fetching OHLCV data for {mint_address} from {start_time} to {end_time}")
            
            # Modified query with timeframe of 2 hours
            query = """
            {
              Solana(dataset: archive) {
                DEXTradeByTokens(
                  orderBy: {ascendingByField: "Block_Time"}
                  where: {
                    Trade: {
                      Currency: {MintAddress: {is: "%s"}}
                      Side: {Currency: {MintAddress: {is: "%s"}}}
                      PriceAsymmetry: {lt: 0.1}
                    }
                    Block: {Time: {since: "%s", till: "%s"}}
                  }
                ) {
                  Block {
                    Time(interval: {in: minutes, count: 1})
                  }
                  volume: sum(of: Trade_Amount)
                  Trade {
                    high: Price(maximum: Trade_Price)
                    low: Price(minimum: Trade_Price)
                    open: Price(minimum: Block_Slot)
                    close: Price(maximum: Block_Slot)
                  }
                  count
                }
              }
            }
            """ % (mint_address, quote_token, start_time, end_time)
            
            # Log the formatted query for debugging
            logger.info(f"Sending query with timestamps: since={start_time}, till={end_time}")
            
            # Execute the query
            response = requests.post(
                self.bitquery_endpoint,
                headers=self.headers,
                json={
                    "query": query,
                    "variables": "{}"
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched OHLCV data for {mint_address}")
                return self._process_ohlcv_data(data, mint_address)
            else:
                logger.error(f"Error fetching data: {response.status_code} - {response.text}")
                return {"status": "error", "error": f"Failed to fetch data: {response.status_code}"}
                
        except Exception as e:
            logger.error(f"Exception occurred while fetching OHLCV data: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def _process_ohlcv_data(self, raw_data, mint_address):
        """
        Process the raw data from Bitquery into a structured OHLCV format
        """
        try:
            ohlcv_data = []
            
            # Add debug logging to see the raw response
            logger.debug(f"Raw data received: {raw_data}")
            
            # Check if we have valid data
            if ('data' in raw_data and 
                'Solana' in raw_data['data'] and 
                'DEXTradeByTokens' in raw_data['data']['Solana'] and
                raw_data['data']['Solana']['DEXTradeByTokens'] is not None):
                
                candles = raw_data['data']['Solana']['DEXTradeByTokens']
                
                # Check if we have any candles
                if not candles:
                    logger.info(f"No trading data found for {mint_address} in the specified timeframe")
                    return {
                        'status': 'success',
                        'mint_address': mint_address,
                        'data': []
                    }
                
                for candle in candles:
                    # Add null checks for each field
                    if (candle.get('Block') and 
                        candle.get('Trade') and 
                        candle['Block'].get('Time')):
                        
                        try:
                            ohlcv_data.append({
                                'timestamp': candle['Block']['Time'],
                                'open': float(candle['Trade'].get('open', 0)),
                                'high': float(candle['Trade'].get('high', 0)),
                                'low': float(candle['Trade'].get('low', 0)),
                                'close': float(candle['Trade'].get('close', 0)),
                                'volume': float(candle.get('volume', 0)),
                                'count': int(candle.get('count', 0))
                            })
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Error processing candle data: {e}, candle: {candle}")
                            continue
                
                return {
                    'status': 'success',
                    'mint_address': mint_address,
                    'data': ohlcv_data
                }
            else:
                logger.warning(f"Invalid or empty data structure for {mint_address}: {raw_data}")
                return {
                    'status': 'success',
                    'mint_address': mint_address,
                    'data': []
                }
                
        except Exception as e:
            logger.error(f"Error processing OHLCV data: {str(e)}")
            logger.error(f"Raw data that caused error: {raw_data}")
            return {
                'status': 'error',
                'message': f'Error processing data: {str(e)}',
                'mint_address': mint_address,
                'data': []
            }


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OHLCVMonitor:
    def __init__(self, api_key):
        """
        Initialize the monitor with a Bitquery API key
        """
        if not api_key:
            raise ValueError("Bitquery API key is required")
        self.api_key = api_key
        self.fetcher = SolanaOHLCVFetcher(api_key=api_key)
        self.processed_tokens = set()
        self.active_tasks = {}
        self.csv_file = 'data/transaction_log.csv'
        self.last_processed_row = 0
        
        # Expected CSV columns
        self.expected_columns = [
            'Token Symbol', 'Buy Type', 'Token Mint', 'Wallet Name', 
            'Date and Time', 'Buy Amount in SOL', 'Price in SOL',
            'Market Cap', 'GMGN Link', 
        ]
    
    def get_token_buyers_info(self, mint_address):
        """
        Retrieve all buyers' information for a specific token from the CSV file
        
        Args:
            mint_address: The Solana token mint address
            
        Returns:
            Dictionary with first holder info and list of all buyers
        """
        try:
            # Check if file exists
            if not Path(self.csv_file).exists():
                logger.warning(f"Transaction log file not found: {self.csv_file}")
                return None

            # Read the CSV file with error handling
            try:
                df = pd.read_csv(self.csv_file, on_bad_lines='skip')
            except Exception as e:
                logger.error(f"Error reading CSV file: {str(e)}")
                return None
            
            # Make sure all required columns exist
            for col in self.expected_columns:
                if col not in df.columns:
                    logger.warning(f"Missing expected column in CSV: {col}")
                    # Try to find alternative column names or continue with available columns
            
            # Filter for transactions related to this token
            token_transactions = df[df['Token Mint'] == mint_address]
            
            if token_transactions.empty:
                logger.warning(f"No transactions found for token: {mint_address}")
                return None
            
            # Get first holder information
            first_holder_rows = token_transactions[token_transactions['Buy Type'] == 'FIRST_HOLDER']
            if first_holder_rows.empty:
                logger.warning(f"No FIRST_HOLDER transaction found for token: {mint_address}")
                first_holder_info = None
            else:
                # Get the first recorded FIRST_HOLDER transaction
                first_holder_row = first_holder_rows.iloc[0]
                first_holder_info = {
                    'wallet_name': first_holder_row.get('Wallet Name', 'Unknown'),
                    'date_time': first_holder_row.get('Date and Time', 'Unknown'),
                    'price_sol': first_holder_row.get('Price in SOL', 0),
                    'market_cap': first_holder_row.get('Market Cap', 0),
                }
            
            # Get all other buyers (both NEW_HOLDER and EXISTING_HOLDER)
            other_buyers = []
            other_holder_rows = token_transactions[token_transactions['Buy Type'] != 'FIRST_HOLDER']
            
            for _, row in other_holder_rows.iterrows():
                buyer_info = {
                    'buy_type': row.get('Buy Type', 'Unknown'),
                    'wallet_name': row.get('Wallet Name', 'Unknown'),
                    'date_time': row.get('Date and Time', 'Unknown'),
                    'price_sol': row.get('Price in SOL', 0),
                }
                other_buyers.append(buyer_info)
            
            # Get token name from the first transaction
            token_name = token_transactions.iloc[0].get('Token Symbol', 'Unknown')
            
            return {
                'token_name': token_name,
                'mint_address': mint_address,
                'first_holder': first_holder_info,
                'other_buyers': other_buyers
            }
            
        except Exception as e:
            logger.error(f"Error retrieving token buyers info: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
            
    def save_to_csv(self, ohlcv_data, token_info=None):
        """
        Save OHLCV data to a CSV file and create a detailed summary
        
        Args:
            ohlcv_data: Processed OHLCV data dictionary
            token_info: Additional token information from transaction log
        
        Returns:
            Boolean indicating success/failure
        """
        try:
            mint_address = ohlcv_data['mint_address']
            data_points = ohlcv_data['data']
            
            if not data_points:
                logger.info(f"No data points to save for {mint_address} - skipping file creation")
                return True
            
            # Check if files already exist
            base_filename = os.path.join(self.fetcher.output_dir, f"{mint_address}")
            csv_filename = f"{base_filename}.csv"
            
            if os.path.exists(csv_filename):
                logger.info(f"CSV file already exists for {mint_address} - skipping generation")
                return True
            
            # Write data to CSV
            with open(csv_filename, 'w', newline='') as csvfile:
                fieldnames = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'count']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for candle in data_points:
                    writer.writerow({
                        'timestamp': candle['timestamp'],  # Fixed: Use 'timestamp' key directly
                        'open': candle['open'],
                        'high': candle['high'],
                        'low': candle['low'],
                        'close': candle['close'],
                        'volume': candle['volume'],
                        'count': candle['count']
                    })
            
            logger.info(f"Successfully saved OHLCV data to {csv_filename}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving data to CSV: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    async def process_new_tokens(self):
        """
        Process new tokens from the transaction log CSV file
        """
        try:
            # Check if file exists
            if not Path(self.csv_file).exists():
                logger.warning(f"Transaction log file not found: {self.csv_file}")
                return

            # Read the CSV file with error handling
            try:
                df = pd.read_csv(self.csv_file, on_bad_lines='skip')  # Skip bad lines
            except Exception as e:
                logger.error(f"Error reading CSV file: {str(e)}")
                return
            
            # Ensure all expected columns exist
            missing_columns = [col for col in self.expected_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing expected columns in CSV: {missing_columns}")
                # Continue with available columns
            
            # Filter for FIRST_HOLDER transactions only
            first_holder_df = df[df['Buy Type'] == 'FIRST_HOLDER']
            
            # Process each unique mint address for tokens with FIRST_HOLDER transactions
            for _, row in first_holder_df.iterrows():
                mint_address = row['Token Mint']
                date_time = row.get('Date and Time')
                
                # Skip already processed tokens
                if mint_address in self.processed_tokens:
                    continue
                    
                # Skip tokens that already have tasks scheduled
                if mint_address in self.active_tasks and not self.active_tasks[mint_address].done():
                    continue
                
                logger.info(f"Found new token: {mint_address}, First holder time: {date_time}")
                
                # Create and schedule a task for this token
                task = asyncio.create_task(self.schedule_fetch_for_token(mint_address, date_time))
                self.active_tasks[mint_address] = task
                
        except Exception as e:
            logger.error(f"Error processing transaction log: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def schedule_fetch_for_token(self, mint_address, timestamp):
        """
        Schedule an OHLCV fetch for a specific token with 3-hour wait and 4-hour data window
        Always ensures that data fetching starts no earlier than the token creation time
        """
        try:
            # Check if files already exist
            base_filename = os.path.join(self.fetcher.output_dir, f"{mint_address}")
            csv_filename = f"{base_filename}.csv"
            summary_filename = f"{base_filename}_summary.txt"
            
            if os.path.exists(csv_filename) and os.path.exists(summary_filename):
                logger.info(f"Files already exist for {mint_address} - skipping fetch")
                return
            
            # Get the first holder row and creation time from transaction log
            try:
                # Use error handling when reading the CSV file
                df = pd.read_csv(self.csv_file, on_bad_lines='skip')
                token_transactions = df[df['Token Mint'] == mint_address]
                first_holder_rows = token_transactions[token_transactions['Buy Type'] == 'FIRST_HOLDER']
                
                if first_holder_rows.empty:
                    logger.warning(f"No FIRST_HOLDER transaction found for token: {mint_address}")
                    return
                    
                first_holder_row = first_holder_rows.iloc[0]
            except Exception as e:
                logger.error(f"Error reading CSV file for token {mint_address}: {str(e)}")
                # Continue with default values
                first_holder_row = {}
            
            # Parse the first holder timestamp
            try:
                reference_dt = datetime.fromisoformat(timestamp)
                if reference_dt.tzinfo is not None:
                    reference_dt = reference_dt.replace(tzinfo=None)
            except ValueError:
                logger.info(f"Parsing timestamp without timezone: {timestamp}")
                if '+' in timestamp:
                    timestamp = timestamp.split('+')[0]
                reference_dt = datetime.fromisoformat(timestamp)
            
            # Parse creation time if available - this is critical for the new logic
            creation_dt = None
            creation_time = first_holder_row.get('Creation Time')
            if creation_time and creation_time != 'N/A':
                try:
                    creation_dt = datetime.fromisoformat(creation_time)
                except (ValueError, TypeError):
                    try:
                        # Try parsing as Unix timestamp (milliseconds)
                        creation_dt = datetime.fromtimestamp(int(creation_time) / 1000)
                    except:
                        logger.warning(f"Could not parse creation time: {creation_time}")
                        creation_dt = None
            
            # Calculate target time (3 hours after first holder)
            target_dt = reference_dt + timedelta(hours=3)
            
            # Calculate planned start time (1 hour before first holder)
            planned_start_dt = reference_dt - timedelta(hours=1)
            
            # IMPORTANT: Always ensure we don't query data from before token creation
            if creation_dt:
                # Use the creation time as the start time if it's later than our planned start
                start_dt = max(planned_start_dt, creation_dt)
                
                if creation_dt > planned_start_dt:
                    logger.info(f"Token created at {creation_dt}, which is after our planned start time of {planned_start_dt}")
                    logger.info(f"Using token creation time as start time to avoid querying non-existent data")
                else:
                    logger.info(f"Using standard time window: 1 hour before first holder (token already existed)")
            else:
                # If we don't have creation time, use the standard window but log a warning
                start_dt = planned_start_dt
                logger.warning(f"Token creation time unknown for {mint_address}. Using standard window, which may include pre-creation data.")
            
            # Calculate wait time
            current_dt = datetime.now()
            wait_seconds = max(0, (target_dt - current_dt).total_seconds())
            
            if wait_seconds > 0:
                logger.info(f"Scheduling fetch for {mint_address}")
                logger.info(f"First holder time: {reference_dt}")
                logger.info(f"Creation time: {creation_dt if creation_dt else 'Unknown'}")
                logger.info(f"Will fetch data from {start_dt} to {target_dt}")
                logger.info(f"Waiting {wait_seconds:.2f} seconds (approximately {wait_seconds/3600:.1f} hours)")
                await asyncio.sleep(wait_seconds)
            else:
                logger.info(f"Target time {target_dt} already passed, fetching immediately")
            
            # Check again after wait in case files were created while waiting
            if os.path.exists(csv_filename) and os.path.exists(summary_filename):
                logger.info(f"Files created while waiting for {mint_address} - skipping fetch")
                return
            
            # Format times for the API call
            start_datetime = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
            end_datetime = target_dt.strftime("%Y-%m-%dT%H:%M:%S")
            
            logger.info(f"Executing OHLCV fetch for {mint_address}")
            logger.info(f"Fetching data from {start_datetime} to {end_datetime}")
            
            result = await self.fetcher.fetch_ohlcv_data(mint_address, start_datetime, end_datetime)
            
            # Get token information for the summary
            token_info = self.get_token_buyers_info(mint_address)
            
            if result['status'] == 'success':
                self.save_to_csv(result, token_info)
                logger.info(f"Successfully fetched and saved OHLCV data for {mint_address}")
                
                # Send PNL report to Discord webhook
                await self.send_pnl_report_to_discord(mint_address, result, first_holder_row)
            else:
                logger.error(f"Failed to fetch OHLCV data for {mint_address}")
            
        except Exception as e:
            logger.error(f"Error processing token {mint_address}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.processed_tokens.add(mint_address)
            if mint_address in self.active_tasks:
                del self.active_tasks[mint_address]
    
    async def monitor_loop(self):
        """
        Continuously monitor for new tokens
        """
        logger.info("Monitor loop started, checking for new tokens...")
        while True:
            try:
                await self.process_new_tokens()
                
                # Brief pause before checking again
                await asyncio.sleep(10)  # Check for new tokens every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in monitor loop: {str(e)}")
                # Log the full exception details for debugging
                import traceback
                logger.error(traceback.format_exc())
                await asyncio.sleep(30)  # Longer pause if there was an error

    async def send_pnl_report_to_discord(self, mint_address, ohlcv_data, first_holder_row):
        """
        Send a PNL report to Discord webhook after fetching OHLCV data
        and log it locally in a summary file
        
        Args:
            mint_address: The token mint address
            ohlcv_data: The OHLCV data dictionary returned from fetch_ohlcv_data
            first_holder_row: DataFrame row containing first holder information
        """
        try:
            # Get Discord webhook URL from environment
            discord_webhook_url = os.getenv('DISCORD_WEBHOOK_PNL')
            if not discord_webhook_url:
                logger.error("DISCORD_PNL webhook URL not found in environment variables")
                return
            
            # Extract data points
            data_points = ohlcv_data['data']
            if not data_points or len(data_points) == 0:
                logger.warning(f"No OHLCV data points available for PNL calculation for {mint_address}")
                return
            
            # Get token information
            token_info = self.get_token_buyers_info(mint_address)
            token_name = token_info['token_name'] if token_info else "Unknown Token"
            
            # Get base price from first holder transaction
            base_price = float(first_holder_row.get('Price in SOL', 0))
            if base_price == 0:
                logger.warning(f"First holder price is zero or not available for {mint_address}")
                return
            
            # Get market cap from first holder transaction
            market_cap = first_holder_row.get('Market Cap', 0)
            try:
                market_cap_float = float(market_cap)
                formatted_market_cap = f"${int(market_cap_float)}"  # No decimals with $ prefix
            except (ValueError, TypeError):
                formatted_market_cap = "Unknown"
                logger.warning(f"Could not parse market cap value: {market_cap}")
            
            # Get the highest price from OHLCV data
            highest_price_point = max(data_points, key=lambda x: x['high'])
            highest_price = float(highest_price_point['high'])
            highest_price_time = highest_price_point['timestamp']
            
            # Get the lowest price from OHLCV data
            lowest_price_point = min(data_points, key=lambda x: x['low'])
            lowest_price = float(lowest_price_point['low'])
            
            # Format timestamp to be more readable with UTC+1 adjustment
            try:
                # Parse ISO format timestamp and adjust to local timezone
                highest_time_dt = datetime.fromisoformat(highest_price_time.replace('Z', '+00:00')).astimezone(local_tz)
                formatted_highest_time = highest_time_dt.strftime("%b %d, %Y %H:%M:%S")
                
                start_time_dt = datetime.fromisoformat(data_points[0]['timestamp'].replace('Z', '+00:00')).astimezone(local_tz)
                end_time_dt = datetime.fromisoformat(data_points[-1]['timestamp'].replace('Z', '+00:00')).astimezone(local_tz)
                formatted_start_time = start_time_dt.strftime("%b %d, %Y %H:%M:%S")
                formatted_end_time = end_time_dt.strftime("%b %d, %Y %H:%M:%S")
            except (ValueError, AttributeError):
                # Fallback if timestamp parsing fails
                formatted_highest_time = highest_price_time
                formatted_start_time = data_points[0]['timestamp']
                formatted_end_time = data_points[-1]['timestamp']
            
            # Calculate PNL metrics based on highest price
            pnl_percentage = ((highest_price - base_price) / base_price) * 100 if base_price > 0 else 0
            
            # Calculate lowest price metrics
            lowest_pnl_percentage = ((lowest_price - base_price) / base_price) * 100 if base_price > 0 else 0
            
            # Calculate latest price metrics for comparison
            latest_price = float(data_points[-1]['close'])
            latest_pnl_percentage = ((latest_price - base_price) / base_price) * 100 if base_price > 0 else 0
            
            # Get wallet name and first holder buy time from first holder row
            wallet_name = first_holder_row.get('Wallet Name', 'Unknown')
            first_holder_time = first_holder_row.get('Date and Time', 'Unknown')  # Assuming this is the buy time
            
            # Get GMGN link from first holder row
            gmgn_link = first_holder_row.get('GMGN Link', 'N/A')
            
            # Get list of wallet names and buy times that bought the token
            buyer_wallets = []
            if token_info and 'other_buyers' in token_info and token_info['other_buyers']:
                for buyer in token_info['other_buyers']:
                    wallet_name = buyer.get('wallet_name', 'Unknown')
                    buy_time = buyer.get('date_time', 'Unknown')  # Assuming this is the buy time
                    buyer_wallets.append(f"{wallet_name} (Buy Time: {buy_time})")  # Format wallet name with buy time
            
            # Remove duplicates while preserving order
            seen = set()
            buyer_wallets = [wallet for wallet in buyer_wallets if not (wallet in seen or seen.add(wallet))]
            
            # Format the message for Discord
            emoji = "🚀" if pnl_percentage >= 0 else "📉"
            title = f"{emoji} Token Performance: {token_name} ({mint_address[:8]}...)"
            
            embed = {
                "title": title,
                "color": 65280 if pnl_percentage >= 0 else 16711680,  # Green or Red
                "fields": [
                    {
                        "name": "🔍 Token Information",
                        "value": f"**Name:** {token_name}\n**Mint:** {mint_address}\n**First Holder:** {wallet_name}\n**First Holder Buy Time:** {first_holder_time}\n**Market Cap:** {formatted_market_cap}\n**GMGN:** [View Token]({gmgn_link})",
                        "inline": False
                    },
                    {
                        "name": "📊 Performance",
                        "value": f"**Max %:** {pnl_percentage:.2f}% (at {formatted_highest_time})\n**Min %:** {lowest_pnl_percentage:.2f}%\n**Current %:** {latest_pnl_percentage:.2f}%",
                        "inline": True
                    },
                    {
                        "name": "⏰ Time Period",
                        "value": f"From {formatted_start_time} to {formatted_end_time}",
                        "inline": True
                    }
                ],
                "timestamp": (datetime.now(local_tz)).isoformat()  # Adjusted to -1 hour
            }
            
            # Add list of buyers if available
            if buyer_wallets:
                buyers_text = "\n".join([f"👤 {wallet}" for wallet in buyer_wallets[:10]])  # Limit to 10 wallets
                if len(buyer_wallets) > 10:
                    buyers_text += f"\n...and {len(buyer_wallets) - 10} more"
                
                embed["fields"].append({
                    "name": "🛒 Token Buyers",
                    "value": buyers_text,
                    "inline": False
                })
            else:
                embed["fields"].append({
                    "name": "🛒 Token Buyers",
                    "value": "No additional buyers found",
                    "inline": False
                })
            
            payload = {
                "embeds": [embed]
            }
            
            # Save data to the summary CSV file
            token_buyers_str = ", ".join(buyer_wallets[:5])  # Limit to first 5 buyers for CSV
            if len(buyer_wallets) > 5:
                token_buyers_str += f" and {len(buyer_wallets) - 5} more"
            
            # Prepare row for CSV
            summary_row = {
                'token_name': token_name,
                'mint_address': mint_address,
                'first_holder': wallet_name,
                'first_holder_buy_time': first_holder_time,
                'market_cap': formatted_market_cap,
                'base_price': f"{base_price:.8f}",
                'highest_price': f"{highest_price:.8f}",
                'highest_price_percentage': f"{pnl_percentage:.2f}",
                'highest_price_time': formatted_highest_time,
                'lowest_price': f"{lowest_price:.8f}",
                'lowest_price_percentage': f"{lowest_pnl_percentage:.2f}",
                'latest_price': f"{latest_price:.8f}",
                'latest_price_percentage': f"{latest_pnl_percentage:.2f}",
                'data_period_from': formatted_start_time,
                'data_period_to': formatted_end_time,
                'token_buyers': token_buyers_str,
                'gmgn_link': gmgn_link
            }
            
            # Write to CSV file
            with open(SUMMARY_CSV_PATH, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=SUMMARY_CSV_COLUMNS)
                writer.writerow(summary_row)
            
            logger.info(f"Successfully added token summary for {mint_address} to {SUMMARY_CSV_PATH}")
            
            # For backwards compatibility, also create the individual summary file
            summary_filename = os.path.join(self.fetcher.output_dir, f"{mint_address}_summary.txt")
            with open(summary_filename, 'w') as summary_file:
                summary_file.write(f"Token Performance Summary: {token_name}\n")
                summary_file.write("=" * 50 + "\n\n")
                
                summary_file.write("Token Information:\n")
                summary_file.write(f"- Name: {token_name}\n")
                summary_file.write(f"- Mint Address: {mint_address}\n")
                summary_file.write(f"- First Holder: {wallet_name}\n")
                summary_file.write(f"- First Holder Buy Time: {first_holder_time}\n")  # Log first holder buy time
                summary_file.write(f"- Market Cap: {formatted_market_cap}\n")
                summary_file.write(f"- GMGN Link: {gmgn_link}\n\n")
                
                summary_file.write("Performance Metrics:\n")
                summary_file.write(f"- Base Price: {base_price:.8f} SOL\n")
                summary_file.write(f"- Highest Price: {highest_price:.8f} SOL ({pnl_percentage:.2f}% gain) at {formatted_highest_time}\n")
                summary_file.write(f"- Lowest Price: {lowest_price:.8f} SOL ({lowest_pnl_percentage:.2f}% change)\n")
                summary_file.write(f"- Latest Price: {latest_price:.8f} SOL ({latest_pnl_percentage:.2f}% change)\n\n")
                
                summary_file.write("Data Period:\n")
                summary_file.write(f"- From: {formatted_start_time}\n")
                summary_file.write(f"- To: {formatted_end_time}\n\n")
                
                if buyer_wallets:
                    summary_file.write("Token Buyers:\n")
                    for i, wallet in enumerate(buyer_wallets, 1):
                        summary_file.write(f"{i}. {wallet}\n")
                else:
                    summary_file.write("Token Buyers: No additional buyers found\n")
            
            logger.info(f"Successfully wrote token summary to {summary_filename}")
            
            # Add delay to avoid rate limiting
            await asyncio.sleep(1.5)  # 1.5 seconds delay
            
            # Send to Discord webhook
            async with aiohttp.ClientSession() as session:
                async with session.post(discord_webhook_url, json=payload) as response:
                    if response.status != 204:
                        response_text = await response.text()
                        logger.error(f"Failed to send PNL report to Discord. Status: {response.status}, Response: {response_text}")
                    else:
                        logger.info(f"Successfully sent PNL report to Discord for {mint_address}")
        
        except Exception as e:
            logger.error(f"Error sending PNL report to Discord for {mint_address}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

async def main():
    # Get API key from environment and verify it exists
    api_key = os.getenv('BITQUERY_API_KEY')
    if not api_key:
        logger.error("BITQUERY_API_KEY not found in environment variables")
        return
    
    logger.info("Starting OHLCV Monitor with API key...")
    monitor = OHLCVMonitor(api_key)
    
    try:
        await monitor.monitor_loop()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())