'''
Wallet Swap Bot - Automated wallet management and optimization system

This bot manages and optimizes wallet tracking by:
1. Analyzing wallet performance using Dune Analytics data
2. Identifying inactive wallets and those with poor ROI
3. Finding suitable replacement wallets based on transaction patterns
4. Automatically updating Helius webhooks with new wallet addresses
5. Generating detailed performance reports and recommendations

The bot integrates with multiple services:
- Dune Analytics for wallet performance data
- Helius API for webhook management
- Discord for reporting and notifications
'''

import json
import logging
import csv
import os
import glob
import time
import random  
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv
import subprocess
import yaml
import sys
import traceback
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/wallet_swap.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add project root to Python path
project_root = str(Path(__file__).parent.parent.absolute())
if project_root not in sys.path:
    sys.path.append(project_root)
    print(f"Added {project_root} to Python path")

try:
    from scripts.gmgn_hybrid import CloudflareBypass, process_html_with_bs4
    from scripts.wallet_transaction_filter import WalletFilter
    from scripts.wallet_extractor import SolscanWalletExtractor
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(f"sys.path: {sys.path}")
    raise

load_dotenv()

# env variables
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_WALLET_REPORT')
DUNE_API_KEY = os.getenv('DUNE_API_KEY')
QUERY_ID = os.getenv('DUNE_QUERY_ID')

# Configuration
QUERY_WAIT_TIME = 60  # Seconds to wait for query execution
ROI_THRESHOLD = 0.5   # Threshold for bad ROI (wallets below this are flagged)
# not implemented MAX_RECOMMENDATIONS = 5  # Max number of swap recommendations per wallet

def load_wallet_names():
    """Load wallet names from wallet_names.json"""
    try:
        with open('config/wallet_names.json', 'r') as f:
            wallet_names = json.load(f)
            return wallet_names
    except Exception as e:
        logger.error(f"Error loading wallet names: {e}")
        return {}

def load_wallet_sections():
    """Load wallet sections from wallet_sections.txt"""
    sections = defaultdict(list)
    current_section = None
    
    try:
        with open('config/wallet_sections.txt', 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('Section'):
                    current_section = line
                    continue
                
                if current_section and ':' in line:
                    wallet_address, wallet_name = line.split(':', 1)
                    sections[current_section].append(wallet_address)
    except Exception as e:
        logger.error(f"Error loading wallet sections: {e}")
    
    return sections

def get_active_wallets():
    """Extract all active wallets from transaction_log.csv"""
    active_wallets = set()
    try:
        with open('data/transaction_log.csv', 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                try:
                    # Find which column has the wallet addresses
                    for key in row:
                        if not key or not row[key]:
                            continue
                            
                        key = str(key).strip()
                        value = str(row[key]).strip()
                        
                        if key == 'Wallet Name':
                            wallet_name = value
                        if 'GMGN Link' in key and value:
                            # Extract wallet address from GMGN link
                            if 'maker=' in value:
                                wallet_address = value.split('maker=')[1].strip()
                                if wallet_address:  # Only add if we got a valid address
                                    active_wallets.add(wallet_address)
                                    logger.debug(f"Added active wallet: {wallet_address}")
                except Exception as e:
                    logger.error(f"Error processing row in transaction log: {str(e)}")
                    continue
                    
        logger.info(f"Found {len(active_wallets)} active wallets")
        return active_wallets
        
    except FileNotFoundError:
        logger.error("Transaction log file not found at data/transaction_log.csv")
        return set()
    except Exception as e:
        logger.error(f"Error loading transaction log: {str(e)}")
        return set()

def find_inactive_wallets():
    """Find inactive wallets by comparing wallet_names with active wallets"""
    wallet_names = load_wallet_names()
    active_wallets = get_active_wallets()
    
    # Find wallets that exist in wallet_names but not in active_wallets
    inactive_wallets = {}
    for wallet_address, wallet_name in wallet_names.items():
        if wallet_address not in active_wallets:
            inactive_wallets[wallet_address] = wallet_name
    
    return inactive_wallets, wallet_names

def organize_by_sections(inactive_wallets):
    """Organize inactive wallets by their sections"""
    sections = load_wallet_sections()
    inactive_by_section = defaultdict(list)
    
    # Find section for each inactive wallet
    for section_name, wallet_list in sections.items():
        for wallet_address in wallet_list:
            if (wallet_address in inactive_wallets):
                inactive_by_section[section_name].append((wallet_address, inactive_wallets[wallet_address]))
    
    return inactive_by_section

def calculate_wallet_roi():
    """Calculate ROI for wallets from token_summaries.csv"""
    wallet_roi = defaultdict(list)
    wallet_names = load_wallet_names()
    reverse_wallet_names = {v: k for k, v in wallet_names.items()}
    
    try:
        # Read data from token_summaries.csv instead of summary files
        with open('data/token_summaries.csv', 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                try:
                    token_name = row.get('token_name', 'Unknown Token')
                    first_holder = row.get('first_holder')
                    base_price = row.get('base_price')
                    highest_price = row.get('highest_price')
                    mint_address = row.get('mint_address', '')
                    
                    if not first_holder or not base_price or not highest_price:
                        continue
                    
                    # Convert prices to float
                    try:
                        entry_price = float(base_price)
                        highest_price = float(highest_price)
                    except (ValueError, TypeError):
                        continue
                    
                    if entry_price == 0:
                        continue
                    
                    # Calculate ROI
                    roi = (highest_price / entry_price) - 1
                    
                    # Record ROI for wallet
                    if first_holder in reverse_wallet_names:
                        wallet_address = reverse_wallet_names[first_holder]
                        wallet_roi[wallet_address].append((roi, token_name, entry_price, highest_price))
                    else:
                        # Try to handle multiple holders
                        if ',' in first_holder:
                            holders = [h.strip() for h in first_holder.split(',')]
                            for holder in holders:
                                if holder in reverse_wallet_names:
                                    wallet_address = reverse_wallet_names[holder]
                                    wallet_roi[wallet_address].append((roi, token_name, entry_price, highest_price))
                except Exception as e:
                    print(f"Error processing row for token {row.get('token_name', 'unknown')}: {e}")
                    continue
    except Exception as e:
        print(f"Error reading token_summaries.csv: {e}")
    
    # Calculate average ROI for each wallet
    avg_roi = {}
    for wallet_address, roi_list in wallet_roi.items():
        if len(roi_list) > 0:
            total_roi = sum(item[0] for item in roi_list)
            avg = total_roi / len(roi_list)
            best_roi = max(roi_list, key=lambda x: x[0])
            avg_roi[wallet_address] = {
                'avg_roi': avg,
                'num_tokens': len(roi_list),
                'best_roi': best_roi[0],
                'best_token': best_roi[1],
                'entry': best_roi[2],
                'highest': best_roi[3]
            }
    
    return avg_roi

def generate_wallet_report():
    """Generate a report of inactive wallets and profitable wallets"""
    inactive_wallets, all_wallets = find_inactive_wallets()
    inactive_by_section = organize_by_sections(inactive_wallets)
    wallet_roi = calculate_wallet_roi()
    
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Generate timestamp for the report filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"reports/wallet_performance_{timestamp}.txt"
    
    # Write the report
    with open(report_path, 'w') as f:
        f.write("INACTIVE WALLETS REPORT\n")
        f.write("======================\n\n")
        f.write(f"Total Inactive Wallets: {len(inactive_wallets)}/{len(all_wallets)}\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("INACTIVE WALLETS BY SECTION\n")
        f.write("==========================\n\n")
        
        for section_name, wallets in inactive_by_section.items():
            f.write(f"{section_name}\n")
            f.write("-" * len(section_name) + "\n")
            
            if not wallets:
                f.write("No inactive wallets in this section.\n\n")
                continue
                
            for wallet_address, wallet_name in wallets:
                f.write(f"{wallet_name} ({wallet_address})\n")
            
            f.write("\n")
        
        # List any wallets that were not found in any section
        unsectioned_wallets = []
        for wallet_address, wallet_name in inactive_wallets.items():
            found_in_section = False
            for section_wallets in inactive_by_section.values():
                if any(w[0] == wallet_address for w in section_wallets):
                    found_in_section = True
                    break
            
            if not found_in_section:
                unsectioned_wallets.append((wallet_address, wallet_name))
        
        if unsectioned_wallets:
            f.write("Wallets Not Found in Any Section\n")
            f.write("-------------------------------\n")
            for wallet_address, wallet_name in unsectioned_wallets:
                f.write(f"{wallet_name} ({wallet_address})\n")
            f.write("\n")
        
        # Add profitable wallets section
        f.write("\n\nALL ACTIVE WALLETS BY PERFORMANCE\n")
        f.write("===============================\n\n")
        f.write("Showing wallets by average potential ROI based on token summary data\n")
        f.write("ROI = (Highest Price / Entry Price) - 1\n\n")
        
        # Sort wallets by average ROI
        sorted_wallets = sorted(wallet_roi.items(), key=lambda x: x[1]['avg_roi'], reverse=True)
        
        f.write(f"{'Wallet Name':<20} {'Avg ROI':<15} {'# Tokens':<10} {'Best Token':<30} {'Best ROI':<15}\n")
        f.write("-" * 90 + "\n")
        
        # Show all wallets, not just top 20
        for wallet_address, data in sorted_wallets:
            wallet_name = all_wallets.get(wallet_address, wallet_address[:8] + "...")
            avg_roi = data['avg_roi']
            num_tokens = data['num_tokens']
            best_token = data['best_token']
            best_roi = data['best_roi']
            
            f.write(f"{wallet_name:<20} {avg_roi:>14.2f}x {num_tokens:^10} {best_token:<30} {best_roi:>14.2f}x\n")
    
    print(f"Report generated: {report_path}")
    return inactive_wallets, wallet_roi, report_path

def execute_dune_query():
    """Execute Dune query and return execution ID"""
    url = f"https://api.dune.com/api/v1/query/{QUERY_ID}/execute"
    headers = {"X-DUNE-API-KEY": DUNE_API_KEY}
    
    try:
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            execution_id = response.json().get('execution_id')
            print(f"Query execution started with ID: {execution_id}")
            return execution_id
        else:
            logger.error(f"Failed to execute query. Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error executing Dune query: {e}")
        return None

def get_dune_query_results():
    """Fetch Dune query results and save to CSV"""
    url = f"https://api.dune.com/api/v1/query/{QUERY_ID}/results"
    headers = {"X-DUNE-API-KEY": DUNE_API_KEY}
    params = {"limit": 200, "offset": 0}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'result' in data and 'rows' in data['result']:
                rows = data['result']['rows']
                
                if rows:
                    # Create directory for CSV files if it doesn't exist
                    os.makedirs('data/dune_data', exist_ok=True)
                    
                    # Generate timestamp for the CSV filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_file = f"data/dune_data/dune_query_results_{timestamp}.csv"
                    
                    # Process and standardize column names for wallet address and ROI
                    processed_rows = []
                    for row in rows:
                        # Standardize the row structure
                        processed_row = {}
                        
                        # Extract wallet_address
                        if 'userAddress' in row:
                            processed_row['wallet_address'] = row['userAddress']
                        else:
                            # Find column containing wallet address
                            for key in row.keys():
                                if any(term in key.lower() for term in ['wallet', 'address', 'user']):
                                    if 'url' not in key.lower():
                                        processed_row['wallet_address'] = row[key]
                                        break
                        
                        # If no wallet address column found, use the first column
                        if 'wallet_address' not in processed_row and len(row) > 0:
                            processed_row['wallet_address'] = list(row.values())[0]
                        
                        # Extract ROI
                        if 'PNL' in row:
                            try:
                                processed_row['roi'] = float(row['PNL'])
                            except (ValueError, TypeError):
                                processed_row['roi'] = 0.0
                        else:
                            # Try to find any ROI or profit related columns
                            for key in row.keys():
                                if any(term in key.lower() for term in ['pnl', 'roi', 'profit', 'return', 'gain']):
                                    try:
                                        processed_row['roi'] = float(row[key])
                                    except (ValueError, TypeError):
                                        processed_row['roi'] = 0.0
                                    break
                        
                        # If no ROI column found, use a default value
                        if 'roi' not in processed_row:
                            processed_row['roi'] = 0.0
                            
                        # Copy all other columns
                        for key, value in row.items():
                            if key not in processed_row:
                                processed_row[key] = value
                        
                        processed_rows.append(processed_row)
                    
                    # Write data to CSV
                    with open(csv_file, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=processed_rows[0].keys())
                        writer.writeheader()
                        writer.writerows(processed_rows)
                    
                    print(f"Results successfully exported to {csv_file}")
                    print(f"Total records: {len(processed_rows)}")
                    return processed_rows, csv_file
                else:
                    logger.error("No results found in the response.")
                    return [], None
            else:
                logger.error("Invalid response format. Result or rows not found.")
                return [], None
        else:
            logger.error(f"Request failed with status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return [], None
            
    except Exception as e:
        logger.error(f"Error fetching Dune query results: {e}")
        return [], None

def find_wallets_for_recommendations(inactive_wallets, wallet_roi):
    """Find wallets that need swap recommendations (inactive or bad ROI)"""
    recommendation_wallets = {}
    
    # Add inactive wallets
    for address, name in inactive_wallets.items():
        recommendation_wallets[address] = {
            'name': name,
            'reason': 'Inactive',
            'avg_roi': None,
            'address': address
        }
    
    # Add wallets with bad ROI (below threshold)
    for address, data in wallet_roi.items():
        if data['avg_roi'] < ROI_THRESHOLD and address not in recommendation_wallets:
            # Get wallet name
            wallet_names = load_wallet_names()
            name = wallet_names.get(address, address[:8] + "...")
            
            recommendation_wallets[address] = {
                'name': name,
                'reason': f"Low ROI ({data['avg_roi']:.2f}x)",
                'avg_roi': data['avg_roi'],
                'address': address
            }
    
    return recommendation_wallets

async def scrape_gmgn_wallets(wallet_addresses: List[str], headless: bool = True, use_proxies: bool = False) -> Dict:
    """
    Scrape GMGN data for a list of wallet addresses using enhanced Cloudflare bypass with exponential backoff.
    Creates a new browser instance for each wallet to improve reliability with additional anti-detection measures.
    """
    logger.info(f"Starting GMGN scraping for {len(wallet_addresses)} wallets...")
    results = {}
    base_delay = 5  # Base delay in seconds
    
    # Optional proxy list if use_proxies is True
    proxies = []
    if use_proxies:
        # Add your proxy list here or load from configuration
        proxies = ["proxy1:port", "proxy2:port", "proxy3:port"]  # Replace with actual proxies
    
    # Shuffle the wallet addresses to make patterns less predictable
    shuffled_wallets = wallet_addresses.copy()
    random.shuffle(shuffled_wallets)
    
    # User agent rotation
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55"
    ]
    
    for i, address in enumerate(shuffled_wallets):
        cf_bypass = None
        wallet_success = False
        
        # Randomly vary browser type (Edge vs Chrome)
        use_edge = random.choice([True, False])
        
        # Select a random user agent
        user_agent = random.choice(user_agents)
        
        # Select a random proxy if enabled
        proxy = None
        if use_proxies and proxies:
            proxy = random.choice(proxies)
        
        # Add a longer initial delay between wallets to reduce detection patterns
        if i > 0:
            # More random delay between wallets (5-15 seconds)
            inter_wallet_delay = 5 + (10 * random.random())
            logger.info(f"Waiting {inter_wallet_delay:.2f} seconds before next wallet...")
            await asyncio.sleep(inter_wallet_delay)
        
        for attempt in range(3):  # Max 3 attempts per wallet
            try:
                logger.info(f"Attempt {attempt + 1}/3 for wallet {address}")
                
                # Close previous driver if exists
                if cf_bypass:
                    cf_bypass.close_driver()
                    
                # Create a new CloudflareBypass instance with randomized settings
                cf_bypass = CloudflareBypass(
                    use_edge=use_edge,
                    headless=headless,
                    user_agent=user_agent,
                    proxy=proxy
                )
                
                if not cf_bypass.initialize_driver():
                    logger.error(f"Failed to initialize browser driver for {address} on attempt {attempt + 1}")
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    await asyncio.sleep(delay)
                    continue
                
                # Add random behavior before hitting the target URL
                await add_random_browser_behavior(cf_bypass)
                
                # Bypass Cloudflare for the specific wallet URL
                url = f"https://gmgn.ai/sol/address/{address}"
                logger.info(f"Scraping {url} (attempt {attempt + 1}, user agent: {user_agent[:30]}...)")
                
                # Added await here
                bypass_result = await cf_bypass.bypass_cloudflare(
                    url=url,
                    max_retries=2, # Inner retries within bypass_cloudflare
                    cloudflare_wait=random.randint(30, 40)  # Randomize wait time
                )
                
                if bypass_result['success']:
                    parsed_result = process_html_with_bs4(
                        html_content=bypass_result['html_content'],
                        url=url
                    )
                    
                    if parsed_result['success']:
                        results[address] = {
                            'url': url,
                            'markdown': parsed_result['markdown'],
                            'html_path': parsed_result.get('html_path'),
                            'markdown_path': parsed_result.get('markdown_path')
                        }
                        logger.info(f"Successfully scraped data for {address}")
                        wallet_success = True
                        break # Exit attempt loop for this wallet
                    else:
                        logger.error(f"Failed to parse HTML for {address}: {parsed_result.get('error')}")
                else:
                    logger.error(f"Failed to bypass Cloudflare for {address} on attempt {attempt + 1}: {bypass_result.get('error')}")
                        
            except Exception as e:
                logger.error(f"Error scraping wallet {address} on attempt {attempt + 1}: {str(e)}")
            
            # If attempt failed, wait before retrying with exponential backoff
            if not wallet_success:
                # Add more randomness to the backoff (±20%)
                randomization_factor = 0.8 + (0.4 * random.random())
                delay = base_delay * (2 ** attempt) * randomization_factor
                logger.info(f"Waiting {delay:.2f} seconds before next attempt for {address}")
                await asyncio.sleep(delay)
        
        # Clean up the browser instance for this wallet
        if cf_bypass:
            try:
                cf_bypass.close_driver()
            except Exception as e:
                logger.error(f"Error closing browser for wallet {address}: {str(e)}")
                
        if not wallet_success:
            logger.error(f"Failed to scrape {address} after all attempts")
            
    return results

async def add_random_browser_behavior(cf_bypass):
    """
    Add random human-like behavior to the browser session before hitting the target URL.
    This helps to make automation less detectable.
    """
    try:
        # Visit a common website first (to establish browser history)
        common_sites = ["https://weather.com", "https://news.google.com", "https://reddit.com", "https://wikipedia.org"]
        decoy_site = random.choice(common_sites)
        
        cf_bypass.driver.get(decoy_site)
        await asyncio.sleep(2 + (3 * random.random()))
        
        # Scroll randomly
        cf_bypass.driver.execute_script(f"window.scrollTo(0, {random.randint(100, 1000)});")
        await asyncio.sleep(1 + random.random())
        
        # Maybe perform another scroll
        if random.random() > 0.5:
            cf_bypass.driver.execute_script(f"window.scrollTo(0, {random.randint(100, 2000)});")
            await asyncio.sleep(1 + random.random())
        
    except Exception as e:
        logger.warning(f"Error during random browser behavior: {str(e)}")
        # Continue anyway - this step is optional

async def filter_potential_wallets(potential_wallets, target_count: int, cloudflare_wait: int = 30, headless: bool = True):
    """Filter potential replacement wallets and scrape their GMGN data"""
    logger.info(f"Starting wallet filtering process for {len(potential_wallets)} potential wallets")
    logger.info(f"Target count: {target_count}")
    
    filtered_wallets = []
    extractor = SolscanWalletExtractor(headless=headless)
    
    for wallet_data in potential_wallets:
        try:
            wallet_address = wallet_data.get('wallet_address')
            if not wallet_address:
                continue
                
            # Process the wallet using SolscanWalletExtractor directly
            result = await extractor.process_wallet(
                wallet_address=wallet_address,
                cloudflare_wait=cloudflare_wait
            )
            
            if result['success']:
                filtered_wallets.append({
                    'address': wallet_address,
                    'data': result['data'],
                    'original_data': wallet_data
                })
                
                logger.info(f"Successfully processed wallet {wallet_address}")
                
                # Check if we've found enough wallets
                if len(filtered_wallets) >= target_count:
                    logger.info(f"Found {target_count} suitable wallets, stopping search")
                    break
            else:
                logger.warning(f"Failed to process wallet {wallet_address}: {result.get('error')}")
                
        except Exception as e:
            logger.error(f"Error processing wallet: {str(e)}")
            continue
            
        # Add delay between wallets
        await asyncio.sleep(2 + (3 * random.random()))
    
    # Clean up
    try:
        extractor.close_driver()
    except Exception as e:
        logger.error(f"Error closing extractor: {str(e)}")
    
    logger.info(f"Filtering complete. Found {len(filtered_wallets)} suitable wallets")
    return filtered_wallets

async def send_recommendations_to_discord(wallets_for_recommendations, filtered_dune_results):
    """Send swap recommendations to Discord webhook as a text file"""
    if not DISCORD_WEBHOOK_URL:
        logger.error("Discord webhook URL not configured")
        return False
        
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    # Generate timestamp for report filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"reports/swap_recommendations_{timestamp}.txt"
    
    # Write recommendations to file
    with open(report_path, 'w') as f:
        f.write("WALLET SWAP RECOMMENDATIONS\n")
        f.write("==========================\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        wallet_index = 1
        total_wallets = len(wallets_for_recommendations)
        
        for address, data in wallets_for_recommendations.items():
            f.write(f"Recommendation {wallet_index}/{total_wallets}\n")
            f.write("-" * 40 + "\n")
            f.write(f"Current Wallet: {data['name']} ({address})\n")
            f.write(f"Reason: {data['reason']}\n")
            
            if data['avg_roi'] is not None:
                f.write(f"Average ROI: {data['avg_roi']:.2f}x\n")
            
            # Find potential replacement from filtered results
            if filtered_dune_results:
                for result in filtered_dune_results:
                    if result['address'] != address:  # Don't recommend the same wallet
                        f.write("\nRecommended Replacement:\n")
                        f.write(f"Address: {result['address']}\n")
                        
                        # Add data from Dune if available
                        dune_data = result.get('original_data', {})
                        if 'roi' in dune_data:
                            f.write(f"Dune ROI: {dune_data['roi']:.2f}x\n")
                        
                        # Add transaction stats if available
                        wallet_data = result.get('data', {})
                        if 'transaction_count' in wallet_data:
                            f.write(f"Transaction Count: {wallet_data['transaction_count']}\n")
                        if 'token_holdings' in wallet_data:
                            f.write(f"Token Holdings: {len(wallet_data['token_holdings'])}\n")
                        
                        # Try to update Helius webhook
                        logger.info(f"Attempting to update Helius webhook for {address} to {result['address']}")
                        webhook_updated = await edit_helius_webhook(address, result['address'])
                        
                        if webhook_updated:
                            f.write("     ✓ Successfully updated webhook\n")
                        else:
                            f.write("     ✗ Failed to update webhook\n")
                        
                        wallet_index += 1
                        break  # Only use first matching replacement
                    else:
                        f.write("     No suitable replacement wallet found\n")
            
            f.write("\n")
    
    # Send file to Discord webhook
    logger.info(f"Report generated: {report_path}")
    logger.info("Sending report to Discord...")
    
    async with aiohttp.ClientSession() as session:
        with open(report_path, 'rb') as f:
            file_data = f.read()
            
        # Create form data with file
        form = aiohttp.FormData()
        form.add_field('file', file_data, 
                      filename=os.path.basename(report_path),
                      content_type='text/plain')
        
        async with session.post(DISCORD_WEBHOOK_URL, data=form) as response:
            if response.status != 204 and response.status != 200:
                response_text = await response.text()
                logger.error(f"Failed to send to Discord. Status: {response.status}, Response: {response_text}")
                return False
            else:
                logger.info("Successfully sent report to Discord")
                return True
    
    logger.info("Finished sending recommendations to Discord")

async def edit_helius_webhook(wallet_address: str, new_wallet_address: str) -> bool:
    """
    Edit a Helius webhook to replace a wallet address with a new one.
    Returns True if successful, False otherwise.
    """
    # Load all webhook IDs and API keys
    webhook_ids = {
        "gergo": os.getenv("HELIUS_WEBHOOK_ID"),
        "aikov": os.getenv("HELIUS_WEBHOOK_ID_2"),
        "sneezyhub": os.getenv("HELIUS_WEBHOOK_ID_3"),
        "llzsolt": os.getenv("HELIUS_WEBHOOK_ID_4"),
        "kaihunf": os.getenv("HELIUS_WEBHOOK_ID_5"),
        "sn33": os.getenv("HELIUS_WEBHOOK_ID_6"),
        "emma": os.getenv("HELIUS_WEBHOOK_ID_7"),
        "balint": os.getenv("HELIUS_WEBHOOK_ID_8")
    }
    
    api_keys = {
        "gergo": os.getenv("HELIUS_API_KEY"),
        "aikov": os.getenv("HELIUS_API_KEY_2"),
        "sneezyhub": os.getenv("HELIUS_API_KEY_3"),
        "llzsolt": os.getenv("HELIUS_API_KEY_4"),
        "kaihunf": os.getenv("HELIUS_API_KEY_5"),
        "sn33": os.getenv("HELIUS_API_KEY_6"),
        "emma": os.getenv("HELIUS_API_KEY_7"),
        "balint": os.getenv("HELIUS_API_KEY_8")
    }
    
    # Find which section contains the wallet
    section = None
    with open('config/wallet_sections.txt', 'r') as f:
        current_section = None
        for line in f:
            line = line.strip()
            if line.startswith('Section'):
                current_section = line.split('#')[1].strip()
            elif wallet_address in line:
                section = current_section
                break
    
    if not section:
        logger.error(f"Could not find section for wallet {wallet_address}")
        return False
    
    webhook_id = webhook_ids.get(section)
    api_key = api_keys.get(section)
    
    if not webhook_id or not api_key:
        logger.error(f"Missing webhook ID or API key for section {section}")
        return False
    
    # Get current webhook configuration
    async with aiohttp.ClientSession() as session:
        try:
            # First get current webhook config
            get_url = f"https://api.helius.xyz/v0/webhooks/{webhook_id}?api-key={api_key}"
            async with session.get(get_url) as response:
                if response.status != 200:
                    logger.error(f"Failed to get webhook config: {response.status}")
                    return False
                
                webhook_config = await response.json()
                current_addresses = webhook_config.get('accountAddresses', [])
                
                # Replace the old wallet with the new one
                if wallet_address in current_addresses:
                    current_addresses.remove(wallet_address)
                    current_addresses.append(new_wallet_address)
                else:
                    logger.error(f"Wallet {wallet_address} not found in webhook addresses")
                    return False
                
                # Update webhook with new addresses
                update_url = f"https://api.helius.xyz/v0/webhooks/{webhook_id}?api-key={api_key}"
                update_data = {
                    "accountAddresses": current_addresses,
                    "transactionTypes": webhook_config.get('transactionTypes', []),
                    "webhookType": webhook_config.get('webhookType', 'enhanced')
                }
                
                async with session.put(update_url, json=update_data) as update_response:
                    if update_response.status != 200:
                        logger.error(f"Failed to update webhook: {update_response.status}")
                        return False
                    
                    logger.info(f"Successfully updated webhook for section {section}")
                    logger.info(f"Replaced {wallet_address} with {new_wallet_address}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error updating webhook: {str(e)}")
            return False

async def main():
    """Main function that runs the wallet swap process"""
    # Step 1: Generate wallet report to identify inactive wallets and wallets with poor ROI
    logger.info("Generating wallet report...")
    inactive_wallets, wallet_roi, report_path = generate_wallet_report()
    
    # Calculate how many replacement wallets we need - corrected calculation
    # Only count wallets with ROI below threshold that aren't already inactive
    low_roi_wallets = len([w for w in wallet_roi.values() 
                          if w['avg_roi'] < ROI_THRESHOLD and 
                          list(w.keys())[0] not in inactive_wallets])
    
    total_wallets_to_replace = len(inactive_wallets) + low_roi_wallets
    logger.info(f"Need to find {total_wallets_to_replace} replacement wallets:")
    logger.info(f"- {len(inactive_wallets)} inactive wallets")
    logger.info(f"- {low_roi_wallets} low ROI wallets")
    
    # Step 2: Execute Dune query
    logger.info(f"Executing Dune query {QUERY_ID}...")
    execution_id = execute_dune_query()
    
    if execution_id:
        # Step 3: Wait for query execution to complete
        logger.info(f"Waiting {QUERY_WAIT_TIME} seconds for query execution...")
        await asyncio.sleep(QUERY_WAIT_TIME)
        
        # Step 4: Fetch query results
        logger.info("Fetching Dune query results...")
        dune_results, csv_file = get_dune_query_results()
        
        if dune_results:
            # Step 5: Find wallets for recommendations
            logger.info("Finding wallets for swap recommendations...")
            wallets_for_recommendations = find_wallets_for_recommendations(inactive_wallets, wallet_roi)
            
            # Step 5.5: Filter potential replacement wallets
            logger.info("Filtering potential replacement wallets by transaction types...")
            filtered_dune_results = await filter_potential_wallets(
                dune_results, 
                target_count=total_wallets_to_replace, 
                cloudflare_wait=40, 
                headless=True
            )
            
            # Step 6: Send recommendations to Discord
            logger.info("Sending swap recommendations to Discord...")
            await send_recommendations_to_discord(wallets_for_recommendations, filtered_dune_results)
        else:
            logger.error("No Dune query results available. Recommendations cannot be generated.")
    else:
        logger.error("Failed to execute Dune query. Cannot proceed with recommendations.")
    
    logger.info("Process completed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error traceback: {traceback.format_exc()}")
