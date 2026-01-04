import time
import json
import asyncio
import logging
import os
from bs4 import BeautifulSoup
import traceback
import argparse
from playwright.async_api import async_playwright, TimeoutError
from typing import List, Dict
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GMGNScraper:
    def __init__(self, headless=True, max_retries=3):
        self.headless = headless
        self.base_url = "https://gmgn.ai/sol/address/"
        self.browser = None
        self.page = None
        self.max_retries = max_retries
        
    async def __aenter__(self):
        logger.info("Starting Playwright...")
        playwright = await async_playwright().start()
        logger.info("Launching browser...")
        
        # Configure browser to look more like a real user
        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1920,1080',
                '--disable-extensions',
                '--disable-popup-blocking',
                '--disable-notifications',
                '--disable-infobars',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process'
            ]
        )
        
        logger.info("Creating new page...")
        self.page = await self.browser.new_page()
        
        # Set a realistic viewport
        await self.page.set_viewport_size({"width": 1920, "height": 1080})
        
        # Set user agent to look like a real browser
        await self.page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        })
        
        # Add random mouse movements and delays to look more human
        await self.page.mouse.move(random.randint(0, 1920), random.randint(0, 1080))
        await asyncio.sleep(random.uniform(0.5, 2))
        
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            logger.info("Closing browser...")
            await self.browser.close()
            
    async def wait_for_cloudflare(self, timeout=60):
        """Wait for Cloudflare challenge to clear"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check for different Cloudflare challenge texts
            challenge_texts = [
                "Checking if the site connection is secure",
                "Please wait while we check your browser",
                "Verifying your browser",
                "Just a moment"
            ]
            
            for text in challenge_texts:
                if await self.page.get_by_text(text).count() > 0:
                    logger.info(f"Cloudflare challenge detected: {text}")
                    # Add random mouse movements while waiting
                    await self.page.mouse.move(
                        random.randint(0, 1920),
                        random.randint(0, 1080)
                    )
                    await asyncio.sleep(random.uniform(1, 3))
                    break
            else:
                # No challenge text found, we might be through
                logger.info("No Cloudflare challenge detected, proceeding...")
                return True
                
        logger.error("Cloudflare challenge timeout")
        return False
            
    async def scrape_wallet(self, wallet_address, cloudflare_wait=60):
        """Scrape wallet data from GMGN"""
        url = f"{self.base_url}{wallet_address}"
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{self.max_retries} - Navigating to {url}...")
                
                # Add random delay before navigation
                await asyncio.sleep(random.uniform(1, 3))
                
                # Navigate with longer timeout
                await self.page.goto(url, wait_until='networkidle', timeout=60000)
                
                # Check for Cloudflare challenge
                if not await self.wait_for_cloudflare(cloudflare_wait):
                    logger.error("Failed to bypass Cloudflare challenge")
                    continue
                
                # Add random scroll behavior
                for _ in range(3):
                    await self.page.evaluate(f"window.scrollTo(0, {random.randint(0, 1000)})")
                    await asyncio.sleep(random.uniform(0.5, 2))
                
                logger.info("Waiting for content to load...")
                
                # Wait for key elements with retry
                try:
                    await self.page.wait_for_selector('.wallet-info', timeout=20000)
                    logger.info("Wallet info section found")
                except TimeoutError:
                    logger.warning("Wallet info section not found, retrying...")
                    continue
                
                logger.info("Getting page content...")
                content = await self.page.content()
                logger.info(f"Got page content (length: {len(content)})")
                
                # Take a screenshot for debugging
                logger.info("Taking screenshot...")
                await self.page.screenshot(path=f"gmgn_screenshot_{attempt + 1}.png")
                
                soup = BeautifulSoup(content, 'html.parser')
                
                # Extract data
                results = {
                    'success': True,
                    'wallet': wallet_address,
                    'data': {}
                }
                
                # Extract wallet info
                logger.info("Extracting wallet info...")
                wallet_info = soup.find('div', {'class': 'wallet-info'})
                if wallet_info:
                    results['data']['wallet_info'] = {
                        'name': wallet_info.find('h1').text.strip() if wallet_info.find('h1') else None,
                        'address': wallet_info.find('p', {'class': 'address'}).text.strip() if wallet_info.find('p', {'class': 'address'}) else None
                    }
                else:
                    logger.warning("No wallet info found in the page")
                    continue
                
                # Extract stats
                logger.info("Extracting stats...")
                stats = soup.find('div', {'class': 'stats'})
                if stats:
                    results['data']['stats'] = {}
                    for stat in stats.find_all('div', {'class': 'stat'}):
                        label = stat.find('label').text.strip()
                        value = stat.find('value').text.strip()
                        results['data']['stats'][label] = value
                
                # Extract transaction history
                logger.info("Extracting transaction history...")
                transactions = soup.find('div', {'class': 'transactions'})
                if transactions:
                    results['data']['transactions'] = []
                    for tx in transactions.find_all('div', {'class': 'transaction'}):
                        transaction = {
                            'hash': tx.find('a', {'class': 'tx-hash'}).text.strip() if tx.find('a', {'class': 'tx-hash'}) else None,
                            'type': tx.find('span', {'class': 'tx-type'}).text.strip() if tx.find('span', {'class': 'tx-type'}) else None,
                            'amount': tx.find('span', {'class': 'tx-amount'}).text.strip() if tx.find('span', {'class': 'tx-amount'}) else None,
                            'time': tx.find('span', {'class': 'tx-time'}).text.strip() if tx.find('span', {'class': 'tx-time'}) else None
                        }
                        results['data']['transactions'].append(transaction)
                
                # Extract token holdings
                logger.info("Extracting token holdings...")
                tokens = soup.find('div', {'class': 'tokens'})
                if tokens:
                    results['data']['tokens'] = []
                    for token in tokens.find_all('div', {'class': 'token'}):
                        token_data = {
                            'symbol': token.find('span', {'class': 'token-symbol'}).text.strip() if token.find('span', {'class': 'token-symbol'}) else None,
                            'balance': token.find('span', {'class': 'token-balance'}).text.strip() if token.find('span', {'class': 'token-balance'}) else None,
                            'value': token.find('span', {'class': 'token-value'}).text.strip() if token.find('span', {'class': 'token-value'}) else None
                        }
                        results['data']['tokens'].append(token_data)
                
                # Extract NFT holdings
                logger.info("Extracting NFT holdings...")
                nfts = soup.find('div', {'class': 'nfts'})
                if nfts:
                    results['data']['nfts'] = []
                    for nft in nfts.find_all('div', {'class': 'nft'}):
                        nft_data = {
                            'name': nft.find('span', {'class': 'nft-name'}).text.strip() if nft.find('span', {'class': 'nft-name'}) else None,
                            'collection': nft.find('span', {'class': 'nft-collection'}).text.strip() if nft.find('span', {'class': 'nft-collection'}) else None,
                            'value': nft.find('span', {'class': 'nft-value'}).text.strip() if nft.find('span', {'class': 'nft-value'}) else None
                        }
                        results['data']['nfts'].append(nft_data)
                        
                logger.info("Scraping completed successfully")
                return results
                
            except Exception as e:
                last_error = str(e)
                logger.error(f"Error on attempt {attempt + 1}: {str(e)}")
                traceback.print_exc()
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                    
        return {
            'success': False,
            'wallet': wallet_address,
            'error': f"Failed after {self.max_retries} attempts. Last error: {last_error}"
        }

    async def scrape_wallets(self, wallets: List[str], cloudflare_wait: int = 30) -> List[Dict]:
        """
        Scrape multiple wallets in parallel
        
        Args:
            wallets: List of wallet addresses to scrape
            cloudflare_wait: Time to wait for Cloudflare challenge in seconds
            
        Returns:
            List of scraping results
        """
        try:
            # Process wallets in parallel
            tasks = []
            for wallet in wallets:
                task = self.scrape_wallet(wallet, cloudflare_wait)
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            processed_results = []
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error scraping wallet: {str(result)}")
                    processed_results.append({
                        'success': False,
                        'error': str(result)
                    })
                else:
                    processed_results.append(result)
            
            return processed_results
            
        except Exception as e:
            logger.error(f"Error in scrape_wallets: {str(e)}")
            return [{
                'success': False,
                'error': str(e)
            } for _ in wallets]

async def main():
    parser = argparse.ArgumentParser(description='GMGN Wallet Scraper')
    parser.add_argument('--wallet', required=True, help='Wallet address to scrape')
    parser.add_argument('--cloudflare-wait', type=int, default=30, help='Time to wait for Cloudflare challenge')
    parser.add_argument('--no-headless', action='store_true', help='Run browser with GUI')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retry attempts')
    
    args = parser.parse_args()
    
    logger.info(f"Starting scraper for wallet {args.wallet}...")
    async with GMGNScraper(headless=not args.no_headless, max_retries=args.max_retries) as scraper:
        result = await scraper.scrape_wallet(args.wallet, args.cloudflare_wait)
        print(json.dumps(result, indent=2))
    logger.info("Scraper finished")

if __name__ == "__main__":
    asyncio.run(main()) 