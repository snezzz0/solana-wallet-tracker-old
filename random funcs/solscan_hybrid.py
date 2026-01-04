import asyncio
import json
import time
import logging
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import argparse
from bs4 import BeautifulSoup
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CloudflareBypass:
    def __init__(self, use_edge=True, headless=False):
        self.driver = None
        self.use_edge = use_edge
        self.headless = headless
        
    def initialize_driver(self):
        """Initialize the browser driver"""
        try:
            if self.use_edge:
                logger.info("Initializing Microsoft Edge driver...")
                options = webdriver.EdgeOptions()
                
                if self.headless:
                    options.add_argument('--headless=new')
                
                # Anti-detection settings
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                options.add_argument('--disable-extensions')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                
                service = EdgeService(EdgeChromiumDriverManager().install())
                self.driver = webdriver.Edge(service=service, options=options)
            else:
                logger.info("Initializing Chrome driver...")
                options = webdriver.ChromeOptions()
                
                if self.headless:
                    options.add_argument('--headless=new')
                
                # Anti-detection settings
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)
                options.add_argument('--disable-extensions')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                
                service = ChromeService(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=options)
            
            # Execute CDP commands to prevent detection
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            })
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.driver.set_page_load_timeout(120)  # Increase timeout to 120 seconds
            self.driver.set_window_size(1920, 1080)
            
            logger.info("Successfully initialized browser driver")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing browser driver: {str(e)}")
            return False
    
    def close_driver(self):
        """Close the browser and release resources"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed successfully")
            except Exception as e:
                logger.error(f"Error closing browser: {str(e)}")
            finally:
                self.driver = None
    
    def is_page_loaded(self, timeout=10):
        """
        Check if the page is fully loaded by monitoring network activity
        
        Args:
            timeout (int): Maximum time to wait in seconds
            
        Returns:
            bool: True if page is loaded, False otherwise
        """
        try:
            # Wait for document.readyState to be complete
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # Check for any loading indicators that might be present
            loading_indicators = self.driver.find_elements(By.CSS_SELECTOR, ".loading, .spinner, .loader")
            if loading_indicators:
                # Wait for loading indicators to disappear
                WebDriverWait(self.driver, timeout).until(
                    lambda d: not any(i.is_displayed() for i in d.find_elements(By.CSS_SELECTOR, ".loading, .spinner, .loader"))
                )
            
            logger.info("Page appears to be fully loaded")
            return True
        
        except (TimeoutException, StaleElementReferenceException):
            logger.warning("Timeout waiting for page to fully load")
            return False
    
    def bypass_cloudflare(self, url, max_retries=3, cloudflare_wait=20):
        """
        Bypass Cloudflare protection and return HTML content
        
        Args:
            url (str): The URL to access
            max_retries (int): Number of retry attempts
            cloudflare_wait (int): Time to wait for Cloudflare challenge in seconds
            
        Returns:
            dict: Result containing HTML content and success status
        """
        if not self.driver:
            success = self.initialize_driver()
            if not success:
                return {'success': False, 'error': 'Failed to initialize driver'}
        
        # Try to load the page with retries
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{max_retries} - Loading {url}")
                self.driver.get(url)
                
                # Wait for the page to fully load
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Check if we're on Cloudflare's challenge page
                if "Checking if the site connection is secure" in self.driver.page_source or "Just a moment" in self.driver.page_source:
                    logger.info("Cloudflare challenge detected, waiting to solve...")
                    
                    # Wait for Cloudflare challenge to be solved
                    time.sleep(cloudflare_wait)
                    
                    # Check if we're still on the challenge page after waiting
                    if "Checking if the site connection is secure" in self.driver.page_source or "Just a moment" in self.driver.page_source:
                        logger.warning(f"Still on Cloudflare challenge page after waiting. Attempt {attempt}/{max_retries}")
                        if attempt < max_retries:
                            time.sleep(5)  # Wait before retrying
                            continue
                        else:
                            return {'success': False, 'error': 'Failed to bypass Cloudflare challenge'}
                
                logger.info(f"Successfully bypassed Cloudflare. Page title: {self.driver.title}")
                
                # Wait for page content to fully load (important for JS-heavy pages)
                time.sleep(5)  # Additional wait to ensure JavaScript renders complete
                
                # Check for specific content based on URL type
                if "/token/" in url:
                    try:
                        logger.info("Waiting for token content to load...")
                        # First try to wait for card-body elements to appear
                        WebDriverWait(self.driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".card-body"))
                        )
                        
                        # Wait for detail-item elements which contain the token data
                        WebDriverWait(self.driver, 15).until(
                            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".detail-item")) > 0
                        )
                        
                        # Wait for the page to be fully loaded
                        self.is_page_loaded(timeout=15)
                        
                        logger.info("Token content loaded successfully")
                    except TimeoutException:
                        logger.warning("Timeout waiting for token content to fully load")
                        
                    # Additional wait to ensure dynamic content is loaded
                    time.sleep(5)
                
                elif "/account/" in url:
                    try:
                        logger.info("Waiting for account content to load...")
                        # Wait for account details to appear
                        WebDriverWait(self.driver, 20).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".card-body"))
                        )
                        
                        # Wait for detail-item elements which contain the account data
                        WebDriverWait(self.driver, 15).until(
                            lambda d: len(d.find_elements(By.CSS_SELECTOR, ".detail-item")) > 0
                        )
                        
                        # Wait for the page to be fully loaded
                        self.is_page_loaded(timeout=15)
                        
                        logger.info("Account content loaded successfully")
                    except TimeoutException:
                        logger.warning("Timeout waiting for account content to fully load")
                    
                    # Additional wait to ensure dynamic content is loaded
                    time.sleep(5)
                
                # Get page content
                html_content = self.driver.page_source
                logger.info(f"Captured HTML content (length: {len(html_content)} chars)")
                
                # Take a screenshot for verification
                screenshot_path = f"solscan_screenshot_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"Saved screenshot to {screenshot_path}")
                
                # Get the current URL (may be different after redirects)
                final_url = self.driver.current_url
                
                return {
                    'success': True,
                    'html_content': html_content,
                    'url': final_url,
                    'title': self.driver.title
                }
                
            except TimeoutException:
                logger.warning(f"Timeout loading the page. Attempt {attempt}/{max_retries}")
                if attempt < max_retries:
                    time.sleep(5)  # Wait before retrying
                    continue
                else:
                    return {'success': False, 'error': 'Timeout loading the page'}
                
            except Exception as e:
                logger.error(f"Error accessing {url}: {str(e)}")
                if attempt < max_retries:
                    time.sleep(5)  # Wait before retrying
                    continue
                else:
                    return {'success': False, 'error': str(e)}
        
        return {'success': False, 'error': 'Failed after all retry attempts'}

def process_html_with_bs4(html_content, url):
    """
    Process HTML content using BeautifulSoup
    
    Args:
        html_content (str): HTML content of the page
        url (str): URL of the page (for context)
    
    Returns:
        dict: Extracted data in markdown format
    """
    logger.info(f"Processing HTML content with BeautifulSoup...")
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract title
        title = soup.title.string if soup.title else "No title found"
        
        # Extract other data based on URL type
        if "/token/" in url:
            return process_token_page(soup, title)
        elif "/account/" in url:
            return process_account_page(soup, title)
        else:
            return {
                'success': True,
                'markdown': f"# {title}\n\nNo specific extraction method for this page type."
            }
    
    except Exception as e:
        logger.error(f"Error using BeautifulSoup to process HTML: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def process_token_page(soup, title):
    """Process token page HTML with BeautifulSoup"""
    try:
        markdown = f"# {title}\n\n"
        
        # Extract price if available
        price_elem = soup.select_one('.token-price-usd, .sol-tv-usd')
        if price_elem:
            price = price_elem.get_text(strip=True)
            markdown += f"**Price**: {price}\n\n"
        
        # Extract metadata - we'll look in a few potential locations
        token_metadata = {}
        
        # Try to find the token address
        address_elems = soup.select('.address-component')
        if address_elems:
            for elem in address_elems:
                parent = elem.find_parent('div', class_='detail-item')
                if parent:
                    label_elem = parent.select_one('.detail-label')
                    if label_elem and 'address' in label_elem.get_text(strip=True).lower():
                        token_metadata['Token Address'] = elem.get_text(strip=True)
                        break
        
        # If token address wasn't found in the previous attempt
        if 'Token Address' not in token_metadata:
            token_addr_container = soup.select_one('.token-address')
            if token_addr_container:
                addr_component = token_addr_container.select_one('.address-component')
                if addr_component:
                    token_metadata['Token Address'] = addr_component.get_text(strip=True)
        
        # Extract token details from all card-body elements
        card_bodies = soup.select('.card-body')
        for card in card_bodies:
            # Extract details from this card
            detail_items = card.select('.detail-item')
            for item in detail_items:
                label_elem = item.select_one('.detail-label')
                value_elem = item.select_one('.detail-value')
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    
                    # First check if value contains an address component
                    addr_component = value_elem.select_one('.address-component')
                    if addr_component:
                        value = addr_component.get_text(strip=True)
                    else:
                        value = value_elem.get_text(strip=True)
                    
                    token_metadata[label] = value
        
        # Add token price info to metadata if we found it
        if price_elem:
            token_metadata['Price'] = price
        
        # Output metadata in markdown
        if token_metadata:
            markdown += "## Token Metadata\n\n"
            for key, value in token_metadata.items():
                markdown += f"**{key}**: {value}\n"
        
        # Try to extract holders information
        holders_section = None
        for heading in soup.select('h3, h2'):
            text = heading.get_text(strip=True)
            if 'holder' in text.lower():
                holders_section = heading.find_parent('div', class_='card')
                break
        
        if holders_section:
            markdown += "\n## Holders Information\n\n"
            # Try to extract holders data
            holders_items = holders_section.select('.detail-item')
            for item in holders_items:
                label_elem = item.select_one('.detail-label')
                value_elem = item.select_one('.detail-value')
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    value = value_elem.get_text(strip=True)
                    markdown += f"**{label}**: {value}\n"
        
        return {
            'success': True,
            'markdown': markdown
        }
    except Exception as e:
        logger.error(f"Error processing token page: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def process_account_page(soup, title):
    """Process account page HTML with BeautifulSoup"""
    try:
        markdown = f"# {title}\n\n"
        
        # Extract account address
        account_data = {}
        address_elem = soup.select_one('.address-component')
        if address_elem:
            account_address = address_elem.get_text(strip=True)
            account_data['Address'] = account_address
            
        # Extract SOL balance if available
        sol_balance_elem = soup.select_one('.sol-balance')
        if sol_balance_elem:
            sol_balance = sol_balance_elem.get_text(strip=True)
            markdown += f"**SOL Balance**: {sol_balance}\n\n"
            account_data['SOL Balance'] = sol_balance
        
        # Extract account details from all card-body elements
        card_bodies = soup.select('.card-body')
        for card in card_bodies:
            # Extract details from this card
            detail_items = card.select('.detail-item')
            for item in detail_items:
                label_elem = item.select_one('.detail-label')
                value_elem = item.select_one('.detail-value')
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    
                    # First check if value contains an address component
                    addr_component = value_elem.select_one('.address-component')
                    if addr_component:
                        value = addr_component.get_text(strip=True)
                    else:
                        value = value_elem.get_text(strip=True)
                    
                    account_data[label] = value
        
        # Output account data in markdown
        if account_data:
            markdown += "## Account Information\n\n"
            for key, value in account_data.items():
                if key not in ['SOL Balance']:  # Avoid duplication
                    markdown += f"**{key}**: {value}\n"
        
        # Try to extract token holdings
        tokens_section = None
        for heading in soup.select('h3, h2'):
            text = heading.get_text(strip=True)
            if 'token' in text.lower() and ('holding' in text.lower() or 'balance' in text.lower()):
                tokens_section = heading.find_parent('div', class_='card')
                break
        
        if tokens_section:
            markdown += "\n## Token Holdings\n\n"
            token_table = tokens_section.select_one('table')
            if token_table:
                # Try to extract tokens from the table
                rows = token_table.select('tr')
                if len(rows) > 1:  # If there's at least a header and one data row
                    # First row is likely the header
                    headers = []
                    for th in rows[0].select('th'):
                        headers.append(th.get_text(strip=True))
                    
                    # Create markdown table header
                    if headers:
                        markdown += "| " + " | ".join(headers) + " |\n"
                        markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                    
                    # Add data rows
                    for row in rows[1:]:
                        cells = []
                        for td in row.select('td'):
                            # Check for address components
                            addr = td.select_one('.address-component')
                            if addr:
                                cells.append(addr.get_text(strip=True))
                            else:
                                cells.append(td.get_text(strip=True))
                        
                        if cells:
                            markdown += "| " + " | ".join(cells) + " |\n"
            else:
                # If no table is found, try to find token list items
                token_items = tokens_section.select('.token-item')
                if token_items:
                    for item in token_items:
                        name_elem = item.select_one('.token-name')
                        amount_elem = item.select_one('.token-amount')
                        
                        if name_elem and amount_elem:
                            token_name = name_elem.get_text(strip=True)
                            token_amount = amount_elem.get_text(strip=True)
                            markdown += f"- **{token_name}**: {token_amount}\n"
        
        # Try to extract transaction history
        tx_section = None
        for heading in soup.select('h3, h2'):
            text = heading.get_text(strip=True)
            if 'transaction' in text.lower():
                tx_section = heading.find_parent('div', class_='card')
                break
        
        if tx_section:
            markdown += "\n## Recent Transactions\n\n"
            tx_table = tx_section.select_one('table')
            if tx_table:
                # Try to extract transactions from the table
                rows = tx_table.select('tr')
                if len(rows) > 1:  # If there's at least a header and one data row
                    # First row is likely the header
                    headers = []
                    for th in rows[0].select('th'):
                        headers.append(th.get_text(strip=True))
                    
                    # Create markdown table header
                    if headers:
                        markdown += "| " + " | ".join(headers) + " |\n"
                        markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                    
                    # Add data rows (limit to 5 transactions to keep output manageable)
                    tx_count = 0
                    for row in rows[1:]:
                        if tx_count >= 5:
                            break
                            
                        cells = []
                        for td in row.select('td'):
                            # Check for address components
                            addr = td.select_one('.address-component')
                            if addr:
                                cells.append(addr.get_text(strip=True))
                            else:
                                cells.append(td.get_text(strip=True))
                        
                        if cells:
                            markdown += "| " + " | ".join(cells) + " |\n"
                            tx_count += 1
                    
                    if len(rows) > 6:  # If there are more transactions
                        markdown += "\n_Showing 5 of " + str(len(rows)-1) + " transactions_\n"
        
        return {
            'success': True,
            'markdown': markdown
        }
    except Exception as e:
        logger.error(f"Error processing account page: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

async def process_solscan_url(url, use_edge=True, headless=False):
    """
    Process a Solscan URL using hybrid approach:
    1. Bypass Cloudflare with Selenium and get full HTML content
    2. Extract data from HTML with BeautifulSoup
    
    Args:
        url (str): Solscan URL to process
        use_edge (bool): Whether to use Edge instead of Chrome
        headless (bool): Whether to run browser in headless mode
    
    Returns:
        dict: Result with extracted data
    """
    # First, bypass Cloudflare and get HTML content
    cloudflare_bypass = CloudflareBypass(use_edge=use_edge, headless=headless)
    
    try:
        logger.info(f"Bypassing Cloudflare for {url}...")
        bypass_result = cloudflare_bypass.bypass_cloudflare(url)
        
        if not bypass_result['success']:
            logger.error(f"Failed to bypass Cloudflare: {bypass_result.get('error')}")
            return {'success': False, 'error': bypass_result.get('error')}
        
        logger.info("Successfully bypassed Cloudflare, now processing HTML with BeautifulSoup...")
        
        # Use BeautifulSoup to process the HTML content
        bs4_result = process_html_with_bs4(
            html_content=bypass_result['html_content'],
            url=bypass_result['url']
        )
        
        return bs4_result
    
    finally:
        # Always close the browser
        cloudflare_bypass.close_driver()

def parse_arguments():
    parser = argparse.ArgumentParser(description='Hybrid scraper for Solscan using Selenium + BeautifulSoup')
    parser.add_argument('--url', type=str, 
                        default="https://solscan.io/token/DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                        help='The Solscan URL to scrape')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')
    parser.add_argument('--use-chrome', action='store_true',
                        help='Use Chrome instead of Edge')
    parser.add_argument('--output', type=str,
                        help='Output file path for the extracted data (JSON format)')
    
    return parser.parse_args()

async def main():
    args = parse_arguments()
    
    result = await process_solscan_url(
        url=args.url,
        use_edge=not args.use_chrome,
        headless=args.headless
    )
    
    if result['success']:
        print("\n--- Extracted Data ---")
        print(result['markdown'])
        
        # Save to output file if specified
        if args.output:
            try:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2)
                print(f"\nData saved to {args.output}")
            except Exception as e:
                print(f"\nError saving data to file: {e}")
    else:
        print(f"\nError: {result.get('error')}")

if __name__ == "__main__":
    asyncio.run(main()) 