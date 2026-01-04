from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import json
import logging
import os
import traceback
import sys
import platform
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SolscanScraper:
    def __init__(self, headless=False, use_edge=False):
        self.headless = headless
        self.driver = None
        self.use_edge = use_edge
    
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
                
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=options)
            
            # Execute CDP commands to prevent detection
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            })
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.driver.set_page_load_timeout(60)
            self.driver.set_window_size(1920, 1080)
            
            logger.info("Successfully initialized browser driver")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing browser driver: {str(e)}")
            logger.error(traceback.format_exc())
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
    
    def get_page(self, url, max_retries=3):
        """
        Load a web page and wait for it to fully load
        
        Args:
            url (str): The URL to load
            max_retries (int): Number of retry attempts if loading fails
            
        Returns:
            dict: Result of the operation
        """
        if not self.driver:
            success = self.initialize_driver()
            if not success:
                return {'status': 'error', 'error': 'Failed to initialize driver'}
        
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
                    
                    # Wait longer for Cloudflare challenge to be solved
                    time.sleep(15)  # Additional wait for Cloudflare
                    
                    # Check if we're still on the challenge page after waiting
                    if "Checking if the site connection is secure" in self.driver.page_source or "Just a moment" in self.driver.page_source:
                        logger.warning(f"Still on Cloudflare challenge page after waiting. Attempt {attempt}/{max_retries}")
                        if attempt < max_retries:
                            time.sleep(5)  # Wait before retrying
                            continue
                        else:
                            return {
                                'status': 'error',
                                'error': 'Failed to bypass Cloudflare challenge'
                            }
                
                logger.info(f"Page loaded successfully: {self.driver.title}")
                return {
                    'status': 'success',
                    'title': self.driver.title,
                    'url': self.driver.current_url,
                    'content_length': len(self.driver.page_source)
                }
                
            except TimeoutException:
                logger.warning(f"Timeout loading the page. Attempt {attempt}/{max_retries}")
                if attempt < max_retries:
                    time.sleep(5)  # Wait before retrying
                    continue
                else:
                    return {
                        'status': 'error',
                        'error': 'Timeout loading the page'
                    }
            
            except WebDriverException as e:
                logger.error(f"WebDriver error: {str(e)}")
                if attempt < max_retries:
                    # Restart the driver
                    self.close_driver()
                    time.sleep(5)
                    success = self.initialize_driver()
                    if not success:
                        return {'status': 'error', 'error': 'Failed to reinitialize driver'}
                    continue
                else:
                    return {
                        'status': 'error',
                        'error': str(e)
                    }
            
            except Exception as e:
                logger.error(f"Error accessing {url}: {str(e)}")
                logger.error(traceback.format_exc())
                if attempt < max_retries:
                    time.sleep(5)  # Wait before retrying
                    continue
                else:
                    return {
                        'status': 'error',
                        'error': str(e)
                    }
        
        return {
            'status': 'error',
            'error': 'Failed to load page after all retry attempts'
        }
    
    def extract_token_data(self, token_url):
        """
        Extract data from a Solscan token page
        
        Args:
            token_url (str): The token URL to scrape
            
        Returns:
            dict: Extracted token data
        """
        result = self.get_page(token_url)
        
        if result['status'] != 'success':
            return result
        
        try:
            # Wait for token information to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".card-body"))
            )
            
            # Get page source for debugging
            page_source = self.driver.page_source
            
            # Extract token data
            token_data = {}
            
            # Extract token name and symbol
            try:
                token_header = self.driver.find_element(By.CSS_SELECTOR, "h1.token-name-header")
                token_data['name'] = token_header.text.strip() if token_header else "Unknown"
            except Exception as e:
                logger.warning(f"Could not extract token name: {str(e)}")
                token_data['name'] = "Unknown"
            
            # Extract token price if available
            try:
                price_element = self.driver.find_element(By.CSS_SELECTOR, ".token-price-usd")
                token_data['price'] = price_element.text.strip() if price_element else "Unknown"
            except Exception as e:
                logger.warning(f"Could not extract token price: {str(e)}")
                token_data['price'] = "Unknown"
            
            # Get all overview data (this will depend on the actual structure of the page)
            try:
                overview_elements = self.driver.find_elements(By.CSS_SELECTOR, ".detail-item")
                for element in overview_elements:
                    try:
                        label = element.find_element(By.CSS_SELECTOR, ".detail-label").text.strip()
                        value = element.find_element(By.CSS_SELECTOR, ".detail-value").text.strip()
                        token_data[label] = value
                    except Exception:
                        continue
            except Exception as e:
                logger.warning(f"Error extracting overview data: {str(e)}")
            
            # If nothing was extracted, try to save the HTML for debugging
            if len(token_data) <= 1:  # Only name was found or less
                debug_file = f"debug_solscan_{int(time.time())}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(page_source)
                logger.info(f"Saved debug HTML to {debug_file}")
                
                # Take a screenshot for debugging
                screenshot_file = f"debug_solscan_{int(time.time())}.png"
                self.driver.save_screenshot(screenshot_file)
                logger.info(f"Saved screenshot to {screenshot_file}")
            
            return {
                'status': 'success',
                'token_data': token_data
            }
            
        except Exception as e:
            logger.error(f"Error extracting token data: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'error': str(e)
            }


def parse_arguments():
    parser = argparse.ArgumentParser(description='Scrape data from Solscan.io using Selenium')
    parser.add_argument('--url', type=str, default="https://solscan.io/token/DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                        help='The Solscan URL to scrape')
    parser.add_argument('--headless', action='store_true',
                        help='Run browser in headless mode')
    parser.add_argument('--use-edge', action='store_true',
                        help='Use Microsoft Edge instead of Chrome')
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    # Create scraper instance
    # Set headless=False to see the browser window (helps with debugging)
    scraper = SolscanScraper(
        headless=args.headless,
        use_edge=args.use_edge
    )
    
    try:
        # Get token data
        result = scraper.extract_token_data(args.url)
        
        # Print result
        print(json.dumps(result, indent=2))
        
    finally:
        # Always close the driver when done
        scraper.close_driver()


if __name__ == "__main__":
    main() 