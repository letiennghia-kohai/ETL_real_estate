import os
import time
import json
import re
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyvirtualdisplay import Display
from airflow.exceptions import AirflowSkipException

from .base_extractor import BaseExtractor


class DothiExtractor(BaseExtractor):
    """
    Extractor for dothi.net
    """
    
    def __init__(self, 
                 base_url: str = "https://dothi.net", 
                 start_path: str = "/nha-dat-ban.htm",
                 max_pages: int = 2,
                 data_path: str = "/opt/airflow/data/raw",
                 headless: bool = False,
                 use_selenium: bool = True,
                 driver_port: int = 9517,
                 *args, **kwargs):
        """
        Initialize the Dothi extractor
        
        Args:
            base_url: Base URL for dothi.net
            start_path: Path to start extraction from
            max_pages: Maximum number of pages to extract
            data_path: Path where extracted data will be stored
            headless: Whether to run browser in headless mode
            use_selenium: Whether to use Selenium (True) or requests/BS4 (False)
            driver_port: Port for ChromeDriver
        """
        super().__init__(
            source_name="dothi",
            base_url=base_url,
            max_items=max_pages * 20,  # Assuming ~20 listings per page
            data_path=data_path,
            *args, **kwargs
        )
        
        self.start_path = start_path
        self.full_url = f"{base_url}{start_path}"
        self.max_pages = max_pages
        self.headless = headless
        self.use_selenium = use_selenium
        self.driver = None
        self.wait = None
        self.display = None
        self.session = requests.Session()
        self.driver_port = driver_port
        
        # Set up headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }
        
        # Initialize last processed ID tracking
        self.last_id_file = os.path.join(self.data_path, f"{self.source_name}_last_id.txt")
        self.last_processed_id = self._load_last_processed_id()

    def _setup_driver(self):
        """Set up Selenium WebDriver if needed"""
        if not self.use_selenium or self.driver is not None:
            return True

        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            self.logger.info("Setting up Selenium WebDriver")
            
            # Start virtual display
            self.display = Display(visible=0, size=(1920, 1080))
            self.display.start()

            options = webdriver.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            
            if self.headless:
                options.add_argument("--headless=new")
                
            # Try undetected-chromedriver first
            try:
                import undetected_chromedriver as uc
                service = Service(port=self.driver_port)
                self.driver = uc.Chrome(
                    options=options,
                    service=service
                )
                self.logger.info("Using undetected-chromedriver")
            except ImportError:
                self.driver = webdriver.Chrome(
                    options=options,
                    service=Service(port=self.driver_port)
                )
                self.logger.info("Using standard Selenium Chrome driver")
                
            self.wait = WebDriverWait(self.driver, 15)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set up WebDriver: {str(e)}")
            if hasattr(self, 'display'):
                self.display.stop()
            return False

    def _close_driver(self):
        """Close the Selenium WebDriver if it's open"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                self.wait = None
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {str(e)}")
        if hasattr(self, 'display'):
            try:
                self.display.stop()
            except Exception as e:
                self.logger.error(f"Error stopping display: {str(e)}")
    def _load_last_processed_id(self) -> Optional[str]:
        """Load last processed ID from file"""
        try:
            if os.path.exists(self.last_id_file):
                with open(self.last_id_file, 'r') as f:
                    return f.read().strip()
        except Exception as e:
            self.logger.warning(f"Error loading last processed ID: {str(e)}")
        return None

    def _save_last_processed_id(self, last_id: str) -> None:
        """Save last processed ID to file"""
        try:
            os.makedirs(os.path.dirname(self.last_id_file), exist_ok=True)
            with open(self.last_id_file, 'w') as f:
                f.write(str(last_id))
            self.logger.info(f"Saved last processed ID: {last_id}")
        except Exception as e:
            self.logger.error(f"Error saving last processed ID: {str(e)}")

    def get_items_list(self, page_url: str = None) -> List[Dict[str, Any]]:
        """
        Get a list of property listings from a page
        
        Args:
            page_url: URL of the page to extract from
            
        Returns:
            List of basic listing information
        """
        if page_url is None:
            page_url = self.full_url
            
        listing_items = []
        
        try:
            if self.use_selenium:
                self.driver.get(page_url)
                self.random_sleep(3, 5)
                
                # Wait for listings to load
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.listProduct li"))
                )
                
                listings = self.driver.find_elements(By.CSS_SELECTOR, "div.listProduct li")
                
                for listing in listings:
                    try:
                        link_element = listing.find_element(By.CSS_SELECTOR, "h3 a")
                        url = link_element.get_attribute("href")
                        title = link_element.text.strip()
                        
                        # Extract ID from URL
                        match = re.search(r'-pr(\d+)\.htm$', url)
                        listing_id = match.group(1) if match else None 
                        
                        listing_items.append({
                            "id": listing_id,
                            "url": url,
                            "title": title,
                            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except NoSuchElementException:
                        continue
            else:
                response = self.session.get(page_url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                listings = soup.select("div.listProduct li")
                
                for listing in listings:
                    try:
                        link = listing.select_one("h3 a")
                        if link:
                            url = link.get("href")
                            title = link.text.strip()
                            
                            # Extract ID from URL
                            listing_id = re.search(r'-(\d+)\.htm$', url)
                            listing_id = listing_id.group(1) if listing_id else ""
                            
                            if not url.startswith("http"):
                                url = f"{self.base_url}{url}"
                                
                            listing_items.append({
                                "id": listing_id,
                                "url": url,
                                "title": title,
                                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                    except Exception as e:
                        self.logger.warning(f"Error extracting listing: {str(e)}")
                        continue
                        
            self.logger.info(f"Found {len(listing_items)} listings on {page_url}")
            return listing_items
            
        except Exception as e:
            self.logger.error(f"Error getting listings from {page_url}: {str(e)}")
            return []

    def extract_item_details(self, item_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract detailed information for a specific listing
        
        Args:
            item_info: Basic information about the listing
            
        Returns:
            Dictionary with detailed listing information
        """
        if not item_info or "url" not in item_info:
            return {"error": "Invalid item info"}
            
        url = item_info["url"]
        
        try:
            listing_data = {
                **item_info,
                "price": "",
                "area": "",
                "location": "",
                "description": "",
                "details": {},
                "contact": {},
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if self.use_selenium:
                self.driver.get(url)
                self.random_sleep(3, 5)
                
                # Wait for page to load
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-detail"))
                )
                
                # Extract basic info
                try:
                    listing_data["title"] = self.driver.find_element(
                        By.CSS_SELECTOR, "div.product-detail h1"
                    ).text.strip()
                except NoSuchElementException:
                    pass
                
                # Extract price and area
                try:
                    price_div = self.driver.find_element(By.CSS_SELECTOR, "div.pd-price")
                    listing_data["price"] = price_div.find_element(
                        By.CSS_SELECTOR, "span.spanprice"
                    ).text.strip()
                    
                    spans = price_div.find_elements(By.TAG_NAME, "span")
                    if len(spans) > 1:
                        listing_data["area"] = spans[1].text.strip()
                except NoSuchElementException:
                    pass
                
                # Extract location
                try:
                    location_div = self.driver.find_element(By.CSS_SELECTOR, "div.pd-location")
                    listing_data["location"] = location_div.text.replace(
                        "Khu vực:", ""
                    ).strip()
                except NoSuchElementException:
                    pass
                
                # Extract description
                try:
                    desc_div = self.driver.find_element(By.CSS_SELECTOR, "div.pd-desc-content")
                    listing_data["description"] = desc_div.text.strip()
                except NoSuchElementException:
                    pass
                
                # Extract details from table
                try:
                    rows = self.driver.find_elements(
                        By.CSS_SELECTOR, "div.pd-dacdiem table#tbl1 tbody tr"
                    )
                    
                    for row in rows:
                        try:
                            cols = row.find_elements(By.TAG_NAME, "td")
                            if len(cols) >= 2:
                                key = cols[0].text.replace(":", "").strip()
                                value = cols[1].text.strip()
                                listing_data["details"][key] = value
                        except:
                            continue
                except NoSuchElementException:
                    pass
                
                # Extract contact info
                try:
                    contact_rows = self.driver.find_elements(
                        By.CSS_SELECTOR, "div.pd-contact table#tbl2 tr"
                    )
                    
                    for row in contact_rows:
                        try:
                            cols = row.find_elements(By.TAG_NAME, "td")
                            if len(cols) >= 2:
                                key = cols[0].text.replace(":", "").strip()
                                value = cols[1].text.strip()
                                listing_data["contact"][key] = value
                        except:
                            continue
                except NoSuchElementException:
                    pass
                
            else:
                response = self.session.get(url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract basic info
                try:
                    title = soup.select_one("div.product-detail h1")
                    if title:
                        listing_data["title"] = title.text.strip()
                except:
                    pass
                
                # Extract price and area
                try:
                    price_div = soup.select_one("div.pd-price")
                    if price_div:
                        price = price_div.select_one("span.spanprice")
                        if price:
                            listing_data["price"] = price.text.strip()
                        
                        spans = price_div.find_all("span")
                        if len(spans) > 1:
                            listing_data["area"] = spans[1].text.strip()
                except:
                    pass
                
                # Extract location
                try:
                    location_div = soup.select_one("div.pd-location")
                    if location_div:
                        listing_data["location"] = location_div.text.replace(
                            "Khu vực:", ""
                        ).strip()
                except:
                    pass
                
                # Extract description
                try:
                    desc_div = soup.select_one("div.pd-desc-content")
                    if desc_div:
                        listing_data["description"] = desc_div.text.strip()
                except:
                    pass
                
                # Extract details from table
                try:
                    rows = soup.select("div.pd-dacdiem table#tbl1 tbody tr")
                    
                    for row in rows:
                        try:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                key = cols[0].text.replace(":", "").strip()
                                value = cols[1].text.strip()
                                listing_data["details"][key] = value
                        except:
                            continue
                except:
                    pass
                
                # Extract contact info
                try:
                    rows = soup.select("div.pd-contact table#tbl2 tr")
                    
                    for row in rows:
                        try:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                key = cols[0].text.replace(":", "").strip()
                                value = cols[1].text.strip()
                                listing_data["contact"][key] = value
                        except:
                            continue
                except:
                    pass
            
            self.logger.info(f"Extracted details for listing {listing_data.get('id', 'N/A')}")
            return listing_data
            
        except Exception as e:
            error_msg = f"Error extracting details from {url}: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg, "url": url}

    def extract(self) -> List[Dict[str, Any]]:
        """
        Main extraction method with ID checkpointing
        """
        try:
            self.metrics["start_time"] = datetime.now()
            
            if self.use_selenium and not self._setup_driver():
                raise Exception("Failed to set up WebDriver")
            
            all_listings = []
            results = []
            
            for page_num in range(1, self.max_pages + 1):
                try:
                    page_url = f"{self.full_url}" if page_num == 1 else f"{self.base_url}/ban-nha-dat/p{page_num}.htm"
                    self.logger.info(f"Extracting page {page_num}/{self.max_pages}: {page_url}")
                    
                    listings = self.get_items_list(page_url)
                    self.metrics["total_items_found"] += len(listings)
                    
                    # Check if we've reached the last processed ID
                    if self.last_processed_id:
                        current_ids = [item['id'] for item in listings if 'id' in item]
                        if self.last_processed_id in current_ids:
                            self.logger.info(f"Found last processed ID {self.last_processed_id} in current page. Stopping extraction.")
                            return []
                    
                    # Process each listing
                    for listing in listings:
                        if self.metrics["items_extracted"] >= self.max_items:
                            self.logger.info(f"Reached maximum items limit ({self.max_items})")
                            break
                            
                        detail_data = self.extract_item_details(listing)
                        
                        if detail_data and "error" not in detail_data:
                            if "id" in detail_data and detail_data["id"]:
                                self.save_item_to_json(detail_data, detail_data["id"])
                                self._save_last_processed_id(detail_data["id"])
                            
                            results.append(detail_data)
                            self.metrics["items_extracted"] += 1
                        else:
                            self.metrics["failures"] += 1
                            
                        self.random_sleep(1, 3)
                    
                    self.random_sleep(2, 5)  # Delay between pages
                    
                except AirflowSkipException:
                    raise
                except Exception as e:
                    self.logger.error(f"Error processing page {page_num}: {str(e)}")
                    continue
            
            # Save batch results
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.save_to_json(results, batch_id)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            raise
        finally:
            if self.use_selenium:
                self._close_driver()
            self.metrics["end_time"] = datetime.now()
            self.log_extraction_report()

    def save_data(self, data, filepath):
            """
            Save data to a JSON file, appending to existing data rather than overwriting.
            If records have the same ID but different timestamps, both will be preserved.
            
            Args:s
                data: The data to save (list of dictionaries)
                filepath: Path where the data should be saved
            
            Returns:
                str: Path to the saved file
            """
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            existing_data = []
            
            # Load existing data if file exists
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    self.logger.info(f"Loaded {len(existing_data)} existing records from {filepath}")
                except Exception as e:
                    self.logger.warning(f"Error loading existing data from {filepath}: {str(e)}")
                    # If file exists but can't be read, create a backup
                    if os.path.getsize(filepath) > 0:
                        backup_path = f"{filepath}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        try:
                            os.rename(filepath, backup_path)
                            self.logger.info(f"Created backup of corrupted file: {backup_path}")
                        except Exception as be:
                            self.logger.error(f"Failed to create backup: {str(be)}")
            
            # Check if data is a list
            if not isinstance(data, list):
                data = [data]
            
            # Create an index of existing items by ID and timestamp
            existing_index = {}
            for item in existing_data:
                if 'id' in item and 'scraped_at' in item:
                    key = f"{item['id']}_{item['scraped_at']}"
                    existing_index[key] = True
            
            # Filter out exact duplicates (same ID and same timestamp)
            new_items = []
            for item in data:
                should_add = True
                if 'id' in item and 'scraped_at' in item:
                    key = f"{item['id']}_{item['scraped_at']}"
                    if key in existing_index:
                        should_add = False
                        
                if should_add:
                    new_items.append(item)
            
            # Merge existing and new data
            combined_data = existing_data + new_items
            
            # Save combined data
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(combined_data, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"Successfully saved {len(combined_data)} records to {filepath} (added {len(new_items)} new items)")
                return filepath
            except Exception as e:
                self.logger.error(f"Error saving data to {filepath}: {str(e)}")
                raise