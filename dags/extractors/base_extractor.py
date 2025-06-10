"""
Base Extractor Module
Provides a base class for all data extractors in the pipeline.
"""

import os
import time
import json
import random
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors
    """
    
    def __init__(self, 
                 source_name: str, 
                 base_url: str, 
                 max_items: int = 10,
                 data_path: str = "/opt/airflow/data/raw"):
        """
        Initialize the base extractor
        
        Args:
            source_name: Name of the data source
            base_url: Base URL for the data source
            max_items: Maximum number of items to extract
            data_path: Path where extracted data will be stored
        """
        self.source_name = source_name
        self.base_url = base_url
        self.max_items = max_items
        self.data_path = data_path
        self.logger = logging.getLogger(f"extractor.{source_name}")
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.join(data_path, source_name), exist_ok=True)
        
        # Track extraction metrics
        self.metrics = {
            "start_time": None,
            "end_time": None,
            "total_items_found": 0,
            "items_extracted": 0,
            "failures": 0
        }
    
    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Main method to extract data from source
        
        Returns:
            List of extracted items as dictionaries
        """
        pass
    
    @abstractmethod
    def get_items_list(self) -> List[Dict[str, Any]]:
        """
        Get a list of items to be extracted
        
        Returns:
            List of items with basic information
        """
        pass
    
    @abstractmethod
    def extract_item_details(self, item_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract detailed information for a specific item
        
        Args:
            item_info: Basic information about the item
            
        Returns:
            Dictionary with detailed item information
        """
        pass
    
    def save_to_json(self, data: List[Dict[str, Any]], batch_id: str = None) -> str:
        """
        Save extracted data to a JSON file
        
        Args:
            data: List of data items to save
            batch_id: Optional batch identifier
            
        Returns:
            Path to the saved file
        """
        if not batch_id:
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
        # Create directory if it doesn't exist
        output_dir = os.path.join(self.data_path, self.source_name.replace("\\", "/"))
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{self.source_name}_{batch_id}.json"
        filepath = os.path.join(output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Saved {len(data)} items to {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"Failed to save data to JSON: {str(e)}")
            raise
    
    def save_item_to_json(self, item: Dict[str, Any], item_id: str) -> str:
        """
        Save a single item to its own JSON file
        
        Args:
            item: Data item to save
            item_id: Unique identifier for the item
            
        Returns:
            Path to the saved file
        """
        filename = f"{item_id}.json"
        filepath = os.path.join(self.data_path, self.source_name, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(item, f, ensure_ascii=False, indent=2)
            
        return filepath
    
    def log_extraction_report(self) -> Dict[str, Any]:
        """
        Log and return extraction metrics
        
        Returns:
            Dictionary with extraction metrics
        """
        if self.metrics["end_time"] is None:
            self.metrics["end_time"] = datetime.now()
            
        duration = (self.metrics["end_time"] - self.metrics["start_time"]).total_seconds()
        
        report = {
            **self.metrics,
            "start_time": self.metrics["start_time"].isoformat(),
            "end_time": self.metrics["end_time"].isoformat(),
            "duration_seconds": duration,
            "source_name": self.source_name
        }
        
        self.logger.info(f"Extraction completed: {report}")
        return report
    
    @staticmethod
    def random_sleep(min_sec: float = 1.0, max_sec: float = 3.0) -> None:
        """
        Sleep for a random amount of time to mimic human behavior
        
        Args:
            min_sec: Minimum sleep time in seconds
            max_sec: Maximum sleep time in seconds
        """
        time.sleep(random.uniform(min_sec, max_sec))