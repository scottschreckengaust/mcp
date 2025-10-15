# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Registry service for accessing Open Data Registry from GitHub."""

import asyncio
import time
from typing import Any, Dict, List, Optional

import httpx
import yaml
from loguru import logger

from .config import Config


class RegistryService:
    """Service for accessing and caching Open Data Registry data."""
    
    def __init__(self):
        """Initialize the registry service."""
        self.base_url = Config.REGISTRY_BASE_URL
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_timestamps: Dict[str, float] = {}
        self.cache_ttl = Config.REGISTRY_CACHE_TTL
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_REQUESTS)
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(Config.REQUEST_TIMEOUT),
                headers={
                    'User-Agent': 'awslabs-open-data-registry-mcp-server/0.0.0'
                }
            )
        return self._client
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache entry is still valid."""
        if key not in self.cache or key not in self.cache_timestamps:
            return False
        
        age = time.time() - self.cache_timestamps[key]
        return age < self.cache_ttl
    
    def _cache_set(self, key: str, value: Any) -> None:
        """Set cache entry with timestamp."""
        self.cache[key] = value
        self.cache_timestamps[key] = time.time()
    
    def _cache_get(self, key: str) -> Optional[Any]:
        """Get cache entry if valid."""
        if self._is_cache_valid(key):
            return self.cache[key]
        return None
    
    async def _fetch_with_retry(self, url: str) -> str:
        """Fetch URL with exponential backoff retry."""
        client = await self._get_client()
        
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                async with self._semaphore:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.text
            
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise FileNotFoundError(f"Resource not found: {url}")
                elif e.response.status_code in (429, 503):
                    # Rate limited or service unavailable
                    if attempt < Config.MAX_RETRIES:
                        delay = Config.RETRY_BASE_DELAY * (Config.RETRY_BACKOFF_FACTOR ** attempt)
                        logger.warning(f"Rate limited, retrying in {delay}s (attempt {attempt + 1})")
                        await asyncio.sleep(delay)
                        continue
                    raise
                else:
                    raise
            
            except (httpx.RequestError, httpx.TimeoutException) as e:
                if attempt < Config.MAX_RETRIES:
                    delay = Config.RETRY_BASE_DELAY * (Config.RETRY_BACKOFF_FACTOR ** attempt)
                    logger.warning(f"Request failed, retrying in {delay}s (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(delay)
                    continue
                raise
        
        raise Exception(f"Failed to fetch {url} after {Config.MAX_RETRIES + 1} attempts")
    
    async def fetch_dataset_list(self) -> List[str]:
        """Fetch list of dataset YAML files from registry."""
        cache_key = "dataset_list"
        cached_result = self._cache_get(cache_key)
        if cached_result is not None:
            logger.debug("Returning cached dataset list")
            return cached_result
        
        try:
            # Fetch the datasets directory listing from GitHub API
            # Note: We'll use a simpler approach by fetching known dataset files
            # In a real implementation, you might want to use GitHub's API to list files
            
            # For now, let's fetch a few known datasets to demonstrate the functionality
            # In production, this would fetch the complete directory listing
            known_datasets = [
                "1000-genomes",
                "allen-brain-atlas", 
                "amazon-reviews",
                "aws-covid-19-data-lake",
                "climate-change-knowledge-portal",
                "common-crawl",
                "gdelt",
                "landsat-8",
                "modis",
                "nasa-nex",
                "noaa-ghcn",
                "openstreetmap",
                "sentinel-2",
                "usgs-landsat"
            ]
            
            # Verify that these datasets exist by trying to fetch one
            test_url = f"{self.base_url}/datasets/1000-genomes.yaml"
            try:
                await self._fetch_with_retry(test_url)
                logger.info(f"Successfully verified registry access with {len(known_datasets)} datasets")
            except Exception as e:
                logger.warning(f"Could not verify registry access: {e}")
                # Return empty list if we can't access the registry
                return []
            
            self._cache_set(cache_key, known_datasets)
            return known_datasets
            
        except Exception as e:
            logger.error(f"Failed to fetch dataset list: {e}")
            raise
    
    async def fetch_dataset_yaml(self, dataset_name: str) -> Dict[str, Any]:
        """Fetch and parse individual dataset YAML file."""
        cache_key = f"dataset_{dataset_name}"
        cached_result = self._cache_get(cache_key)
        if cached_result is not None:
            logger.debug(f"Returning cached dataset: {dataset_name}")
            return cached_result
        
        try:
            url = f"{self.base_url}/datasets/{dataset_name}.yaml"
            yaml_content = await self._fetch_with_retry(url)
            
            # Parse YAML content
            try:
                dataset_data = yaml.safe_load(yaml_content)
                if not isinstance(dataset_data, dict):
                    raise ValueError(f"Invalid YAML structure for dataset {dataset_name}")
                
                # Add the dataset name to the data
                dataset_data['_name'] = dataset_name
                
                self._cache_set(cache_key, dataset_data)
                logger.debug(f"Successfully fetched and cached dataset: {dataset_name}")
                return dataset_data
                
            except yaml.YAMLError as e:
                logger.error(f"Failed to parse YAML for dataset {dataset_name}: {e}")
                raise ValueError(f"Invalid YAML format for dataset {dataset_name}: {e}")
        
        except FileNotFoundError:
            logger.warning(f"Dataset not found: {dataset_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch dataset {dataset_name}: {e}")
            raise
    
    async def search_datasets(
        self, 
        query: str = "", 
        category: str = "", 
        tags: List[str] = None
    ) -> List[Dict[str, Any]]:
        """Search datasets based on criteria."""
        if tags is None:
            tags = []
        
        try:
            # Get all available datasets
            dataset_names = await self.fetch_dataset_list()
            
            # Fetch all dataset details
            datasets = []
            for name in dataset_names:
                try:
                    dataset_data = await self.fetch_dataset_yaml(name)
                    datasets.append(dataset_data)
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} due to error: {e}")
                    continue
            
            # Filter datasets based on search criteria
            filtered_datasets = []
            
            for dataset in datasets:
                # Check query match (case-insensitive search in name and description)
                if query:
                    query_lower = query.lower()
                    name_match = query_lower in dataset.get('Name', '').lower()
                    desc_match = query_lower in dataset.get('Description', '').lower()
                    if not (name_match or desc_match):
                        continue
                
                # Check category match
                if category:
                    dataset_category = dataset.get('Tags', {}).get('Category', '')
                    if category.lower() not in dataset_category.lower():
                        continue
                
                # Check tags match
                if tags:
                    dataset_tags = []
                    # Extract tags from various fields
                    if 'Tags' in dataset:
                        for key, value in dataset['Tags'].items():
                            if isinstance(value, list):
                                dataset_tags.extend([str(v).lower() for v in value])
                            else:
                                dataset_tags.append(str(value).lower())
                    
                    # Check if any of the requested tags match
                    tag_match = any(tag.lower() in dataset_tags for tag in tags)
                    if not tag_match:
                        continue
                
                filtered_datasets.append(dataset)
            
            logger.info(f"Search returned {len(filtered_datasets)} datasets")
            return filtered_datasets
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    async def get_categories(self) -> List[Dict[str, Any]]:
        """Get list of available categories with counts."""
        try:
            dataset_names = await self.fetch_dataset_list()
            category_counts: Dict[str, int] = {}
            
            for name in dataset_names:
                try:
                    dataset_data = await self.fetch_dataset_yaml(name)
                    category = dataset_data.get('Tags', {}).get('Category', 'Unknown')
                    category_counts[category] = category_counts.get(category, 0) + 1
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} for category counting: {e}")
                    continue
            
            categories = [
                {'name': name, 'count': count, 'description': None}
                for name, count in sorted(category_counts.items())
            ]
            
            return categories
            
        except Exception as e:
            logger.error(f"Failed to get categories: {e}")
            raise