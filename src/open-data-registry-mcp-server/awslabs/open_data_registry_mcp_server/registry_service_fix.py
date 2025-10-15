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

"""Fixed Registry service for accessing Open Data Registry from GitHub."""

import asyncio
import json
import re
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
            # Use GitHub API to fetch the actual list of dataset files
            # The Open Data Registry maintains its datasets in a specific GitHub repo
            github_api_url = "https://api.github.com/repos/awslabs/open-data-registry/contents/datasets"
            
            client = await self._get_client()
            
            # Try GitHub API first
            try:
                response = await client.get(github_api_url)
                response.raise_for_status()
                
                files = response.json()
                dataset_names = []
                
                for file_info in files:
                    if file_info.get('name', '').endswith('.yaml'):
                        # Remove .yaml extension to get dataset name
                        dataset_name = file_info['name'][:-5]
                        dataset_names.append(dataset_name)
                
                logger.info(f"Fetched {len(dataset_names)} datasets from GitHub API")
                
                if dataset_names:
                    self._cache_set(cache_key, dataset_names)
                    return dataset_names
                
            except Exception as e:
                logger.warning(f"GitHub API failed, falling back to alternative method: {e}")
            
            # Alternative method: Try fetching the directory listing from raw GitHub
            # This approach fetches the HTML page and parses dataset names
            raw_url = "https://github.com/awslabs/open-data-registry/tree/main/datasets"
            
            try:
                response = await client.get(raw_url)
                response.raise_for_status()
                
                # Parse HTML to extract .yaml filenames
                # Look for patterns like "datasets/dataset-name.yaml"
                pattern = r'datasets/([^/]+)\.yaml'
                matches = re.findall(pattern, response.text)
                
                if matches:
                    dataset_names = list(set(matches))  # Remove duplicates
                    logger.info(f"Fetched {len(dataset_names)} datasets from HTML parsing")
                    self._cache_set(cache_key, dataset_names)
                    return dataset_names
                    
            except Exception as e:
                logger.warning(f"HTML parsing failed: {e}")
            
            # Final fallback: Use an expanded hardcoded list
            # This ensures the service works even if external fetching fails
            fallback_datasets = [
                "1000-genomes",
                "allen-brain-atlas", 
                "amazon-reviews",
                "aws-covid-19-data-lake",
                "aws-public-blockchain",
                "broad-references",
                "cancer-dependency-map",
                "ccle",
                "chembl",
                "climate-change-knowledge-portal",
                "cmap",
                "cmip6",
                "collab",
                "common-crawl",
                "copernicus-dem-30m",
                "covid19-lake",
                "encode-project",
                "era5",
                "foldingathome-covid19",
                "gdelt",
                "gedi",
                "gnomad",
                "goes-16",
                "goes-17",
                "gtex",
                "hca",
                "hrrr",
                "human-microbiome-project",
                "inat",
                "landsat-8",
                "modis",
                "naip",
                "nasa-nex",
                "nasa-nex-gddp",
                "ncbi-sra",
                "nexrad",
                "noaa-cdr",
                "noaa-ghcn",
                "noaa-goes",
                "noaa-isd",
                "nwm-archive",
                "openaq",
                "openneuro",
                "openstreetmap",
                "orca",
                "patentsview",
                "pdb",
                "planet-nicfi",
                "pubchem",
                "rapid",
                "roda",
                "sentinel-1",
                "sentinel-2",
                "sentinel-2-l2a-cogs",
                "sentinel-3",
                "sentinel-5p",
                "silam",
                "snomed-ct-us",
                "spacenet",
                "target",
                "tcga",
                "terraclimate",
                "topmed",
                "usgs-landsat",
                "usgs-lidar",
                "wrf-se-ak-ar5"
            ]
            
            logger.warning(f"Using fallback dataset list with {len(fallback_datasets)} datasets")
            self._cache_set(cache_key, fallback_datasets)
            return fallback_datasets
            
        except Exception as e:
            logger.error(f"Failed to fetch dataset list: {e}")
            # Return empty list rather than failing completely
            return []
    
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
            category_descriptions: Dict[str, str] = {
                "Aerospace": "Aviation and space-related datasets",
                "Agriculture": "Agricultural and farming data",
                "Astronomy": "Astronomical observations and space data",
                "Atmospheric Science": "Weather and atmospheric datasets",
                "Biology": "Biological and life sciences data",
                "Chemistry": "Chemical compounds and molecular data",
                "Climate": "Climate and environmental datasets",
                "Demographics": "Population and demographic statistics",
                "Economics": "Economic indicators and financial data",
                "Education": "Educational resources and statistics",
                "Energy": "Energy production and consumption data",
                "Environmental": "Environmental monitoring and conservation data",
                "Genomics": "Genetic and genomic sequence data",
                "Geospatial": "Geographic and mapping datasets",
                "Health": "Health and medical datasets",
                "Imaging": "Image collections and visual data",
                "Machine Learning": "Datasets for ML training and research",
                "Meteorology": "Weather and meteorological data",
                "Neuroscience": "Brain and neurological research data",
                "Oceanography": "Ocean and marine datasets",
                "Physics": "Physical sciences datasets",
                "Satellite Imagery": "Earth observation and satellite data",
                "Social Science": "Social and behavioral research data",
                "Transportation": "Traffic and transportation datasets"
            }
            
            # Process datasets to count categories
            for name in dataset_names:
                try:
                    dataset_data = await self.fetch_dataset_yaml(name)
                    
                    # Try different locations where category might be stored
                    category = None
                    
                    # Check Tags.Category
                    if 'Tags' in dataset_data and isinstance(dataset_data['Tags'], dict):
                        category = dataset_data['Tags'].get('Category')
                    
                    # Check top-level Category
                    if not category and 'Category' in dataset_data:
                        category = dataset_data['Category']
                    
                    # Default to Unknown if no category found
                    if not category:
                        category = 'Unknown'
                    
                    # Handle list of categories (take first one)
                    if isinstance(category, list) and category:
                        category = category[0]
                    
                    category = str(category)
                    category_counts[category] = category_counts.get(category, 0) + 1
                    
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} for category counting: {e}")
                    continue
            
            # Create category list with descriptions
            categories = []
            for name, count in sorted(category_counts.items()):
                categories.append({
                    'name': name,
                    'count': count,
                    'description': category_descriptions.get(name, f"Datasets in {name} category")
                })
            
            logger.info(f"Found {len(categories)} categories across {sum(category_counts.values())} datasets")
            return categories
            
        except Exception as e:
            logger.error(f"Failed to get categories: {e}")
            # Return some default categories if fetching fails
            return [
                {'name': 'Unknown', 'count': 0, 'description': 'Uncategorized datasets'},
                {'name': 'Error', 'count': 0, 'description': f'Failed to fetch categories: {str(e)}'}
            ]
