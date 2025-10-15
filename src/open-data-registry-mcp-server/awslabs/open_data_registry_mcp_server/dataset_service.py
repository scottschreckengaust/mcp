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

"""Dataset service for search and retrieval operations."""

import re
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from .config import Config
from .dataset_indexer import DatasetIndexer
from .models import CategoryInfo, DatasetDetails, DatasetSummary, SearchResults
from .registry_service import RegistryService


class DatasetService:
    """Service for dataset search, filtering, and retrieval operations."""
    
    def __init__(self, registry_service: RegistryService):
        """Initialize the dataset service."""
        self.registry_service = registry_service
        self.indexer = DatasetIndexer()
        self._search_index: Optional[Dict[str, Any]] = None
    
    async def _ensure_search_index(self) -> Dict[str, Any]:
        """Ensure search index is built and up to date."""
        if self._search_index is None:
            await self._build_search_index()
        return self._search_index
    
    async def _build_search_index(self) -> None:
        """Build search index from all datasets."""
        try:
            logger.info("Building search index...")
            dataset_names = await self.registry_service.fetch_dataset_list()
            datasets = []
            
            for name in dataset_names:
                try:
                    dataset_data = await self.registry_service.fetch_dataset_yaml(name)
                    datasets.append(dataset_data)
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} in index: {e}")
                    continue
            
            self._search_index = self.indexer.create_search_index(datasets)
            logger.info(f"Search index built with {len(datasets)} datasets")
            
        except Exception as e:
            logger.error(f"Failed to build search index: {e}")
            self._search_index = {
                'by_name': {},
                'by_category': {},
                'by_tag': {},
                'keywords': {}
            }
    
    async def search_datasets(
        self,
        query: str = "",
        category: str = "",
        tags: List[str] = None,
        limit: int = None,
        offset: int = 0
    ) -> List[DatasetSummary]:
        """Search for datasets based on criteria with ranking."""
        if tags is None:
            tags = []
        
        if limit is None:
            limit = Config.DEFAULT_SEARCH_LIMIT
        
        limit = min(limit, Config.MAX_SEARCH_LIMIT)
        
        # Validate pagination parameters
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        if limit <= 0:
            raise ValueError("Limit must be positive")
        
        try:
            # Get matching datasets with scores
            scored_datasets = await self._search_with_scoring(query, category, tags)
            
            # Sort by relevance score (descending)
            scored_datasets.sort(key=lambda x: x[1], reverse=True)
            
            # Apply pagination
            start_idx = max(0, offset)
            end_idx = start_idx + limit
            paginated_datasets = scored_datasets[start_idx:end_idx]
            
            # Extract datasets
            datasets = [dataset for dataset, _ in paginated_datasets]
            
            # Convert to DatasetSummary objects
            summaries = []
            for dataset in datasets:
                try:
                    summary = self.indexer.extract_dataset_summary(dataset)
                    summaries.append(summary)
                except Exception as e:
                    logger.warning(f"Failed to create summary for dataset: {e}")
                    continue
            
            logger.info(f"Search returned {len(summaries)} results (offset: {offset}, limit: {limit})")
            return summaries
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    async def search_datasets_with_pagination(
        self,
        query: str = "",
        category: str = "",
        tags: List[str] = None,
        limit: int = None,
        offset: int = 0
    ) -> SearchResults:
        """Search for datasets with detailed pagination information."""
        if tags is None:
            tags = []
        
        if limit is None:
            limit = Config.DEFAULT_SEARCH_LIMIT
        
        limit = min(limit, Config.MAX_SEARCH_LIMIT)
        
        try:
            # Get all matching datasets with scores
            scored_datasets = await self._search_with_scoring(query, category, tags)
            total_count = len(scored_datasets)
            
            # Sort by relevance score (descending)
            scored_datasets.sort(key=lambda x: x[1], reverse=True)
            
            # Apply pagination
            start_idx = max(0, offset)
            end_idx = start_idx + limit
            paginated_datasets = scored_datasets[start_idx:end_idx]
            
            # Extract datasets and convert to summaries
            summaries = []
            for dataset, _ in paginated_datasets:
                try:
                    summary = self.indexer.extract_dataset_summary(dataset)
                    summaries.append(summary)
                except Exception as e:
                    logger.warning(f"Failed to create summary for dataset: {e}")
                    continue
            
            has_more = end_idx < total_count
            
            return SearchResults(
                datasets=summaries,
                total_count=total_count,
                offset=offset,
                limit=limit,
                has_more=has_more
            )
            
        except Exception as e:
            logger.error(f"Search with pagination failed: {e}")
            raise
    
    async def _search_with_scoring(
        self,
        query: str,
        category: str,
        tags: List[str]
    ) -> List[Tuple[Dict[str, Any], float]]:
        """Search datasets and return with relevance scores."""
        index = await self._ensure_search_index()
        candidate_datasets: Dict[str, Tuple[Dict[str, Any], float]] = {}
        
        # If no search criteria, return all datasets with equal score
        if not query and not category and not tags:
            dataset_names = await self.registry_service.fetch_dataset_list()
            for name in dataset_names:
                try:
                    dataset = await self.registry_service.fetch_dataset_yaml(name)
                    candidate_datasets[name] = (dataset, 1.0)
                except Exception as e:
                    logger.warning(f"Skipping dataset {name}: {e}")
                    continue
            return list(candidate_datasets.values())
        
        # Search by query keywords
        if query:
            query_datasets = await self._search_by_query(query, index)
            for dataset, score in query_datasets:
                name = dataset.get('_name', dataset.get('Name', ''))
                if name in candidate_datasets:
                    candidate_datasets[name] = (dataset, candidate_datasets[name][1] + score)
                else:
                    candidate_datasets[name] = (dataset, score)
        
        # Filter by category
        if category:
            category_datasets = self._filter_by_category(category, index)
            if query:
                # If we have query results, filter them by category
                filtered_candidates = {}
                for name, (dataset, score) in candidate_datasets.items():
                    if any(d.get('_name') == name for d in category_datasets):
                        filtered_candidates[name] = (dataset, score + 0.5)  # Bonus for category match
                candidate_datasets = filtered_candidates
            else:
                # If no query, start with category results
                for dataset in category_datasets:
                    name = dataset.get('_name', dataset.get('Name', ''))
                    candidate_datasets[name] = (dataset, 1.0)
        
        # Filter by tags
        if tags:
            tag_datasets = self._filter_by_tags(tags, index)
            if query or category:
                # Filter existing candidates by tags
                filtered_candidates = {}
                for name, (dataset, score) in candidate_datasets.items():
                    if any(d.get('_name') == name for d in tag_datasets):
                        filtered_candidates[name] = (dataset, score + 0.3)  # Bonus for tag match
                candidate_datasets = filtered_candidates
            else:
                # If no other criteria, start with tag results
                for dataset in tag_datasets:
                    name = dataset.get('_name', dataset.get('Name', ''))
                    candidate_datasets[name] = (dataset, 1.0)
        
        return list(candidate_datasets.values())
    
    async def _search_by_query(
        self,
        query: str,
        index: Dict[str, Any]
    ) -> List[Tuple[Dict[str, Any], float]]:
        """Search datasets by query with scoring."""
        results: Dict[str, Tuple[Dict[str, Any], float]] = {}
        query_lower = query.lower()
        query_words = re.findall(r'\w+', query_lower)
        
        # Search by exact name match (highest score)
        if query_lower in index['by_name']:
            dataset = index['by_name'][query_lower]
            name = dataset.get('_name', dataset.get('Name', ''))
            results[name] = (dataset, 10.0)
        
        # Search by partial name match
        for name, dataset in index['by_name'].items():
            if query_lower in name and name not in [d[0].get('_name', '') for d in results.values()]:
                dataset_name = dataset.get('_name', dataset.get('Name', ''))
                results[dataset_name] = (dataset, 5.0)
        
        # Search by keywords in description and name
        for word in query_words:
            if word in index['keywords']:
                for dataset in index['keywords'][word]:
                    name = dataset.get('_name', dataset.get('Name', ''))
                    if name in results:
                        # Boost score for additional keyword matches
                        current_score = results[name][1]
                        results[name] = (dataset, current_score + 1.0)
                    else:
                        results[name] = (dataset, 2.0)
        
        # Search in descriptions for phrase matches
        if len(query_words) > 1:
            phrase_datasets = await self._search_phrase_in_descriptions(query)
            for dataset in phrase_datasets:
                name = dataset.get('_name', dataset.get('Name', ''))
                if name in results:
                    current_score = results[name][1]
                    results[name] = (dataset, current_score + 3.0)
                else:
                    results[name] = (dataset, 3.0)
        
        return list(results.values())
    
    async def _search_phrase_in_descriptions(self, query: str) -> List[Dict[str, Any]]:
        """Search for phrase matches in dataset descriptions."""
        matching_datasets = []
        query_lower = query.lower()
        
        try:
            dataset_names = await self.registry_service.fetch_dataset_list()
            for name in dataset_names:
                try:
                    dataset = await self.registry_service.fetch_dataset_yaml(name)
                    description = dataset.get('Description', '').lower()
                    if query_lower in description:
                        matching_datasets.append(dataset)
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} in phrase search: {e}")
                    continue
        except Exception as e:
            logger.error(f"Failed phrase search: {e}")
        
        return matching_datasets
    
    def _filter_by_category(self, category: str, index: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter datasets by category."""
        category_lower = category.lower()
        
        # Exact category match
        if category_lower in index['by_category']:
            return index['by_category'][category_lower]
        
        # Partial category match
        matching_datasets = []
        for cat, datasets in index['by_category'].items():
            if category_lower in cat:
                matching_datasets.extend(datasets)
        
        return matching_datasets
    
    def _filter_by_tags(self, tags: List[str], index: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter datasets by tags."""
        matching_datasets = []
        tags_lower = [tag.lower() for tag in tags]
        
        for tag in tags_lower:
            if tag in index['by_tag']:
                matching_datasets.extend(index['by_tag'][tag])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_datasets = []
        for dataset in matching_datasets:
            name = dataset.get('_name', dataset.get('Name', ''))
            if name not in seen:
                seen.add(name)
                unique_datasets.append(dataset)
        
        return unique_datasets
    
    async def get_dataset_details(self, dataset_name: str) -> DatasetDetails:
        """Get detailed information for a specific dataset."""
        try:
            dataset_data = await self.registry_service.fetch_dataset_yaml(dataset_name)
            details = self.indexer.extract_dataset_details(dataset_data)
            return details
            
        except FileNotFoundError:
            raise ValueError(f"Dataset '{dataset_name}' not found")
        except Exception as e:
            logger.error(f"Failed to get dataset details for {dataset_name}: {e}")
            raise
    
    async def list_categories(self) -> List[CategoryInfo]:
        """Get list of available ADXCategories with counts."""
        try:
            categories_data = await self.registry_service.get_categories()
            categories = []
            
            for cat_data in categories_data:
                categories.append(CategoryInfo(
                    name=cat_data['name'],
                    count=cat_data['count'],
                    description=cat_data.get('description')
                ))
            
            return categories
            
        except Exception as e:
            logger.error(f"Failed to list categories: {e}")
            raise
    
    async def list_tags(self) -> List[CategoryInfo]:
        """Get list of available tags with counts."""
        try:
            tags_data = await self.registry_service.get_tags()
            tags = []
            
            for tag_data in tags_data:
                tags.append(CategoryInfo(
                    name=tag_data['name'],
                    count=tag_data['count'],
                    description=None  # Tags don't have descriptions
                ))
            
            return tags
            
        except Exception as e:
            logger.error(f"Failed to list tags: {e}")
            raise
    
    async def get_dataset_names(self) -> List[str]:
        """Get list of all available dataset names."""
        try:
            return await self.registry_service.fetch_dataset_list()
        except Exception as e:
            logger.error(f"Failed to get dataset names: {e}")
            raise
    
    def calculate_relevance_score(
        self,
        dataset: Dict[str, Any],
        query: str,
        category: str,
        tags: List[str]
    ) -> float:
        """Calculate relevance score for a dataset based on search criteria."""
        score = 0.0
        
        try:
            name = dataset.get('Name', '').lower()
            description = dataset.get('Description', '').lower()
            dataset_category = dataset.get('Tags', {}).get('Category', '').lower()
            
            # Query matching
            if query:
                query_lower = query.lower()
                # Exact name match gets highest score
                if query_lower == name:
                    score += 10.0
                # Partial name match
                elif query_lower in name:
                    score += 5.0
                # Description match
                elif query_lower in description:
                    score += 3.0
                # Word matches
                query_words = re.findall(r'\w+', query_lower)
                for word in query_words:
                    if word in name:
                        score += 2.0
                    elif word in description:
                        score += 1.0
            
            # Category matching
            if category and category.lower() in dataset_category:
                score += 2.0
            
            # Tag matching
            if tags:
                dataset_tags = self.indexer._extract_tags(dataset)
                dataset_tags_lower = [tag.lower() for tag in dataset_tags]
                for tag in tags:
                    if tag.lower() in dataset_tags_lower:
                        score += 1.5
            
            return score
            
        except Exception as e:
            logger.warning(f"Failed to calculate relevance score: {e}")
            return 0.0
