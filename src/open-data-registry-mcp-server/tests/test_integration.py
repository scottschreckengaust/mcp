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

"""Integration tests for Open Data Registry MCP Server."""

import pytest
from awslabs.open_data_registry_mcp_server.config import Config
from awslabs.open_data_registry_mcp_server.dataset_service import DatasetService
from awslabs.open_data_registry_mcp_server.registry_service import RegistryService
from awslabs.open_data_registry_mcp_server.sample_service import SampleService


@pytest.mark.asyncio
@pytest.mark.live
class TestRegistryServiceIntegration:
    """Integration tests for RegistryService with real data."""
    
    async def test_fetch_dataset_list(self):
        """Test fetching the dataset list from the registry."""
        service = RegistryService()
        try:
            datasets = await service.fetch_dataset_list()
            assert isinstance(datasets, list)
            assert len(datasets) > 0
            # Check that we have some known datasets
            assert any('1000-genomes' in name for name in datasets)
        finally:
            await service.close()
    
    async def test_fetch_dataset_yaml(self):
        """Test fetching a specific dataset YAML."""
        service = RegistryService()
        try:
            # Test with a known dataset
            dataset_data = await service.fetch_dataset_yaml('1000-genomes')
            assert isinstance(dataset_data, dict)
            assert 'Name' in dataset_data or '_name' in dataset_data
            assert 'Description' in dataset_data
        finally:
            await service.close()
    
    async def test_search_datasets(self):
        """Test searching datasets."""
        service = RegistryService()
        try:
            results = await service.search_datasets(query='genomics')
            assert isinstance(results, list)
            # Should find at least one genomics-related dataset
            assert len(results) > 0
        finally:
            await service.close()


@pytest.mark.asyncio
@pytest.mark.live
class TestDatasetServiceIntegration:
    """Integration tests for DatasetService with real data."""
    
    async def test_search_datasets(self):
        """Test dataset search functionality."""
        registry_service = RegistryService()
        dataset_service = DatasetService(registry_service)
        
        try:
            # Test basic search
            results = await dataset_service.search_datasets(query='data', limit=5)
            assert isinstance(results, list)
            assert len(results) <= 5
            
            # Test category search
            categories = await dataset_service.list_categories()
            if categories:
                first_category = categories[0].name
                category_results = await dataset_service.search_datasets(
                    category=first_category, 
                    limit=3
                )
                assert isinstance(category_results, list)
                assert len(category_results) <= 3
                
        finally:
            await registry_service.close()
    
    async def test_get_dataset_details(self):
        """Test getting detailed dataset information."""
        registry_service = RegistryService()
        dataset_service = DatasetService(registry_service)
        
        try:
            details = await dataset_service.get_dataset_details('1000-genomes')
            assert details.name
            assert details.description
            assert details.provider
            assert details.category
            assert details.license
            
        finally:
            await registry_service.close()
    
    async def test_list_categories(self):
        """Test listing dataset categories."""
        registry_service = RegistryService()
        dataset_service = DatasetService(registry_service)
        
        try:
            categories = await dataset_service.list_categories()
            assert isinstance(categories, list)
            assert len(categories) > 0
            
            # Check category structure
            for category in categories:
                assert hasattr(category, 'name')
                assert hasattr(category, 'count')
                assert category.count > 0
                
        finally:
            await registry_service.close()


@pytest.mark.asyncio
@pytest.mark.live
class TestSampleServiceIntegration:
    """Integration tests for SampleService with real data."""
    
    async def test_get_sample_data(self):
        """Test getting sample data for a dataset."""
        registry_service = RegistryService()
        sample_service = SampleService()
        
        try:
            # Get dataset metadata first
            dataset_data = await registry_service.fetch_dataset_yaml('1000-genomes')
            
            # Try to get sample data
            sample_response = await sample_service.get_sample_data(
                dataset_name='1000-genomes',
                dataset_data=dataset_data,
                sample_size=10
            )
            
            assert sample_response.dataset_name == '1000-genomes'
            assert sample_response.sample_type in ['data', 'schema', 'documentation']
            assert sample_response.notes is not None
            
        finally:
            await registry_service.close()
            await sample_service.close()


@pytest.mark.asyncio
@pytest.mark.live
class TestEndToEndWorkflow:
    """End-to-end workflow tests."""
    
    async def test_complete_dataset_discovery_workflow(self):
        """Test a complete dataset discovery workflow."""
        registry_service = RegistryService()
        dataset_service = DatasetService(registry_service)
        sample_service = SampleService()
        
        try:
            # Step 1: Search for datasets
            search_results = await dataset_service.search_datasets(
                query='climate', 
                limit=3
            )
            assert len(search_results) > 0
            
            # Step 2: Get details for first result
            first_dataset = search_results[0]
            details = await dataset_service.get_dataset_details(first_dataset.name)
            assert details.name == first_dataset.name
            
            # Step 3: Try to get sample data
            dataset_data = await registry_service.fetch_dataset_yaml(first_dataset.name)
            sample_response = await sample_service.get_sample_data(
                dataset_name=first_dataset.name,
                dataset_data=dataset_data,
                sample_size=5
            )
            assert sample_response.dataset_name == first_dataset.name
            
            # Step 4: List categories
            categories = await dataset_service.list_categories()
            assert len(categories) > 0
            
        finally:
            await registry_service.close()
            await sample_service.close()


@pytest.mark.asyncio
class TestErrorHandling:
    """Test error handling scenarios."""
    
    async def test_nonexistent_dataset(self):
        """Test handling of nonexistent dataset requests."""
        registry_service = RegistryService()
        dataset_service = DatasetService(registry_service)
        
        try:
            with pytest.raises(ValueError):
                await dataset_service.get_dataset_details('nonexistent-dataset-12345')
        finally:
            await registry_service.close()
    
    async def test_invalid_search_parameters(self):
        """Test handling of invalid search parameters."""
        registry_service = RegistryService()
        dataset_service = DatasetService(registry_service)
        
        try:
            # Test negative offset
            with pytest.raises(ValueError):
                await dataset_service.search_datasets(offset=-1)
            
            # Test zero limit
            with pytest.raises(ValueError):
                await dataset_service.search_datasets(limit=0)
                
        finally:
            await registry_service.close()


@pytest.mark.asyncio
class TestConfiguration:
    """Test configuration and environment variables."""
    
    def test_config_validation(self):
        """Test configuration validation."""
        # This should not raise an exception
        Config.validate()
        
        # Test that required config values are set
        assert Config.REGISTRY_BASE_URL
        assert Config.REQUEST_TIMEOUT > 0
        assert Config.MAX_SAMPLE_SIZE > 0
        assert Config.DEFAULT_SEARCH_LIMIT > 0