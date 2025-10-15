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

"""awslabs Open Data Registry MCP Server implementation."""

import os
import sys
from typing import List

from loguru import logger
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from .config import Config
from .dataset_service import DatasetService
from .models import CategoryInfo, DatasetDetails, DatasetSummary, SampleDataResponse
from .prompts import register_prompts
from .registry_service import RegistryService
from .resources import register_resources
from .sample_service import SampleService


# Set up logging
logger.remove()
logger.add(sys.stderr, level=Config.LOG_LEVEL)

# Initialize services
registry_service = RegistryService()
dataset_service = DatasetService(registry_service)
sample_service = SampleService()

mcp = FastMCP(
    "awslabs.open-data-registry-mcp-server",
    instructions="""
    # AWS Open Data Registry MCP Server
    
    This server provides access to AWS's Open Data Registry, enabling discovery and analysis of open datasets.
    
    ## Best Practices
    - Use search_datasets to find relevant datasets by keywords or categories
    - Get detailed information with get_dataset_details before accessing data
    - Use get_sample_data to understand dataset structure before full analysis
    - Check list_categories to see available dataset types
    
    ## Tool Selection Guide
    - search_datasets: Find datasets by keywords, categories, or tags
    - get_dataset_details: Get comprehensive metadata for a specific dataset
    - get_sample_data: Preview dataset structure and sample records
    - list_categories: Browse available dataset categories
    """,
    dependencies=[
        'pydantic',
        'httpx',
        'pyyaml',
        'boto3',
        'beautifulsoup4',
    ],
)


@mcp.tool()
async def search_datasets(
    ctx: Context,
    query: str = Field(default="", description="Search keywords for datasets"),
    category: str = Field(default="", description="Filter by dataset category"),
    tags: List[str] = Field(default=[], description="Filter by tags"),
    limit: int = Field(default=10, description="Maximum results to return", gt=0, le=50)
) -> List[DatasetSummary]:
    """Search for datasets in the AWS Open Data Registry.
    
    This tool searches through the Open Data Registry to find datasets matching
    your criteria. You can search by keywords, filter by category, or specify tags.
    
    Args:
        query: Keywords to search for in dataset names and descriptions
        category: Filter results by dataset category (e.g., "climate", "genomics")
        tags: List of tags to filter by
        limit: Maximum number of results to return (1-50)
    
    Returns:
        List of matching datasets with summary information
    """
    try:
        logger.info(f"Searching datasets: query='{query}', category='{category}', tags={tags}, limit={limit}")
        
        results = await dataset_service.search_datasets(
            query=query,
            category=category,
            tags=tags,
            limit=limit
        )
        
        logger.info(f"Found {len(results)} datasets")
        return results
        
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise ValueError(f"Search failed: {str(e)}")


@mcp.tool()
async def get_dataset_details(
    ctx: Context,
    dataset_name: str = Field(description="Name/identifier of the dataset")
) -> DatasetDetails:
    """Get detailed information about a specific dataset.
    
    This tool retrieves comprehensive metadata for a dataset including licensing,
    data formats, access methods, contact information, and available resources.
    
    Args:
        dataset_name: The name or identifier of the dataset
    
    Returns:
        Detailed dataset information including metadata and access details
    """
    try:
        logger.info(f"Getting details for dataset: {dataset_name}")
        
        details = await dataset_service.get_dataset_details(dataset_name)
        
        logger.info(f"Retrieved details for dataset: {dataset_name}")
        return details
        
    except Exception as e:
        logger.error(f"Failed to get dataset details for {dataset_name}: {e}")
        raise ValueError(f"Failed to get dataset details: {str(e)}")


@mcp.tool()
async def get_sample_data(
    ctx: Context,
    dataset_name: str = Field(description="Name/identifier of the dataset"),
    sample_size: int = Field(default=100, description="Number of sample records", gt=0, le=1000)
) -> SampleDataResponse:
    """Get sample data or schema information for a dataset.
    
    This tool attempts to retrieve sample data from the dataset to help understand
    its structure, format, and content. If direct data access isn't available,
    it will provide schema information or documentation.
    
    Args:
        dataset_name: The name or identifier of the dataset
        sample_size: Number of sample records to retrieve (1-1000)
    
    Returns:
        Sample data with column information and example records, or schema information
    """
    try:
        logger.info(f"Getting sample data for dataset: {dataset_name}, size: {sample_size}")
        
        # First get the dataset metadata
        dataset_data = await registry_service.fetch_dataset_yaml(dataset_name)
        
        # Then get sample data
        sample_response = await sample_service.get_sample_data(
            dataset_name=dataset_name,
            dataset_data=dataset_data,
            sample_size=sample_size
        )
        
        logger.info(f"Retrieved sample data for dataset: {dataset_name}")
        return sample_response
        
    except Exception as e:
        logger.error(f"Failed to get sample data for {dataset_name}: {e}")
        raise ValueError(f"Failed to get sample data: {str(e)}")


@mcp.tool()
async def list_categories(ctx: Context) -> List[CategoryInfo]:
    """Get list of available dataset categories.
    
    This tool returns all available categories in the Open Data Registry
    along with the count of datasets in each category.
    
    Returns:
        List of categories with dataset counts
    """
    try:
        logger.info("Listing dataset categories")
        
        categories = await dataset_service.list_categories()
        
        logger.info(f"Found {len(categories)} categories")
        return categories
        
    except Exception as e:
        logger.error(f"Failed to list categories: {e}")
        raise ValueError(f"Failed to list categories: {str(e)}")


# Register prompts and resources
register_prompts(mcp, registry_service, dataset_service)
register_resources(mcp, registry_service, dataset_service)


async def cleanup():
    """Cleanup resources on shutdown."""
    try:
        await registry_service.close()
        await sample_service.close()
        logger.info("Cleanup completed")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")


def main():
    """Run the MCP server with CLI argument support."""
    try:
        logger.info("Starting AWS Open Data Registry MCP Server")
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise
    finally:
        # Note: cleanup() is async, but we can't await it here
        # The FastMCP framework should handle cleanup automatically
        logger.info("Server shutdown")


if __name__ == '__main__':
    main()
