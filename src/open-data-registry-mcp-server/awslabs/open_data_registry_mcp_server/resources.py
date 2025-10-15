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

"""MCP resources for dataset information access."""

import json
from datetime import datetime

from loguru import logger
from mcp.server.fastmcp import Context

from .dataset_service import DatasetService
from .registry_service import RegistryService


def register_resources(mcp, registry_service: RegistryService, dataset_service: DatasetService):
    """Register MCP resources with the FastMCP server."""
    
    @mcp.resource("dataset://{dataset_name}/metadata")
    async def dataset_metadata_resource(
        ctx: Context,
        dataset_name: str
    ) -> str:
        """Provide dataset metadata as a readable resource.
        
        This resource exposes comprehensive dataset metadata in a structured format
        that can be easily consumed by AI assistants for analysis and reference.
        
        Args:
            dataset_name: Name of the dataset to retrieve metadata for
        
        Returns:
            Formatted dataset metadata as a string
        """
        try:
            logger.info(f"Fetching metadata resource for dataset: {dataset_name}")
            
            # Get detailed dataset information
            details = await dataset_service.get_dataset_details(dataset_name)
            
            # Format as structured text
            metadata = f"""# Dataset Metadata: {details.name}

## Basic Information
- **Name:** {details.name}
- **Provider:** {details.provider}
- **Category:** {details.category}
- **License:** {details.license}
- **Last Updated:** {details.last_updated}
- **Size:** {details.size}
- **Update Frequency:** {details.update_frequency}

## Description
{details.description}

## Data Formats
{', '.join(details.data_format) if details.data_format else 'Not specified'}

## Geographic Coverage
{', '.join(details.regions) if details.regions else 'Not specified'}

## Access Methods
{', '.join(details.access_methods) if details.access_methods else 'Not specified'}

## Tags
{', '.join(details.tags) if details.tags else 'None'}

## Contact Information"""
            
            if details.contact:
                if details.contact.organization:
                    metadata += f"\n- **Organization:** {details.contact.organization}"
                if details.contact.name:
                    metadata += f"\n- **Contact:** {details.contact.name}"
                if details.contact.email:
                    metadata += f"\n- **Email:** {details.contact.email}"
            else:
                metadata += "\nNo contact information available"
            
            metadata += f"\n\n## Resources ({len(details.resources)} available)"
            if details.resources:
                for i, resource in enumerate(details.resources, 1):
                    metadata += f"\n{i}. **Type:** {resource.type}"
                    if resource.arn:
                        metadata += f"\n   - **ARN:** {resource.arn}"
                    if resource.region:
                        metadata += f"\n   - **Region:** {resource.region}"
                    if resource.description:
                        metadata += f"\n   - **Description:** {resource.description}"
                    metadata += "\n"
            else:
                metadata += "\nNo specific resources listed"
            
            if details.documentation:
                metadata += f"\n## Documentation\n{details.documentation}"
            
            metadata += f"\n\n---\n*Metadata retrieved on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC*"
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to fetch metadata resource for {dataset_name}: {e}")
            return f"Error: Could not retrieve metadata for dataset '{dataset_name}': {str(e)}"    

    @mcp.resource("dataset://{dataset_name}/documentation")
    async def dataset_documentation_resource(
        ctx: Context,
        dataset_name: str
    ) -> str:
        """Provide dataset documentation as a readable resource.
        
        This resource provides access to dataset documentation, including
        official documentation links and extracted schema information.
        
        Args:
            dataset_name: Name of the dataset to retrieve documentation for
        
        Returns:
            Formatted dataset documentation as a string
        """
        try:
            logger.info(f"Fetching documentation resource for dataset: {dataset_name}")
            
            # Get dataset details and raw YAML data
            details = await dataset_service.get_dataset_details(dataset_name)
            raw_data = await registry_service.fetch_dataset_yaml(dataset_name)
            
            documentation = f"""# Dataset Documentation: {details.name}

## Overview
{details.description}

## Official Documentation
"""
            
            if details.documentation:
                documentation += f"{details.documentation}\n"
            else:
                documentation += "No official documentation URL provided\n"
            
            documentation += f"""
## Dataset Specification

### Basic Properties
- **Managed By:** {raw_data.get('ManagedBy', 'Not specified')}
- **Contact:** {raw_data.get('Contact', 'Not specified')}
- **License:** {details.license}
- **Update Frequency:** {details.update_frequency}

### Data Characteristics
- **Data Formats:** {', '.join(details.data_format) if details.data_format else 'Not specified'}
- **Geographic Coverage:** {', '.join(details.regions) if details.regions else 'Global or not specified'}
- **Temporal Coverage:** {raw_data.get('TemporalCoverage', 'Not specified')}

### Technical Details
"""
            
            # Add resource information
            if details.resources:
                documentation += "#### Available Resources:\n"
                for i, resource in enumerate(details.resources, 1):
                    documentation += f"{i}. **{resource.type}**\n"
                    if resource.description:
                        documentation += f"   - {resource.description}\n"
                    if resource.arn:
                        documentation += f"   - ARN: `{resource.arn}`\n"
                    if resource.region:
                        documentation += f"   - Region: {resource.region}\n"
                    documentation += "\n"
            
            # Add tags and categorization
            if details.tags:
                documentation += f"#### Tags and Categories:\n"
                documentation += f"- **Category:** {details.category}\n"
                documentation += f"- **Tags:** {', '.join(details.tags)}\n\n"
            
            # Add any additional metadata from raw YAML
            if 'DataAtRest' in raw_data:
                data_at_rest = raw_data['DataAtRest']
                documentation += "#### Data Storage Information:\n"
                if isinstance(data_at_rest, dict):
                    for key, value in data_at_rest.items():
                        documentation += f"- **{key}:** {value}\n"
                else:
                    documentation += f"- {data_at_rest}\n"
                documentation += "\n"
            
            if 'UpdatedBy' in raw_data:
                updated_by = raw_data['UpdatedBy']
                documentation += "#### Update Information:\n"
                if isinstance(updated_by, dict):
                    for key, value in updated_by.items():
                        documentation += f"- **{key}:** {value}\n"
                else:
                    documentation += f"- {updated_by}\n"
                documentation += "\n"
            
            documentation += f"""
## Usage Guidelines

### Access Instructions
1. Review the license terms: {details.license}
2. Check available access methods: {', '.join(details.access_methods) if details.access_methods else 'See resources above'}
3. Follow any authentication requirements
4. Respect usage limitations and attribution requirements

### Best Practices
- Always cite the dataset provider: {details.provider}
- Check for updates regularly (Update frequency: {details.update_frequency})
- Review documentation for any usage restrictions
- Consider data quality and limitations for your use case

---
*Documentation retrieved on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC*
"""
            
            return documentation
            
        except Exception as e:
            logger.error(f"Failed to fetch documentation resource for {dataset_name}: {e}")
            return f"Error: Could not retrieve documentation for dataset '{dataset_name}': {str(e)}"
    
    @mcp.resource("registry://categories")
    async def registry_categories_resource(ctx: Context) -> str:
        """Provide complete registry category information as a resource.
        
        This resource provides an overview of all available categories in the
        Open Data Registry with dataset counts and descriptions.
        
        Returns:
            Formatted category information as a string
        """
        try:
            logger.info("Fetching registry categories resource")
            
            categories = await dataset_service.list_categories()
            
            categories_doc = f"""# Open Data Registry Categories

## Overview
The AWS Open Data Registry contains datasets across {len(categories)} categories.
This resource provides a complete overview of available categories and their contents.

## Category Breakdown

"""
            
            # Sort categories by count (descending)
            sorted_categories = sorted(categories, key=lambda x: x.count, reverse=True)
            
            total_datasets = sum(cat.count for cat in categories)
            
            for i, category in enumerate(sorted_categories, 1):
                percentage = (category.count / total_datasets * 100) if total_datasets > 0 else 0
                categories_doc += f"""### {i}. {category.name}
- **Dataset Count:** {category.count} ({percentage:.1f}% of total)
- **Description:** {category.description if category.description else 'No description available'}

"""
            
            categories_doc += f"""
## Summary Statistics
- **Total Categories:** {len(categories)}
- **Total Datasets:** {total_datasets}
- **Average Datasets per Category:** {total_datasets / len(categories):.1f}

## Usage Tips
- Use the `search_datasets` tool with the `category` parameter to find datasets in specific categories
- Categories can help narrow down searches when looking for domain-specific data
- Some datasets may be tagged with multiple categories

---
*Category information retrieved on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC*
"""
            
            return categories_doc
            
        except Exception as e:
            logger.error(f"Failed to fetch categories resource: {e}")
            return f"Error: Could not retrieve category information: {str(e)}"
    
    @mcp.resource("registry://recent-updates")
    async def recent_updates_resource(ctx: Context) -> str:
        """Provide information about recently updated datasets.
        
        This resource shows datasets that have been recently updated or added
        to the registry, helping users discover new or refreshed data.
        
        Returns:
            Information about recent dataset updates
        """
        try:
            logger.info("Fetching recent updates resource")
            
            # Get all dataset names and their details
            dataset_names = await dataset_service.get_dataset_names()
            
            # Sample a subset for recent updates (to avoid overwhelming the system)
            sample_size = min(20, len(dataset_names))
            sample_names = dataset_names[:sample_size]
            
            recent_updates_doc = f"""# Recent Dataset Updates

## Overview
This resource shows information about dataset updates in the AWS Open Data Registry.
Showing sample of {sample_size} datasets from {len(dataset_names)} total datasets.

## Dataset Status
"""
            
            datasets_with_dates = []
            
            for name in sample_names:
                try:
                    details = await dataset_service.get_dataset_details(name)
                    datasets_with_dates.append({
                        'name': details.name,
                        'provider': details.provider,
                        'category': details.category,
                        'last_updated': details.last_updated,
                        'update_frequency': details.update_frequency,
                        'description': details.description[:100] + '...' if len(details.description) > 100 else details.description
                    })
                except Exception as e:
                    logger.debug(f"Skipping dataset {name} in recent updates: {e}")
                    continue
            
            # Group by update frequency
            frequency_groups = {}
            for dataset in datasets_with_dates:
                freq = dataset['update_frequency']
                if freq not in frequency_groups:
                    frequency_groups[freq] = []
                frequency_groups[freq].append(dataset)
            
            for frequency, datasets in frequency_groups.items():
                recent_updates_doc += f"""
### {frequency} Updates ({len(datasets)} datasets)
"""
                for dataset in datasets[:5]:  # Show top 5 per frequency
                    recent_updates_doc += f"""
#### {dataset['name']}
- **Provider:** {dataset['provider']}
- **Category:** {dataset['category']}
- **Last Updated:** {dataset['last_updated']}
- **Description:** {dataset['description']}

"""
            
            recent_updates_doc += f"""
## Update Frequency Distribution
"""
            for frequency, datasets in sorted(frequency_groups.items(), key=lambda x: len(x[1]), reverse=True):
                recent_updates_doc += f"- **{frequency}:** {len(datasets)} datasets\n"
            
            recent_updates_doc += f"""

## Monitoring Tips
- Check datasets with "Daily" or "Weekly" update frequencies for the freshest data
- "Irregular" or "As needed" updates may indicate event-driven or research datasets
- Use the `get_dataset_details` tool to check specific update information
- Consider setting up monitoring for critical datasets in your workflows

---
*Update information retrieved on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC*
*Note: This is a sample of available datasets. Use search tools to find specific datasets.*
"""
            
            return recent_updates_doc
            
        except Exception as e:
            logger.error(f"Failed to fetch recent updates resource: {e}")
            return f"Error: Could not retrieve recent updates information: {str(e)}"