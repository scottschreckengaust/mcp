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

"""Dataset indexing and metadata extraction utilities."""

import re
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from .models import DatasetDetails, DatasetSummary, ContactInfo, ResourceInfo


class DatasetIndexer:
    """Utility class for indexing and extracting metadata from dataset YAML."""
    
    @staticmethod
    def extract_dataset_summary(dataset_data: Dict[str, Any]) -> DatasetSummary:
        """Extract summary information from dataset YAML data."""
        try:
            name = dataset_data.get('_name', dataset_data.get('Name', 'Unknown'))
            description = dataset_data.get('Description', '')
            
            # Extract provider/maintainer information
            maintainer = dataset_data.get('ManagedBy', '')
            contact = dataset_data.get('Contact', '')
            provider = maintainer or contact or 'Unknown'
            
            # Extract category from tags
            tags_data = dataset_data.get('Tags', {})
            category = tags_data.get('Category', 'Unknown')
            
            # Extract all tags
            tags = DatasetIndexer._extract_tags(dataset_data)
            
            # Extract license
            license_info = dataset_data.get('License', 'Unknown')
            
            # Extract update information
            last_updated = dataset_data.get('UpdatedBy', {}).get('Date', 'Unknown')
            if isinstance(last_updated, dict):
                last_updated = 'Unknown'
            
            # Extract access methods
            access_methods = DatasetIndexer._extract_access_methods(dataset_data)
            
            return DatasetSummary(
                name=name,
                description=description,
                provider=provider,
                category=category,
                tags=tags,
                license=license_info,
                last_updated=str(last_updated),
                access_methods=access_methods
            )
            
        except Exception as e:
            logger.error(f"Failed to extract dataset summary: {e}")
            # Return minimal summary with available data
            return DatasetSummary(
                name=dataset_data.get('_name', 'Unknown'),
                description=dataset_data.get('Description', ''),
                provider='Unknown',
                category='Unknown',
                tags=[],
                license='Unknown',
                last_updated='Unknown',
                access_methods=[]
            )
    
    @staticmethod
    def extract_dataset_details(dataset_data: Dict[str, Any]) -> DatasetDetails:
        """Extract detailed information from dataset YAML data."""
        try:
            # Start with summary data
            summary = DatasetIndexer.extract_dataset_summary(dataset_data)
            
            # Extract additional details
            documentation = dataset_data.get('Documentation', '')
            if isinstance(documentation, list) and documentation:
                documentation = documentation[0]
            
            # Extract data formats
            data_format = DatasetIndexer._extract_data_formats(dataset_data)
            
            # Extract update frequency
            update_frequency = dataset_data.get('UpdateFrequency', 'Unknown')
            
            # Extract size information
            size = 'Unknown'
            if 'DataAtRest' in dataset_data:
                size_info = dataset_data['DataAtRest']
                if isinstance(size_info, dict):
                    size = size_info.get('SizeInBytes', 'Unknown')
                    if size != 'Unknown':
                        size = DatasetIndexer._format_size(size)
            
            # Extract regions
            regions = DatasetIndexer._extract_regions(dataset_data)
            
            # Extract contact information
            contact = DatasetIndexer._extract_contact_info(dataset_data)
            
            # Extract resources
            resources = DatasetIndexer._extract_resources(dataset_data)
            
            return DatasetDetails(
                name=summary.name,
                description=summary.description,
                provider=summary.provider,
                category=summary.category,
                tags=summary.tags,
                license=summary.license,
                last_updated=summary.last_updated,
                access_methods=summary.access_methods,
                documentation=documentation,
                data_format=data_format,
                update_frequency=update_frequency,
                size=size,
                regions=regions,
                contact=contact,
                resources=resources
            )
            
        except Exception as e:
            logger.error(f"Failed to extract dataset details: {e}")
            # Fallback to summary data
            summary = DatasetIndexer.extract_dataset_summary(dataset_data)
            return DatasetDetails(
                name=summary.name,
                description=summary.description,
                provider=summary.provider,
                category=summary.category,
                tags=summary.tags,
                license=summary.license,
                last_updated=summary.last_updated,
                access_methods=summary.access_methods,
                documentation='',
                data_format=[],
                update_frequency='Unknown',
                size='Unknown',
                regions=[],
                contact=None,
                resources=[]
            )
    
    @staticmethod
    def _extract_tags(dataset_data: Dict[str, Any]) -> List[str]:
        """Extract all tags from dataset data."""
        tags: Set[str] = set()
        
        tags_section = dataset_data.get('Tags', {})
        if isinstance(tags_section, dict):
            for key, value in tags_section.items():
                if isinstance(value, list):
                    tags.update(str(v) for v in value)
                elif value:
                    tags.add(str(value))
        
        # Also extract from other relevant fields
        if 'Category' in dataset_data:
            tags.add(str(dataset_data['Category']))
        
        return sorted(list(tags))
    
    @staticmethod
    def _extract_access_methods(dataset_data: Dict[str, Any]) -> List[str]:
        """Extract access methods from dataset data."""
        methods: Set[str] = set()
        
        # Check for S3 resources
        if 'Resources' in dataset_data:
            resources = dataset_data['Resources']
            if isinstance(resources, list):
                for resource in resources:
                    if isinstance(resource, dict):
                        resource_type = resource.get('Type', '')
                        if 'S3' in resource_type:
                            methods.add('S3')
                        elif 'API' in resource_type:
                            methods.add('API')
                        elif 'HTTP' in resource_type or 'HTTPS' in resource_type:
                            methods.add('HTTP')
        
        # Check for data at rest information
        if 'DataAtRest' in dataset_data:
            methods.add('S3')
        
        return sorted(list(methods))
    
    @staticmethod
    def _extract_data_formats(dataset_data: Dict[str, Any]) -> List[str]:
        """Extract data formats from dataset data."""
        formats: Set[str] = set()
        
        # Check file formats in resources
        if 'Resources' in dataset_data:
            resources = dataset_data['Resources']
            if isinstance(resources, list):
                for resource in resources:
                    if isinstance(resource, dict):
                        description = resource.get('Description', '').lower()
                        # Look for common format indicators
                        format_patterns = {
                            'csv': r'\bcsv\b',
                            'json': r'\bjson\b',
                            'parquet': r'\bparquet\b',
                            'netcdf': r'\bnetcdf\b|\bnc\b',
                            'hdf5': r'\bhdf5?\b',
                            'geotiff': r'\bgeotiff?\b|\btiff?\b',
                            'shapefile': r'\bshapefile\b|\bshp\b',
                            'xml': r'\bxml\b',
                            'txt': r'\btext\b|\btxt\b'
                        }
                        
                        for format_name, pattern in format_patterns.items():
                            if re.search(pattern, description):
                                formats.add(format_name.upper())
        
        # Check description for format mentions
        description = dataset_data.get('Description', '').lower()
        format_patterns = {
            'CSV': r'\bcsv\b',
            'JSON': r'\bjson\b',
            'Parquet': r'\bparquet\b',
            'NetCDF': r'\bnetcdf\b',
            'HDF5': r'\bhdf5?\b',
            'GeoTIFF': r'\bgeotiff?\b',
            'Shapefile': r'\bshapefile\b'
        }
        
        for format_name, pattern in format_patterns.items():
            if re.search(pattern, description):
                formats.add(format_name)
        
        return sorted(list(formats))
    
    @staticmethod
    def _extract_regions(dataset_data: Dict[str, Any]) -> List[str]:
        """Extract AWS regions from dataset data."""
        regions: Set[str] = set()
        
        if 'Resources' in dataset_data:
            resources = dataset_data['Resources']
            if isinstance(resources, list):
                for resource in resources:
                    if isinstance(resource, dict):
                        region = resource.get('Region', '')
                        if region:
                            regions.add(region)
        
        # Check data at rest information
        if 'DataAtRest' in dataset_data:
            data_at_rest = dataset_data['DataAtRest']
            if isinstance(data_at_rest, dict):
                location = data_at_rest.get('Location', '')
                # Extract region from S3 ARN or location
                if 's3://' in location or 'arn:aws:s3' in location:
                    # Try to extract region from ARN or bucket name patterns
                    region_match = re.search(r'us-[a-z]+-\d+|eu-[a-z]+-\d+|ap-[a-z]+-\d+', location)
                    if region_match:
                        regions.add(region_match.group())
        
        return sorted(list(regions))
    
    @staticmethod
    def _extract_contact_info(dataset_data: Dict[str, Any]) -> Optional[ContactInfo]:
        """Extract contact information from dataset data."""
        contact_data = dataset_data.get('Contact', '')
        managed_by = dataset_data.get('ManagedBy', '')
        
        if not contact_data and not managed_by:
            return None
        
        # Try to parse email from contact string
        email = None
        name = None
        organization = None
        
        if isinstance(contact_data, str) and contact_data:
            # Look for email pattern
            email_match = re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', contact_data)
            if email_match:
                email = email_match.group()
            
            # Use the contact string as name if no email found
            if not email:
                name = contact_data
        
        if managed_by:
            organization = managed_by
        
        if email or name or organization:
            return ContactInfo(
                name=name,
                email=email,
                organization=organization
            )
        
        return None
    
    @staticmethod
    def _extract_resources(dataset_data: Dict[str, Any]) -> List[ResourceInfo]:
        """Extract resource information from dataset data."""
        resources = []
        
        if 'Resources' in dataset_data:
            resource_list = dataset_data['Resources']
            if isinstance(resource_list, list):
                for resource in resource_list:
                    if isinstance(resource, dict):
                        resource_type = resource.get('Type', 'Unknown')
                        arn = resource.get('ARN', '')
                        region = resource.get('Region', '')
                        description = resource.get('Description', '')
                        
                        resources.append(ResourceInfo(
                            type=resource_type,
                            arn=arn if arn else None,
                            region=region if region else None,
                            description=description if description else None
                        ))
        
        return resources
    
    @staticmethod
    def _format_size(size_bytes: Any) -> str:
        """Format size in bytes to human readable format."""
        try:
            size = int(size_bytes)
            
            # Convert to appropriate unit
            units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
            unit_index = 0
            
            while size >= 1024 and unit_index < len(units) - 1:
                size /= 1024
                unit_index += 1
            
            return f"{size:.1f} {units[unit_index]}"
            
        except (ValueError, TypeError):
            return str(size_bytes)
    
    @staticmethod
    def create_search_index(datasets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a search index for faster dataset lookups."""
        index = {
            'by_name': {},
            'by_category': {},
            'by_tag': {},
            'keywords': {}
        }
        
        for dataset in datasets:
            try:
                summary = DatasetIndexer.extract_dataset_summary(dataset)
                
                # Index by name
                index['by_name'][summary.name.lower()] = dataset
                
                # Index by category
                category = summary.category.lower()
                if category not in index['by_category']:
                    index['by_category'][category] = []
                index['by_category'][category].append(dataset)
                
                # Index by tags
                for tag in summary.tags:
                    tag_lower = tag.lower()
                    if tag_lower not in index['by_tag']:
                        index['by_tag'][tag_lower] = []
                    index['by_tag'][tag_lower].append(dataset)
                
                # Index keywords from name and description
                keywords = (summary.name + ' ' + summary.description).lower().split()
                for keyword in keywords:
                    # Clean keyword
                    keyword = re.sub(r'[^\w]', '', keyword)
                    if len(keyword) > 2:  # Skip very short words
                        if keyword not in index['keywords']:
                            index['keywords'][keyword] = []
                        index['keywords'][keyword].append(dataset)
                
            except Exception as e:
                logger.warning(f"Failed to index dataset: {e}")
                continue
        
        return index