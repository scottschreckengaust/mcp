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

"""Sample data service for accessing dataset samples and schema information."""

import csv
import io
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import httpx
from bs4 import BeautifulSoup
from loguru import logger

from .config import Config
from .models import ColumnInfo, SampleDataResponse


class SampleService:
    """Service for accessing sample data from various sources."""
    
    def __init__(self):
        """Initialize the sample service."""
        self._http_client: Optional[httpx.AsyncClient] = None
        self._s3_client: Optional[Any] = None
    
    async def _get_http_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(Config.SAMPLE_REQUEST_TIMEOUT),
                headers={
                    'User-Agent': 'awslabs-open-data-registry-mcp-server/0.0.0'
                }
            )
        return self._http_client
    
    def _get_s3_client(self):
        """Get or create S3 client."""
        if self._s3_client is None:
            try:
                self._s3_client = boto3.client('s3', region_name=Config.AWS_REGION)
            except Exception as e:
                logger.warning(f"Failed to create S3 client: {e}")
                self._s3_client = None
        return self._s3_client
    
    async def close(self) -> None:
        """Close HTTP client."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
    
    async def get_sample_data(
        self,
        dataset_name: str,
        dataset_data: Dict[str, Any],
        sample_size: int = None
    ) -> SampleDataResponse:
        """Get sample data using multiple strategies."""
        if sample_size is None:
            sample_size = Config.DEFAULT_SAMPLE_SIZE
        
        sample_size = min(sample_size, Config.MAX_SAMPLE_SIZE)
        
        logger.info(f"Getting sample data for dataset: {dataset_name}")
        
        # Strategy 1: Try S3 public access
        try:
            s3_result = await self._try_s3_sample(dataset_data, sample_size)
            if s3_result:
                logger.info(f"Successfully retrieved S3 sample for {dataset_name}")
                return SampleDataResponse(
                    dataset_name=dataset_name,
                    sample_type='data',
                    columns=s3_result[0],
                    sample_records=s3_result[1],
                    notes="Sample data retrieved from S3 public bucket"
                )
        except Exception as e:
            logger.debug(f"S3 sample failed for {dataset_name}: {e}")
        
        # Strategy 2: Try documentation parsing
        try:
            doc_result = await self._try_documentation_sample(dataset_data)
            if doc_result:
                logger.info(f"Successfully extracted schema from documentation for {dataset_name}")
                return SampleDataResponse(
                    dataset_name=dataset_name,
                    sample_type='schema',
                    schema_info=doc_result,
                    notes="Schema information extracted from documentation"
                )
        except Exception as e:
            logger.debug(f"Documentation sample failed for {dataset_name}: {e}")
        
        # Strategy 3: Try registry metadata
        try:
            metadata_result = self._try_registry_metadata(dataset_data)
            if metadata_result:
                logger.info(f"Successfully extracted metadata sample for {dataset_name}")
                return SampleDataResponse(
                    dataset_name=dataset_name,
                    sample_type='documentation',
                    schema_info=metadata_result,
                    notes="Information extracted from registry metadata"
                )
        except Exception as e:
            logger.debug(f"Registry metadata sample failed for {dataset_name}: {e}")
        
        # Strategy 4: Try API endpoints (if available)
        try:
            api_result = await self._try_api_sample(dataset_data, sample_size)
            if api_result:
                logger.info(f"Successfully retrieved API sample for {dataset_name}")
                return SampleDataResponse(
                    dataset_name=dataset_name,
                    sample_type='data',
                    columns=api_result[0],
                    sample_records=api_result[1],
                    notes="Sample data retrieved from API endpoint"
                )
        except Exception as e:
            logger.debug(f"API sample failed for {dataset_name}: {e}")
        
        # Fallback: Return basic information
        logger.warning(f"No sample data available for {dataset_name}")
        return SampleDataResponse(
            dataset_name=dataset_name,
            sample_type='documentation',
            schema_info=self._create_basic_info(dataset_data),
            notes="Sample data not accessible. Basic dataset information provided."
        )
    
    async def _try_s3_sample(
        self,
        dataset_data: Dict[str, Any],
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Try to get sample data from S3 public buckets."""
        s3_client = self._get_s3_client()
        if not s3_client:
            return None
        
        # Extract S3 resources
        s3_resources = self._extract_s3_resources(dataset_data)
        if not s3_resources:
            return None
        
        for resource in s3_resources:
            try:
                bucket, prefix = resource
                
                # List objects in the bucket with the prefix
                response = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    MaxKeys=10  # Limit to first 10 objects
                )
                
                if 'Contents' not in response:
                    continue
                
                # Try to find a suitable file for sampling
                for obj in response['Contents']:
                    key = obj['Key']
                    if self._is_sampleable_file(key):
                        try:
                            sample_data = await self._sample_s3_file(s3_client, bucket, key, sample_size)
                            if sample_data:
                                return sample_data
                        except Exception as e:
                            logger.debug(f"Failed to sample S3 file {bucket}/{key}: {e}")
                            continue
                
            except Exception as e:
                logger.debug(f"Failed to access S3 resource {resource}: {e}")
                continue
        
        return None
    
    def _extract_s3_resources(self, dataset_data: Dict[str, Any]) -> List[Tuple[str, str]]:
        """Extract S3 bucket and prefix information from dataset."""
        resources = []
        
        # Check Resources section
        if 'Resources' in dataset_data:
            resource_list = dataset_data['Resources']
            if isinstance(resource_list, list):
                for resource in resource_list:
                    if isinstance(resource, dict):
                        arn = resource.get('ARN', '')
                        if 'arn:aws:s3:::' in arn:
                            # Parse S3 ARN
                            bucket_info = self._parse_s3_arn(arn)
                            if bucket_info:
                                resources.append(bucket_info)
        
        # Check DataAtRest section
        if 'DataAtRest' in dataset_data:
            data_at_rest = dataset_data['DataAtRest']
            if isinstance(data_at_rest, dict):
                location = data_at_rest.get('Location', '')
                if location.startswith('s3://'):
                    bucket_info = self._parse_s3_url(location)
                    if bucket_info:
                        resources.append(bucket_info)
        
        return resources
    
    def _parse_s3_arn(self, arn: str) -> Optional[Tuple[str, str]]:
        """Parse S3 ARN to extract bucket and prefix."""
        try:
            # ARN format: arn:aws:s3:::bucket-name/prefix
            if 'arn:aws:s3:::' in arn:
                s3_part = arn.split('arn:aws:s3:::')[1]
                if '/' in s3_part:
                    bucket, prefix = s3_part.split('/', 1)
                    return (bucket, prefix)
                else:
                    return (s3_part, '')
        except Exception as e:
            logger.debug(f"Failed to parse S3 ARN {arn}: {e}")
        return None
    
    def _parse_s3_url(self, url: str) -> Optional[Tuple[str, str]]:
        """Parse S3 URL to extract bucket and prefix."""
        try:
            if url.startswith('s3://'):
                parsed = urlparse(url)
                bucket = parsed.netloc
                prefix = parsed.path.lstrip('/')
                return (bucket, prefix)
        except Exception as e:
            logger.debug(f"Failed to parse S3 URL {url}: {e}")
        return None
    
    def _is_sampleable_file(self, key: str) -> bool:
        """Check if file is suitable for sampling."""
        key_lower = key.lower()
        
        # Check for supported formats
        for format_ext in Config.SUPPORTED_SAMPLE_FORMATS:
            if key_lower.endswith(f'.{format_ext}'):
                return True
        
        # Skip very large files or directories
        if key.endswith('/'):
            return False
        
        # Skip binary formats that are hard to sample
        skip_extensions = ['.zip', '.tar', '.gz', '.bz2', '.7z', '.rar']
        for ext in skip_extensions:
            if key_lower.endswith(ext):
                return False
        
        return True
    
    async def _sample_s3_file(
        self,
        s3_client,
        bucket: str,
        key: str,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Sample data from an S3 file."""
        try:
            # Get file info first
            head_response = s3_client.head_object(Bucket=bucket, Key=key)
            file_size = head_response['ContentLength']
            
            # Skip very large files
            if file_size > 100 * 1024 * 1024:  # 100MB limit
                logger.debug(f"Skipping large file {bucket}/{key} ({file_size} bytes)")
                return None
            
            # Download a portion of the file for sampling
            download_size = min(file_size, 1024 * 1024)  # 1MB max
            response = s3_client.get_object(
                Bucket=bucket,
                Key=key,
                Range=f'bytes=0-{download_size-1}'
            )
            
            content = response['Body'].read()
            
            # Determine file format and parse
            return self._parse_by_format(content, key, sample_size)
            
        except Exception as e:
            logger.debug(f"Failed to sample S3 file {bucket}/{key}: {e}")
        
        return None
    
    def _parse_by_format(
        self,
        content: bytes,
        filename: str,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Parse content based on file format detection."""
        filename_lower = filename.lower()
        
        # Try format-specific parsers
        if filename_lower.endswith('.csv'):
            return self._parse_csv_sample(content, sample_size)
        elif filename_lower.endswith(('.json', '.jsonl')):
            return self._parse_json_sample(content, sample_size)
        elif filename_lower.endswith(('.txt', '.tsv')):
            return self._parse_text_sample(content, sample_size)
        elif filename_lower.endswith(('.xml',)):
            return self._parse_xml_sample(content, sample_size)
        elif filename_lower.endswith(('.yaml', '.yml')):
            return self._parse_yaml_sample(content, sample_size)
        else:
            # Try to detect format from content
            return self._parse_by_content_detection(content, sample_size)
    
    def _parse_by_content_detection(
        self,
        content: bytes,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Detect format from content and parse accordingly."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            text_sample = text_content[:1000]  # First 1KB for detection
            
            # Try JSON first
            if text_sample.strip().startswith(('{', '[')):
                result = self._parse_json_sample(content, sample_size)
                if result:
                    return result
            
            # Try XML
            if text_sample.strip().startswith('<'):
                result = self._parse_xml_sample(content, sample_size)
                if result:
                    return result
            
            # Try CSV (look for comma-separated values)
            if ',' in text_sample and '\n' in text_sample:
                result = self._parse_csv_sample(content, sample_size)
                if result:
                    return result
            
            # Try TSV (look for tab-separated values)
            if '\t' in text_sample and '\n' in text_sample:
                result = self._parse_text_sample(content, sample_size)
                if result:
                    return result
            
            # Fallback to text parsing
            return self._parse_text_sample(content, sample_size)
            
        except Exception as e:
            logger.debug(f"Content detection failed: {e}")
        
        return None
    
    def _parse_xml_sample(
        self,
        content: bytes,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Parse XML content for sample data."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            soup = BeautifulSoup(text_content, 'xml')
            
            # Find repeating elements that could represent records
            records = []
            
            # Look for common record patterns
            record_tags = ['record', 'item', 'entry', 'row', 'data']
            
            for tag_name in record_tags:
                elements = soup.find_all(tag_name)
                if elements:
                    for element in elements[:sample_size]:
                        record = {}
                        for child in element.find_all():
                            if child.string:
                                record[child.name] = child.string.strip()
                        if record:
                            records.append(record)
                    break
            
            # If no standard record tags, try to extract from any repeating structure
            if not records:
                all_tags = soup.find_all()
                tag_counts = {}
                for tag in all_tags:
                    if tag.name:
                        tag_counts[tag.name] = tag_counts.get(tag.name, 0) + 1
                
                # Find most common tag (likely record container)
                if tag_counts:
                    most_common_tag = max(tag_counts, key=tag_counts.get)
                    if tag_counts[most_common_tag] > 1:
                        elements = soup.find_all(most_common_tag)[:sample_size]
                        for element in elements:
                            record = {}
                            for child in element.find_all():
                                if child.string:
                                    record[child.name] = child.string.strip()
                            if record:
                                records.append(record)
            
            if records:
                columns = self._extract_columns_from_records(records)
                return (columns, records)
            
        except Exception as e:
            logger.debug(f"Failed to parse XML sample: {e}")
        
        return None
    
    def _parse_yaml_sample(
        self,
        content: bytes,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Parse YAML content for sample data."""
        try:
            import yaml
            
            text_content = content.decode('utf-8', errors='ignore')
            data = yaml.safe_load(text_content)
            
            records = []
            
            if isinstance(data, list):
                # Array of records
                for item in data[:sample_size]:
                    if isinstance(item, dict):
                        records.append(item)
            elif isinstance(data, dict):
                # Single record or nested structure
                if all(isinstance(v, dict) for v in data.values()):
                    # Nested dictionaries - treat each as a record
                    for key, value in list(data.items())[:sample_size]:
                        if isinstance(value, dict):
                            record = {'id': key}
                            record.update(value)
                            records.append(record)
                else:
                    # Single flat record
                    records.append(data)
            
            if records:
                columns = self._extract_columns_from_records(records)
                return (columns, records)
            
        except Exception as e:
            logger.debug(f"Failed to parse YAML sample: {e}")
        
        return None
    
    def _parse_csv_sample(
        self,
        content: bytes,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Parse CSV content for sample data."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            csv_reader = csv.DictReader(io.StringIO(text_content))
            
            # Get column names
            fieldnames = csv_reader.fieldnames
            if not fieldnames:
                return None
            
            # Create column info
            columns = [
                ColumnInfo(name=name, type='string', description=None)
                for name in fieldnames
            ]
            
            # Sample records
            records = []
            for i, row in enumerate(csv_reader):
                if i >= sample_size:
                    break
                records.append(dict(row))
            
            # Infer data types from sample
            if records:
                self._infer_column_types(columns, records)
            
            return (columns, records)
            
        except Exception as e:
            logger.debug(f"Failed to parse CSV sample: {e}")
        
        return None
    
    def _parse_json_sample(
        self,
        content: bytes,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Parse JSON content for sample data."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            
            # Try parsing as JSON array
            try:
                data = json.loads(text_content)
                if isinstance(data, list) and data:
                    records = data[:sample_size]
                    if isinstance(records[0], dict):
                        columns = self._extract_columns_from_records(records)
                        return (columns, records)
            except json.JSONDecodeError:
                pass
            
            # Try parsing as JSONL (one JSON object per line)
            lines = text_content.strip().split('\n')
            records = []
            for line in lines[:sample_size]:
                try:
                    record = json.loads(line.strip())
                    if isinstance(record, dict):
                        records.append(record)
                except json.JSONDecodeError:
                    continue
            
            if records:
                columns = self._extract_columns_from_records(records)
                return (columns, records)
            
        except Exception as e:
            logger.debug(f"Failed to parse JSON sample: {e}")
        
        return None
    
    def _parse_text_sample(
        self,
        content: bytes,
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Parse text content for sample data."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            lines = text_content.strip().split('\n')
            
            # Try to detect delimiter
            first_line = lines[0] if lines else ""
            delimiter = '\t' if '\t' in first_line else ','
            
            # Parse as delimited text
            reader = csv.DictReader(lines, delimiter=delimiter)
            fieldnames = reader.fieldnames
            
            if fieldnames:
                columns = [
                    ColumnInfo(name=name, type='string', description=None)
                    for name in fieldnames
                ]
                
                records = []
                for i, row in enumerate(reader):
                    if i >= sample_size:
                        break
                    records.append(dict(row))
                
                if records:
                    self._infer_column_types(columns, records)
                
                return (columns, records)
            
        except Exception as e:
            logger.debug(f"Failed to parse text sample: {e}")
        
        return None
    
    def _extract_columns_from_records(self, records: List[Dict[str, Any]]) -> List[ColumnInfo]:
        """Extract column information from JSON records."""
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        
        columns = []
        for key in sorted(all_keys):
            # Infer type from first non-null value
            col_type = 'string'
            example_values = []
            
            for record in records:
                if key in record and record[key] is not None:
                    value = record[key]
                    example_values.append(value)
                    
                    if isinstance(value, bool):
                        col_type = 'boolean'
                    elif isinstance(value, int):
                        col_type = 'integer'
                    elif isinstance(value, float):
                        col_type = 'number'
                    elif isinstance(value, (list, dict)):
                        col_type = 'object'
                    
                    if len(example_values) >= 3:
                        break
            
            columns.append(ColumnInfo(
                name=key,
                type=col_type,
                description=None,
                example_values=example_values[:3]
            ))
        
        return columns
    
    def _infer_column_types(self, columns: List[ColumnInfo], records: List[Dict[str, Any]]) -> None:
        """Infer data types for columns based on sample data."""
        for column in columns:
            values = []
            non_null_values = []
            
            for record in records:
                if column.name in record:
                    value = record[column.name]
                    values.append(value)
                    if value is not None and str(value).strip():
                        non_null_values.append(str(value).strip())
                if len(values) >= 10:  # Check more values for better inference
                    break
            
            if non_null_values:
                column.example_values = non_null_values[:3]
                
                # Enhanced type inference
                column.type = self._infer_data_type(non_null_values)
    
    def _infer_data_type(self, values: List[str]) -> str:
        """Infer data type from a list of string values."""
        if not values:
            return 'string'
        
        # Count how many values match each type
        type_counts = {
            'integer': 0,
            'number': 0,
            'boolean': 0,
            'date': 0,
            'datetime': 0,
            'email': 0,
            'url': 0,
            'string': 0
        }
        
        for value in values:
            if self._is_integer(value):
                type_counts['integer'] += 1
            elif self._is_number(value):
                type_counts['number'] += 1
            elif self._is_boolean(value):
                type_counts['boolean'] += 1
            elif self._is_datetime(value):
                type_counts['datetime'] += 1
            elif self._is_date(value):
                type_counts['date'] += 1
            elif self._is_email(value):
                type_counts['email'] += 1
            elif self._is_url(value):
                type_counts['url'] += 1
            else:
                type_counts['string'] += 1
        
        # Determine the most likely type (require at least 70% match)
        total_values = len(values)
        threshold = max(1, int(total_values * 0.7))
        
        # Check in order of specificity
        if type_counts['integer'] >= threshold:
            return 'integer'
        elif type_counts['number'] >= threshold:
            return 'number'
        elif type_counts['boolean'] >= threshold:
            return 'boolean'
        elif type_counts['datetime'] >= threshold:
            return 'datetime'
        elif type_counts['date'] >= threshold:
            return 'date'
        elif type_counts['email'] >= threshold:
            return 'email'
        elif type_counts['url'] >= threshold:
            return 'url'
        else:
            return 'string'
    
    def _is_number(self, value: str) -> bool:
        """Check if string represents a number."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_integer(self, value: str) -> bool:
        """Check if string represents an integer."""
        try:
            int(value)
            return '.' not in str(value)
        except (ValueError, TypeError):
            return False
    
    def _is_boolean(self, value: str) -> bool:
        """Check if string represents a boolean."""
        return str(value).lower() in ('true', 'false', '1', '0', 'yes', 'no')
    
    def _is_date(self, value: str) -> bool:
        """Check if string represents a date."""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{4}/\d{2}/\d{2}$',  # YYYY/MM/DD
            r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
            r'^\d{4}\d{2}\d{2}$',    # YYYYMMDD
        ]
        return any(re.match(pattern, str(value)) for pattern in date_patterns)
    
    def _is_datetime(self, value: str) -> bool:
        """Check if string represents a datetime."""
        datetime_patterns = [
            r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',  # ISO format
            r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',     # MM/DD/YYYY HH:MM:SS
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',     # YYYY-MM-DD HH:MM:SS
        ]
        return any(re.match(pattern, str(value)) for pattern in datetime_patterns)
    
    def _is_email(self, value: str) -> bool:
        """Check if string represents an email address."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, str(value)))
    
    def _is_url(self, value: str) -> bool:
        """Check if string represents a URL."""
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(url_pattern, str(value)))
    
    async def _try_documentation_sample(self, dataset_data: Dict[str, Any]) -> Optional[str]:
        """Try to extract schema information from documentation."""
        documentation_url = dataset_data.get('Documentation', '')
        if not documentation_url:
            return None
        
        if isinstance(documentation_url, list):
            documentation_url = documentation_url[0] if documentation_url else ''
        
        if not documentation_url.startswith('http'):
            return None
        
        try:
            client = await self._get_http_client()
            response = await client.get(documentation_url)
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for schema information
            schema_info = self._extract_schema_from_html(soup)
            return schema_info
            
        except Exception as e:
            logger.debug(f"Failed to fetch documentation: {e}")
        
        return None
    
    def _extract_schema_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract schema information from HTML documentation."""
        schema_parts = []
        
        # Look for tables that might contain schema info
        tables = soup.find_all('table')
        for table in tables:
            headers = [th.get_text().strip() for th in table.find_all('th')]
            if any(keyword in ' '.join(headers).lower() for keyword in ['column', 'field', 'attribute', 'schema']):
                schema_parts.append(f"Table: {table.get_text()[:500]}")
        
        # Look for code blocks that might contain schema
        code_blocks = soup.find_all(['code', 'pre'])
        for code in code_blocks:
            text = code.get_text().strip()
            if any(keyword in text.lower() for keyword in ['schema', 'structure', 'format', 'columns']):
                schema_parts.append(f"Schema: {text[:300]}")
        
        # Look for lists that might describe fields
        lists = soup.find_all(['ul', 'ol'])
        for lst in lists:
            text = lst.get_text()
            if any(keyword in text.lower() for keyword in ['field', 'column', 'attribute']):
                schema_parts.append(f"Fields: {text[:300]}")
        
        return '\n\n'.join(schema_parts) if schema_parts else None
    
    def _try_registry_metadata(self, dataset_data: Dict[str, Any]) -> Optional[str]:
        """Extract information from registry metadata."""
        info_parts = []
        
        # Basic information
        if 'Description' in dataset_data:
            info_parts.append(f"Description: {dataset_data['Description']}")
        
        # Data format information
        if 'Resources' in dataset_data:
            resources = dataset_data['Resources']
            if isinstance(resources, list):
                for resource in resources:
                    if isinstance(resource, dict):
                        desc = resource.get('Description', '')
                        if desc:
                            info_parts.append(f"Resource: {desc}")
        
        # Tags and categories
        if 'Tags' in dataset_data:
            tags = dataset_data['Tags']
            if isinstance(tags, dict):
                for key, value in tags.items():
                    info_parts.append(f"{key}: {value}")
        
        return '\n\n'.join(info_parts) if info_parts else None
    
    async def _try_api_sample(
        self,
        dataset_data: Dict[str, Any],
        sample_size: int
    ) -> Optional[Tuple[List[ColumnInfo], List[Dict[str, Any]]]]:
        """Try to get sample data from API endpoints."""
        # Look for API resources
        api_urls = self._extract_api_urls(dataset_data)
        if not api_urls:
            return None
        
        client = await self._get_http_client()
        
        for url in api_urls:
            try:
                response = await client.get(url)
                response.raise_for_status()
                
                # Try to parse as JSON
                try:
                    data = response.json()
                    if isinstance(data, list) and data:
                        records = data[:sample_size]
                        if isinstance(records[0], dict):
                            columns = self._extract_columns_from_records(records)
                            return (columns, records)
                    elif isinstance(data, dict):
                        # Single record
                        columns = self._extract_columns_from_records([data])
                        return (columns, [data])
                except json.JSONDecodeError:
                    pass
                
            except Exception as e:
                logger.debug(f"Failed to sample API {url}: {e}")
                continue
        
        return None
    
    def _extract_api_urls(self, dataset_data: Dict[str, Any]) -> List[str]:
        """Extract API URLs from dataset data."""
        urls = []
        
        if 'Resources' in dataset_data:
            resources = dataset_data['Resources']
            if isinstance(resources, list):
                for resource in resources:
                    if isinstance(resource, dict):
                        resource_type = resource.get('Type', '').lower()
                        if 'api' in resource_type:
                            # Look for URL in description or other fields
                            desc = resource.get('Description', '')
                            url_match = re.search(r'https?://[^\s]+', desc)
                            if url_match:
                                urls.append(url_match.group())
        
        return urls
    
    def _create_basic_info(self, dataset_data: Dict[str, Any]) -> str:
        """Create basic information when no sample data is available."""
        info_parts = []
        
        info_parts.append("Dataset Information:")
        info_parts.append(f"Name: {dataset_data.get('Name', 'Unknown')}")
        info_parts.append(f"Description: {dataset_data.get('Description', 'No description available')}")
        
        if 'ManagedBy' in dataset_data:
            info_parts.append(f"Managed By: {dataset_data['ManagedBy']}")
        
        if 'License' in dataset_data:
            info_parts.append(f"License: {dataset_data['License']}")
        
        if 'Tags' in dataset_data:
            tags = dataset_data['Tags']
            if isinstance(tags, dict):
                info_parts.append("Tags:")
                for key, value in tags.items():
                    info_parts.append(f"  {key}: {value}")
        
        info_parts.append("\nNote: Sample data is not directly accessible for this dataset.")
        info_parts.append("Please refer to the dataset documentation for detailed schema information.")
        
        return '\n'.join(info_parts)