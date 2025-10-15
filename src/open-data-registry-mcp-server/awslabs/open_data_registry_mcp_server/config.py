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

"""Configuration settings for Open Data Registry MCP Server."""

import os
from typing import List


class Config:
    """Configuration class for the Open Data Registry MCP Server."""
    
    # Logging configuration
    LOG_LEVEL: str = os.getenv('FASTMCP_LOG_LEVEL', 'WARNING')
    
    # Registry configuration
    REGISTRY_BASE_URL: str = os.getenv(
        'REGISTRY_BASE_URL', 
        'https://raw.githubusercontent.com/awslabs/open-data-registry/main'
    )
    REGISTRY_CACHE_TTL: int = int(os.getenv('REGISTRY_CACHE_TTL', '3600'))  # 1 hour
    
    # Request configuration
    REQUEST_TIMEOUT: int = int(os.getenv('REQUEST_TIMEOUT', '30'))  # 30 seconds
    SAMPLE_REQUEST_TIMEOUT: int = int(os.getenv('SAMPLE_REQUEST_TIMEOUT', '10'))  # 10 seconds
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv('MAX_CONCURRENT_REQUESTS', '5'))
    
    # Sample data configuration
    MAX_SAMPLE_SIZE: int = int(os.getenv('MAX_SAMPLE_SIZE', '1000'))
    DEFAULT_SAMPLE_SIZE: int = int(os.getenv('DEFAULT_SAMPLE_SIZE', '100'))
    
    # AWS configuration
    AWS_REGION: str = os.getenv('AWS_REGION', 'us-east-1')
    
    # Retry configuration
    MAX_RETRIES: int = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_BACKOFF_FACTOR: float = float(os.getenv('RETRY_BACKOFF_FACTOR', '2.0'))
    RETRY_BASE_DELAY: float = float(os.getenv('RETRY_BASE_DELAY', '1.0'))
    
    # Search configuration
    DEFAULT_SEARCH_LIMIT: int = int(os.getenv('DEFAULT_SEARCH_LIMIT', '10'))
    MAX_SEARCH_LIMIT: int = int(os.getenv('MAX_SEARCH_LIMIT', '50'))
    
    # Supported data formats for sample extraction
    SUPPORTED_SAMPLE_FORMATS: List[str] = [
        'csv', 'json', 'jsonl', 'parquet', 'tsv', 'txt', 'xml', 'yaml', 'yml'
    ]
    
    @classmethod
    def validate(cls) -> None:
        """Validate configuration values."""
        if cls.REGISTRY_CACHE_TTL < 0:
            raise ValueError("REGISTRY_CACHE_TTL must be non-negative")
        
        if cls.REQUEST_TIMEOUT <= 0:
            raise ValueError("REQUEST_TIMEOUT must be positive")
        
        if cls.MAX_SAMPLE_SIZE <= 0:
            raise ValueError("MAX_SAMPLE_SIZE must be positive")
        
        if cls.MAX_RETRIES < 0:
            raise ValueError("MAX_RETRIES must be non-negative")
        
        if cls.DEFAULT_SEARCH_LIMIT <= 0 or cls.DEFAULT_SEARCH_LIMIT > cls.MAX_SEARCH_LIMIT:
            raise ValueError("DEFAULT_SEARCH_LIMIT must be positive and <= MAX_SEARCH_LIMIT")


# Validate configuration on import
Config.validate()