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

"""awslabs.open-data-registry-mcp-server

An MCP server providing access to AWS's Open Data Registry for dataset discovery and analysis.
"""

from .config import Config
from .dataset_indexer import DatasetIndexer
from .dataset_service import DatasetService
from .models import (
    CategoryInfo,
    ColumnInfo,
    ContactInfo,
    DatasetDetails,
    DatasetSummary,
    ErrorResponse,
    ResourceInfo,
    SampleDataResponse,
    SearchRequest,
    SearchResults,
)
from .registry_service import RegistryService
from .sample_service import SampleService

__version__ = '0.0.0'

__all__ = [
    'Config',
    'CategoryInfo',
    'ColumnInfo', 
    'ContactInfo',
    'DatasetDetails',
    'DatasetIndexer',
    'DatasetService',
    'DatasetSummary',
    'ErrorResponse',
    'RegistryService',
    'ResourceInfo',
    'SampleDataResponse',
    'SampleService',
    'SearchRequest',
    'SearchResults',
]
