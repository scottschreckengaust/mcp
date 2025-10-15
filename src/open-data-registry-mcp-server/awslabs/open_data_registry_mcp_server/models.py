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

"""Pydantic data models for Open Data Registry MCP Server."""

from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional


class ContactInfo(BaseModel):
    """Contact information for dataset providers."""
    
    name: Optional[str] = None
    email: Optional[str] = None
    organization: Optional[str] = None


class ResourceInfo(BaseModel):
    """Information about dataset resources (S3 buckets, APIs, etc.)."""
    
    type: str = Field(description="Type of resource (e.g., 'S3 Bucket', 'API', 'Website')")
    arn: Optional[str] = Field(None, description="AWS ARN if applicable")
    region: Optional[str] = Field(None, description="AWS region if applicable")
    description: Optional[str] = Field(None, description="Description of the resource")


class DatasetSummary(BaseModel):
    """Summary information for a dataset."""
    
    name: str = Field(description="Dataset name/identifier")
    description: str = Field(description="Brief description of the dataset")
    provider: str = Field(description="Organization or entity providing the dataset")
    category: str = Field(description="Dataset category")
    tags: List[str] = Field(default=[], description="List of tags associated with the dataset")
    license: str = Field(description="Dataset license")
    last_updated: str = Field(description="Last update date")
    access_methods: List[str] = Field(default=[], description="Available access methods")


class DatasetDetails(DatasetSummary):
    """Detailed information for a dataset."""
    
    documentation: str = Field(description="Documentation URL or content")
    data_format: List[str] = Field(default=[], description="Data formats available")
    update_frequency: str = Field(description="How frequently the dataset is updated")
    size: str = Field(description="Dataset size information")
    regions: List[str] = Field(default=[], description="AWS regions where data is available")
    contact: Optional[ContactInfo] = Field(None, description="Contact information")
    resources: List[ResourceInfo] = Field(default=[], description="Available resources")


class ColumnInfo(BaseModel):
    """Information about a dataset column/field."""
    
    name: str = Field(description="Column name")
    type: str = Field(description="Data type")
    description: Optional[str] = Field(None, description="Column description")
    example_values: Optional[List[Any]] = Field(None, description="Example values")


class SampleDataResponse(BaseModel):
    """Response containing sample data or schema information."""
    
    dataset_name: str = Field(description="Name of the dataset")
    sample_type: Literal['data', 'schema', 'documentation'] = Field(
        description="Type of sample information provided"
    )
    columns: Optional[List[ColumnInfo]] = Field(None, description="Column information")
    sample_records: Optional[List[Dict[str, Any]]] = Field(None, description="Sample data records")
    schema_info: Optional[str] = Field(None, description="Schema information as text")
    notes: Optional[str] = Field(None, description="Additional notes about the sample")


class CategoryInfo(BaseModel):
    """Information about a dataset category."""
    
    name: str = Field(description="Category name")
    count: int = Field(description="Number of datasets in this category")
    description: Optional[str] = Field(None, description="Category description")


class SearchRequest(BaseModel):
    """Request parameters for dataset search."""
    
    query: str = Field(default="", description="Search keywords")
    category: str = Field(default="", description="Filter by category")
    tags: List[str] = Field(default=[], description="Filter by tags")
    limit: int = Field(default=10, ge=1, le=50, description="Maximum results to return")


class SearchResults(BaseModel):
    """Search results with pagination information."""
    
    datasets: List[DatasetSummary] = Field(description="List of matching datasets")
    total_count: int = Field(description="Total number of matching datasets")
    offset: int = Field(description="Starting offset for this page")
    limit: int = Field(description="Maximum results per page")
    has_more: bool = Field(description="Whether there are more results available")


class ErrorResponse(BaseModel):
    """Standard error response format."""
    
    error: Dict[str, Any] = Field(description="Error details")
    
    @classmethod
    def create(
        cls,
        code: str,
        message: str,
        details: Optional[Any] = None,
        retry_after: Optional[int] = None
    ) -> 'ErrorResponse':
        """Create a standardized error response."""
        error_data = {
            'code': code,
            'message': message
        }
        if details is not None:
            error_data['details'] = details
        if retry_after is not None:
            error_data['retry_after'] = retry_after
            
        return cls(error=error_data)