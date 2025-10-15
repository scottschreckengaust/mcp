# AWS Open Data Registry MCP Server

A Model Context Protocol (MCP) server that provides access to AWS's Open Data Registry, enabling AI assistants to discover, analyze, and work with open datasets hosted on AWS.

## Overview

The AWS Open Data Registry MCP Server allows AI assistants to:

- **Search and discover** open datasets by keywords, categories, or tags
- **Retrieve detailed metadata** including licensing, formats, and access methods
- **Access sample data** to understand dataset structure and content
- **Get guided analysis prompts** for research and data integration workflows
- **Browse dataset categories** and recent updates

This server connects to the [AWS Open Data Registry](https://github.com/awslabs/open-data-registry), which catalogs datasets made freely available on AWS.

## Features

### 🔍 **MCP Tools**
- `search_datasets` - Find datasets by keywords, categories, or tags
- `get_dataset_details` - Get comprehensive metadata for specific datasets
- `get_sample_data` - Preview dataset structure and sample records
- `list_categories` - Browse available dataset categories

### 📝 **MCP Prompts**
- `analyze_dataset_for_research` - Generate research-focused dataset analysis workflows
- `compare_datasets` - Create structured comparisons between multiple datasets
- `data_integration_assessment` - Evaluate dataset integration possibilities

### 📚 **MCP Resources**
- `dataset://{name}/metadata` - Access dataset metadata as readable resources
- `dataset://{name}/documentation` - Get dataset documentation and schemas
- `registry://categories` - Browse complete category information
- `registry://recent-updates` - View recently updated datasets

## Installation

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager

### Install from Source

```bash
# Clone the repository
git clone https://github.com/awslabs/mcp.git
cd mcp/src/open-data-registry-mcp-server

# Install dependencies
uv sync

# Run the server
uv run awslabs.open-data-registry-mcp-server
```

### Install via uvx

```bash
# Run directly with uvx
uvx awslabs.open-data-registry-mcp-server
```

## Configuration

The server can be configured using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `FASTMCP_LOG_LEVEL` | `WARNING` | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `REGISTRY_CACHE_TTL` | `3600` | Cache TTL in seconds for registry data |
| `REQUEST_TIMEOUT` | `30` | HTTP request timeout in seconds |
| `MAX_SAMPLE_SIZE` | `1000` | Maximum number of sample records |
| `AWS_REGION` | `us-east-1` | AWS region for S3 access |

## Usage Examples

### Basic Dataset Search

```python
# Search for climate-related datasets
results = await search_datasets(
    query="climate",
    limit=5
)

# Search by category
genomics_datasets = await search_datasets(
    category="genomics",
    limit=10
)

# Search with tags
satellite_data = await search_datasets(
    tags=["satellite", "imagery"],
    limit=3
)
```

### Get Dataset Information

```python
# Get detailed information about a dataset
details = await get_dataset_details("landsat-8")

print(f"Dataset: {details.name}")
print(f"Provider: {details.provider}")
print(f"License: {details.license}")
print(f"Formats: {', '.join(details.data_format)}")
```

### Sample Data Access

```python
# Get sample data to understand structure
sample = await get_sample_data(
    dataset_name="common-crawl",
    sample_size=100
)

if sample.columns:
    print("Dataset columns:")
    for col in sample.columns:
        print(f"  {col.name}: {col.type}")

if sample.sample_records:
    print(f"Sample records: {len(sample.sample_records)}")
```

### Using Prompts for Analysis

```python
# Generate research analysis workflow
research_prompt = await analyze_dataset_for_research(
    research_topic="urban air quality",
    data_requirements="Hourly measurements, multiple cities, recent data"
)

# Compare multiple datasets
comparison = await compare_datasets(
    dataset_names=["epa-air-quality", "openaq", "purple-air"]
)

# Assess integration possibilities
integration_plan = await data_integration_assessment(
    primary_dataset="landsat-8",
    integration_goals="Combine satellite imagery with ground truth data"
)
```

### Accessing Resources

```python
# Get dataset metadata as a resource
metadata = await mcp.read_resource("dataset://landsat-8/metadata")

# Get documentation
docs = await mcp.read_resource("dataset://landsat-8/documentation")

# Browse categories
categories = await mcp.read_resource("registry://categories")

# Check recent updates
updates = await mcp.read_resource("registry://recent-updates")
```

## MCP Client Configuration

### Claude Desktop

Add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "open-data-registry": {
      "command": "uvx",
      "args": ["awslabs.open-data-registry-mcp-server"],
      "env": {
        "FASTMCP_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

### Other MCP Clients

For other MCP clients, use:

```bash
uvx awslabs.open-data-registry-mcp-server
```

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov --cov-branch --cov-report=term-missing

# Run only integration tests (requires internet)
uv run pytest -m live

# Run without live tests
uv run pytest -m "not live"
```

### Local Development

```bash
# Install in development mode
uv sync --all-groups

# Run with debug logging
FASTMCP_LOG_LEVEL=DEBUG uv run awslabs.open-data-registry-mcp-server

# Test with MCP Inspector
npx @modelcontextprotocol/inspector uvx awslabs.open-data-registry-mcp-server
```

## Architecture

The server is built with a modular architecture:

- **RegistryService** - Handles GitHub registry access and caching
- **DatasetService** - Provides search and metadata operations
- **SampleService** - Attempts to access sample data from various sources
- **FastMCP Integration** - Exposes tools, prompts, and resources

### Data Access Strategies

The server uses multiple strategies to provide sample data:

1. **S3 Public Access** - Direct access to public S3 buckets
2. **Documentation Parsing** - Extract schema from dataset documentation
3. **Registry Metadata** - Use format information from YAML definitions
4. **API Endpoints** - Access datasets through public APIs

## Supported Data Formats

The server can parse and provide samples for:

- CSV, TSV (Comma/Tab-separated values)
- JSON, JSONL (JavaScript Object Notation)
- XML (Extensible Markup Language)
- YAML (YAML Ain't Markup Language)
- Plain text files

## Error Handling

The server includes comprehensive error handling for:

- Network connectivity issues
- Invalid dataset names
- Rate limiting from external services
- Malformed data files
- Authentication failures

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:

- Create an issue in the [GitHub repository](https://github.com/awslabs/mcp/issues)
- Check the [AWS Open Data Registry](https://github.com/awslabs/open-data-registry) for dataset-specific questions
- Review the [Model Context Protocol documentation](https://modelcontextprotocol.io/)

## Related Projects

- [AWS Open Data Registry](https://github.com/awslabs/open-data-registry) - The source registry
- [Model Context Protocol](https://modelcontextprotocol.io/) - The protocol specification
- [FastMCP](https://github.com/modelcontextprotocol/python-sdk) - Python MCP framework
