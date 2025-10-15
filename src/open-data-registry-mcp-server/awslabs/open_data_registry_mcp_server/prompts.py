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

"""MCP prompts for guided dataset analysis workflows."""

from typing import List

from mcp.server.fastmcp import Context
from pydantic import Field

from .dataset_service import DatasetService
from .registry_service import RegistryService


def register_prompts(mcp, registry_service: RegistryService, dataset_service: DatasetService):
    """Register MCP prompts with the FastMCP server."""
    
    @mcp.prompt()
    async def analyze_dataset_for_research(
        ctx: Context,
        research_topic: str = Field(description="Research topic or domain of interest"),
        data_requirements: str = Field(description="Specific data requirements or constraints")
    ) -> str:
        """Generate a comprehensive analysis prompt for finding datasets relevant to research.
        
        This prompt helps researchers systematically evaluate datasets for their research needs,
        providing structured guidance for dataset selection and analysis planning.
        
        Args:
            research_topic: The research topic or domain of interest
            data_requirements: Specific data requirements, constraints, or preferences
        
        Returns:
            A comprehensive analysis prompt for dataset research
        """
        
        # Search for relevant datasets
        try:
            relevant_datasets = await dataset_service.search_datasets(
                query=research_topic,
                limit=10
            )
        except Exception:
            relevant_datasets = []
        
        prompt = f"""# Dataset Analysis for Research: {research_topic}

## Research Context
**Topic:** {research_topic}
**Data Requirements:** {data_requirements}

## Recommended Analysis Approach

### 1. Dataset Discovery and Initial Screening
"""
        
        if relevant_datasets:
            prompt += f"""
**Potentially Relevant Datasets Found ({len(relevant_datasets)}):**
"""
            for i, dataset in enumerate(relevant_datasets[:5], 1):
                prompt += f"""
{i}. **{dataset.name}**
   - Provider: {dataset.provider}
   - Category: {dataset.category}
   - Description: {dataset.description[:200]}{'...' if len(dataset.description) > 200 else ''}
   - License: {dataset.license}
"""
        else:
            prompt += """
**No directly matching datasets found in initial search.**
Consider:
- Broadening search terms
- Exploring related categories
- Checking for datasets in adjacent research domains
"""
        
        prompt += f"""

### 2. Dataset Evaluation Criteria

For each potential dataset, evaluate:

**A. Research Relevance**
- Does the dataset directly address your research questions?
- What aspects of "{research_topic}" does it cover?
- Are there any gaps in coverage for your specific needs?

**B. Data Quality and Completeness**
- What is the temporal coverage (date range)?
- What is the spatial/geographic coverage?
- Are there missing values or data gaps?
- What is the data collection methodology?

**C. Technical Suitability**
- Data format and accessibility
- File sizes and processing requirements
- Required technical skills for analysis
- Integration with your existing tools/workflow

**D. Legal and Ethical Considerations**
- License compatibility with your research
- Attribution requirements
- Any usage restrictions
- Privacy or ethical considerations

### 3. Detailed Dataset Investigation

For promising datasets, investigate:

1. **Get detailed metadata:** Use get_dataset_details tool
2. **Examine data structure:** Use get_sample_data tool
3. **Review documentation:** Check official documentation links
4. **Assess data quality:** Look for validation information
5. **Check update frequency:** Ensure data freshness meets needs

### 4. Research Design Considerations

**Data Requirements Analysis:**
{data_requirements}

**Questions to Address:**
- How will you combine multiple datasets if needed?
- What preprocessing steps will be required?
- Are there potential biases in the data collection?
- What are the limitations of the available data?

### 5. Next Steps

1. **Prioritize datasets** based on evaluation criteria
2. **Download sample data** for top candidates
3. **Perform exploratory data analysis** on samples
4. **Develop data processing pipeline** for selected datasets
5. **Document data provenance** and limitations for your research

### 6. Alternative Data Sources

If Open Data Registry datasets don't fully meet your needs:
- Consider complementary commercial datasets
- Look for academic research datasets
- Explore government agency data portals
- Check domain-specific data repositories

## Evaluation Template

For each dataset, rate (1-5 scale):
- [ ] Relevance to research topic: ___/5
- [ ] Data quality and completeness: ___/5
- [ ] Technical accessibility: ___/5
- [ ] License compatibility: ___/5
- [ ] Documentation quality: ___/5

**Overall suitability score: ___/25**

## Notes and Observations
[Space for your analysis notes]

---
*This analysis framework helps ensure systematic evaluation of datasets for research purposes. Adapt the criteria based on your specific research methodology and requirements.*
"""
        
        return prompt
    
    @mcp.prompt()
    async def compare_datasets(
        ctx: Context,
        dataset_names: List[str] = Field(description="List of dataset names to compare")
    ) -> str:
        """Generate a prompt for comparing multiple datasets across key dimensions.
        
        This prompt provides a structured framework for comparing datasets to help
        users make informed decisions about which datasets best meet their needs.
        
        Args:
            dataset_names: List of dataset names to compare
        
        Returns:
            A structured comparison prompt for the specified datasets
        """
        
        if not dataset_names:
            return """# Dataset Comparison Analysis

Please provide a list of dataset names to compare. Use the search_datasets tool to find relevant datasets first.

## Example Usage
1. Search for datasets: `search_datasets(query="your topic")`
2. Select 2-5 datasets for comparison
3. Use this prompt with the selected dataset names
"""
        
        # Fetch details for each dataset
        dataset_details = {}
        for name in dataset_names:
            try:
                details = await dataset_service.get_dataset_details(name)
                dataset_details[name] = details
            except Exception as e:
                dataset_details[name] = f"Error fetching details: {e}"
        
        prompt = f"""# Dataset Comparison Analysis

## Datasets Being Compared
{', '.join(dataset_names)}

## Comparison Framework

### 1. Basic Information Comparison

| Aspect | {' | '.join(dataset_names)} |
|--------|{' | '.join(['---'] * len(dataset_names))} |
"""
        
        # Add comparison rows
        comparison_aspects = [
            ('Provider', 'provider'),
            ('Category', 'category'),
            ('License', 'license'),
            ('Last Updated', 'last_updated'),
            ('Size', 'size'),
            ('Update Frequency', 'update_frequency')
        ]
        
        for aspect_name, aspect_key in comparison_aspects:
            row = f"| {aspect_name} |"
            for name in dataset_names:
                if isinstance(dataset_details[name], str):
                    value = "Error"
                else:
                    value = getattr(dataset_details[name], aspect_key, 'N/A')
                    if isinstance(value, list):
                        value = ', '.join(value) if value else 'None'
                row += f" {value} |"
            prompt += f"{row}\n"
        
        prompt += """

### 2. Detailed Analysis

"""
        
        for i, name in enumerate(dataset_names, 1):
            details = dataset_details[name]
            if isinstance(details, str):
                prompt += f"""
#### {i}. {name}
**Status:** {details}
"""
            else:
                prompt += f"""
#### {i}. {name}

**Description:** {details.description}

**Key Characteristics:**
- **Data Formats:** {', '.join(details.data_format) if details.data_format else 'Not specified'}
- **Geographic Coverage:** {', '.join(details.regions) if details.regions else 'Not specified'}
- **Access Methods:** {', '.join(details.access_methods) if details.access_methods else 'Not specified'}
- **Tags:** {', '.join(details.tags) if details.tags else 'None'}

**Contact Information:**
"""
                if details.contact:
                    if details.contact.organization:
                        prompt += f"- Organization: {details.contact.organization}\n"
                    if details.contact.email:
                        prompt += f"- Email: {details.contact.email}\n"
                    if details.contact.name:
                        prompt += f"- Contact: {details.contact.name}\n"
                else:
                    prompt += "- No contact information available\n"
                
                prompt += f"""
**Resources:** {len(details.resources)} resource(s) available
**Documentation:** {details.documentation if details.documentation else 'Not provided'}
"""
        
        prompt += """

### 3. Comparison Criteria

Rate each dataset (1-5 scale) on the following criteria:

#### Data Quality and Completeness
"""
        for name in dataset_names:
            prompt += f"- [ ] {name}: ___/5\n"
        
        prompt += """
#### Relevance to Your Use Case
"""
        for name in dataset_names:
            prompt += f"- [ ] {name}: ___/5\n"
        
        prompt += """
#### Technical Accessibility
"""
        for name in dataset_names:
            prompt += f"- [ ] {name}: ___/5\n"
        
        prompt += """
#### Documentation Quality
"""
        for name in dataset_names:
            prompt += f"- [ ] {name}: ___/5\n"
        
        prompt += """
#### License and Usage Terms
"""
        for name in dataset_names:
            prompt += f"- [ ] {name}: ___/5\n"
        
        prompt += """

### 4. Integration Considerations

**Compatibility Analysis:**
- Do the datasets use compatible data formats?
- Are the temporal ranges overlapping or complementary?
- Can the datasets be easily joined or merged?
- Are there common identifiers or keys?

**Processing Requirements:**
- What tools/skills are needed for each dataset?
- What are the computational requirements?
- Are there preprocessing steps needed?

### 5. Decision Matrix

| Dataset | Quality | Relevance | Accessibility | Documentation | License | **Total** |
|---------|---------|-----------|---------------|---------------|---------|-----------|
"""
        for name in dataset_names:
            prompt += f"| {name} | ___/5 | ___/5 | ___/5 | ___/5 | ___/5 | **___/25** |\n"
        
        prompt += """

### 6. Recommendations

Based on your analysis:

1. **Primary Dataset:** [Your choice and reasoning]
2. **Secondary Datasets:** [Complementary datasets and their roles]
3. **Integration Strategy:** [How you plan to combine datasets]
4. **Potential Challenges:** [Issues to address]

### 7. Next Steps

- [ ] Download sample data from top candidates
- [ ] Perform exploratory data analysis
- [ ] Test data integration approaches
- [ ] Validate data quality assumptions
- [ ] Develop processing pipeline

---
*Use this comparison to make informed decisions about dataset selection and integration strategies.*
"""
        
        return prompt
    
    @mcp.prompt()
    async def data_integration_assessment(
        ctx: Context,
        primary_dataset: str = Field(description="Primary dataset name"),
        integration_goals: str = Field(description="Goals for data integration")
    ) -> str:
        """Generate a prompt for assessing data integration possibilities.
        
        This prompt helps evaluate how datasets can be combined and integrated,
        identifying potential challenges and recommending integration approaches.
        
        Args:
            primary_dataset: The name of the primary dataset
            integration_goals: Description of what you want to achieve through integration
        
        Returns:
            A comprehensive data integration assessment prompt
        """
        
        # Get details for the primary dataset
        try:
            primary_details = await dataset_service.get_dataset_details(primary_dataset)
        except Exception as e:
            primary_details = None
        
        # Search for potentially complementary datasets
        try:
            if primary_details:
                # Use category and tags to find related datasets
                related_datasets = await dataset_service.search_datasets(
                    category=primary_details.category,
                    tags=primary_details.tags[:3],  # Use first 3 tags
                    limit=8
                )
                # Remove the primary dataset from results
                related_datasets = [d for d in related_datasets if d.name != primary_dataset]
            else:
                related_datasets = []
        except Exception:
            related_datasets = []
        
        prompt = f"""# Data Integration Assessment

## Integration Context
**Primary Dataset:** {primary_dataset}
**Integration Goals:** {integration_goals}

## Primary Dataset Analysis
"""
        
        if primary_details:
            prompt += f"""
**Dataset Overview:**
- **Description:** {primary_details.description}
- **Provider:** {primary_details.provider}
- **Category:** {primary_details.category}
- **Data Formats:** {', '.join(primary_details.data_format) if primary_details.data_format else 'Not specified'}
- **Geographic Coverage:** {', '.join(primary_details.regions) if primary_details.regions else 'Not specified'}
- **Update Frequency:** {primary_details.update_frequency}
- **Size:** {primary_details.size}

**Key Characteristics for Integration:**
- **Tags:** {', '.join(primary_details.tags) if primary_details.tags else 'None'}
- **Access Methods:** {', '.join(primary_details.access_methods) if primary_details.access_methods else 'Not specified'}
- **License:** {primary_details.license}
"""
        else:
            prompt += f"""
**Error:** Could not retrieve details for dataset '{primary_dataset}'.
Please verify the dataset name and try again.
"""
        
        if related_datasets:
            prompt += f"""

## Potentially Complementary Datasets

Based on category and tags, these datasets might be suitable for integration:
"""
            for i, dataset in enumerate(related_datasets[:5], 1):
                prompt += f"""
### {i}. {dataset.name}
- **Provider:** {dataset.provider}
- **Description:** {dataset.description[:150]}{'...' if len(dataset.description) > 150 else ''}
- **Category:** {dataset.category}
- **License:** {dataset.license}
- **Access Methods:** {', '.join(dataset.access_methods) if dataset.access_methods else 'Not specified'}
"""
        
        prompt += f"""

## Integration Assessment Framework

### 1. Integration Goals Analysis
**Your stated goals:** {integration_goals}

**Questions to consider:**
- What specific insights do you hope to gain from integration?
- Are you looking to enrich, validate, or expand your primary dataset?
- What are the key variables or dimensions you want to combine?
- What is your target output format or analysis approach?

### 2. Technical Compatibility Assessment

For each potential integration dataset, evaluate:

#### A. Data Format Compatibility
- [ ] **File formats:** Are formats compatible or easily convertible?
- [ ] **Data structures:** Do schemas align or can they be harmonized?
- [ ] **Encoding:** Are character encodings consistent?

#### B. Temporal Alignment
- [ ] **Time periods:** Do datasets cover overlapping time periods?
- [ ] **Temporal resolution:** Are time granularities compatible?
- [ ] **Update cycles:** Do update frequencies align for ongoing integration?

#### C. Spatial/Geographic Alignment
- [ ] **Geographic coverage:** Do datasets cover the same regions?
- [ ] **Coordinate systems:** Are spatial reference systems compatible?
- [ ] **Resolution:** Are spatial granularities appropriate for integration?

#### D. Semantic Compatibility
- [ ] **Variable definitions:** Are key variables defined consistently?
- [ ] **Units of measurement:** Are units compatible or convertible?
- [ ] **Classification systems:** Do categorical variables align?

### 3. Integration Strategies

#### Strategy 1: Direct Joining
**When to use:** Datasets share common identifiers
**Requirements:**
- Common key fields (IDs, coordinates, timestamps)
- Compatible data types for join keys
- Reasonable overlap in key values

**Implementation steps:**
1. Identify common key fields
2. Standardize key formats
3. Perform join operation (inner, left, outer)
4. Handle missing values and duplicates

#### Strategy 2: Spatial Integration
**When to use:** Datasets have geographic components
**Requirements:**
- Spatial coordinates or geographic identifiers
- Compatible coordinate reference systems
- Appropriate spatial resolution

**Implementation steps:**
1. Standardize coordinate systems
2. Perform spatial joins or overlays
3. Handle boundary effects
4. Aggregate to common spatial units

#### Strategy 3: Temporal Integration
**When to use:** Time-series data with different temporal patterns
**Requirements:**
- Timestamp information
- Compatible time zones and formats
- Overlapping time periods

**Implementation steps:**
1. Standardize timestamp formats
2. Align temporal resolution
3. Handle missing time periods
4. Synchronize update cycles

#### Strategy 4: Statistical Integration
**When to use:** Datasets cannot be directly joined
**Requirements:**
- Statistical relationships between variables
- Sufficient sample sizes
- Appropriate statistical methods

**Implementation steps:**
1. Identify statistical relationships
2. Develop integration models
3. Validate integration accuracy
4. Quantify uncertainty

### 4. Integration Challenges and Solutions

#### Common Challenges:
1. **Scale mismatches:** Different levels of aggregation
2. **Missing data:** Incomplete coverage in one or more datasets
3. **Quality differences:** Varying data quality standards
4. **License conflicts:** Incompatible usage terms
5. **Processing complexity:** Technical integration difficulties

#### Mitigation Strategies:
- **Data harmonization:** Standardize formats and definitions
- **Quality assessment:** Evaluate and document data quality
- **Gap analysis:** Identify and address missing data
- **Legal review:** Ensure license compatibility
- **Pilot testing:** Start with small-scale integration tests

### 5. Implementation Plan

#### Phase 1: Preparation
- [ ] Download sample data from all datasets
- [ ] Perform exploratory data analysis
- [ ] Document data schemas and formats
- [ ] Identify integration keys and methods

#### Phase 2: Pilot Integration
- [ ] Implement integration on sample data
- [ ] Test different integration strategies
- [ ] Evaluate integration quality
- [ ] Document lessons learned

#### Phase 3: Full Integration
- [ ] Scale up successful integration approach
- [ ] Implement quality control measures
- [ ] Create integration documentation
- [ ] Establish update procedures

#### Phase 4: Validation
- [ ] Validate integrated dataset
- [ ] Compare with expected outcomes
- [ ] Test with downstream analysis
- [ ] Document integration metadata

### 6. Success Metrics

Define how you'll measure integration success:
- [ ] **Completeness:** Percentage of records successfully integrated
- [ ] **Accuracy:** Validation against known ground truth
- [ ] **Consistency:** Internal consistency checks
- [ ] **Utility:** Fitness for intended analysis purpose

### 7. Risk Assessment

**High Risk Factors:**
- Incompatible licenses
- Poor data quality in key datasets
- Complex technical integration requirements
- Lack of common identifiers

**Medium Risk Factors:**
- Different update frequencies
- Scale mismatches
- Missing documentation

**Low Risk Factors:**
- Minor format differences
- Temporal gaps in non-critical periods
- Different but convertible units

### 8. Next Steps

1. **Prioritize integration candidates** based on assessment
2. **Develop detailed integration plan** for top candidates
3. **Create integration prototype** with sample data
4. **Validate integration approach** before full implementation
5. **Document integration process** for reproducibility

---
*This assessment provides a systematic approach to evaluating and planning data integration projects. Adapt the framework based on your specific technical requirements and constraints.*
"""
        
        return prompt