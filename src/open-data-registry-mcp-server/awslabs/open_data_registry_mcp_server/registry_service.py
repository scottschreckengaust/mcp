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

"""Fixed Registry service for accessing Open Data Registry from GitHub."""

import asyncio
import json
import re
import time
from typing import Any, Dict, List, Optional

import httpx
import yaml
from loguru import logger

from .config import Config


class RegistryService:
    """Service for accessing and caching Open Data Registry data."""
    
    def __init__(self):
        """Initialize the registry service."""
        self.base_url = Config.REGISTRY_BASE_URL
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_timestamps: Dict[str, float] = {}
        self.cache_ttl = Config.REGISTRY_CACHE_TTL
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_REQUESTS)
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(Config.REQUEST_TIMEOUT),
                headers={
                    'User-Agent': 'awslabs-open-data-registry-mcp-server/0.0.0'
                }
            )
        return self._client
    
    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache entry is still valid."""
        if key not in self.cache or key not in self.cache_timestamps:
            return False
        
        age = time.time() - self.cache_timestamps[key]
        return age < self.cache_ttl
    
    def _cache_set(self, key: str, value: Any) -> None:
        """Set cache entry with timestamp."""
        self.cache[key] = value
        self.cache_timestamps[key] = time.time()
    
    def _cache_get(self, key: str) -> Optional[Any]:
        """Get cache entry if valid."""
        if self._is_cache_valid(key):
            return self.cache[key]
        return None
    
    async def _fetch_with_retry(self, url: str) -> str:
        """Fetch URL with exponential backoff retry."""
        client = await self._get_client()
        
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                async with self._semaphore:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response.text
            
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    raise FileNotFoundError(f"Resource not found: {url}")
                elif e.response.status_code in (429, 503):
                    # Rate limited or service unavailable
                    if attempt < Config.MAX_RETRIES:
                        delay = Config.RETRY_BASE_DELAY * (Config.RETRY_BACKOFF_FACTOR ** attempt)
                        logger.warning(f"Rate limited, retrying in {delay}s (attempt {attempt + 1})")
                        await asyncio.sleep(delay)
                        continue
                    raise
                else:
                    raise
            
            except (httpx.RequestError, httpx.TimeoutException) as e:
                if attempt < Config.MAX_RETRIES:
                    delay = Config.RETRY_BASE_DELAY * (Config.RETRY_BACKOFF_FACTOR ** attempt)
                    logger.warning(f"Request failed, retrying in {delay}s (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(delay)
                    continue
                raise
        
        raise Exception(f"Failed to fetch {url} after {Config.MAX_RETRIES + 1} attempts")
    
    async def fetch_dataset_list(self) -> List[str]:
        """Fetch list of dataset YAML files from registry."""
        cache_key = "dataset_list"
        cached_result = self._cache_get(cache_key)
        if cached_result is not None:
            logger.debug("Returning cached dataset list")
            return cached_result
        
        try:
            # Use GitHub API to fetch the actual list of dataset files
            # The Open Data Registry maintains its datasets in a specific GitHub repo
            github_api_url = "https://api.github.com/repos/awslabs/open-data-registry/contents/datasets"
            
            client = await self._get_client()
            
            # Try GitHub API first
            try:
                response = await client.get(github_api_url)
                response.raise_for_status()
                
                files = response.json()
                dataset_names = []
                
                for file_info in files:
                    if file_info.get('name', '').endswith('.yaml'):
                        # Remove .yaml extension to get dataset name
                        dataset_name = file_info['name'][:-5]
                        dataset_names.append(dataset_name)
                
                logger.info(f"Fetched {len(dataset_names)} datasets from GitHub API")
                
                if dataset_names:
                    self._cache_set(cache_key, dataset_names)
                    return dataset_names
                
            except Exception as e:
                logger.warning(f"GitHub API failed, falling back to alternative method: {e}")
            
            # Alternative method: Try fetching the directory listing from raw GitHub
            # This approach fetches the HTML page and parses dataset names
            raw_url = "https://github.com/awslabs/open-data-registry/tree/main/datasets"
            
            try:
                response = await client.get(raw_url)
                response.raise_for_status()
                
                # Parse HTML to extract .yaml filenames
                # Look for patterns like "datasets/dataset-name.yaml"
                pattern = r'datasets/([^/]+)\.yaml'
                matches = re.findall(pattern, response.text)
                
                if matches:
                    dataset_names = list(set(matches))  # Remove duplicates
                    logger.info(f"Fetched {len(dataset_names)} datasets from HTML parsing")
                    self._cache_set(cache_key, dataset_names)
                    return dataset_names
                    
            except Exception as e:
                logger.warning(f"HTML parsing failed: {e}")
            
            # Final fallback: Use an expanded hardcoded list
            # This ensures the service works even if external fetching fails
            fallback_datasets = [
                "1000-genomes",
                "1000-genomes-data-lakehouse-ready",
                "1kg-ont-vienna",
                "3dcompat",
                "3kricegenome",
                "4dnucleome",
                "990-spreadsheets",
                "abeja-cc-ja",
                "aev-a2d2",
                "africa-field-boundary-labels",
                "afsis",
                "ag-loam",
                "ai3",
                "airborne-object-tracking",
                "aiwp",
                "allen-brain-observatory",
                "allen-cell-imaging-collections",
                "allen-ivy-glioblastoma-atlas",
                "allen-mouse-brain-atlas",
                "allen-nd-ephys-compression",
                "allen-nd-open-data",
                "allen-sea-ad-atlas",
                "allen-synphys",
                "allenai-arc",
                "allenai-aristo-mini",
                "allenai-diagrams",
                "allenai-drop",
                "allenai-meaningful-citations",
                "allenai-quoref",
                "allenai-ropes",
                "allenai-tablestore",
                "allenai-tablestore-questions",
                "allenai-tqa",
                "allenai-tuple-kb",
                "allenai-zest",
                "allthebacteria",
                "amazon-berkeley-objects",
                "amazon-bin-imagery",
                "amazon-conversational-product-search",
                "amazon-last-mile-challenges",
                "amazon-pqa",
                "amazon-reviews-ml",
                "amazon-seller-contact-intent-sequence",
                "amazonia",
                "answer-reformulation",
                "aodn_animal_acoustic_tracking_delayed_qc",
                "aodn_animal_ctd_satellite_relay_tagging_delayed_qc",
                "aodn_model_sea_level_anomaly_gridded_realtime",
                "aodn_mooring_ctd_delayed_qc",
                "aodn_mooring_hourly_timeseries_delayed_qc",
                "aodn_mooring_satellite_altimetry_calibration_validation",
                "aodn_radar_bonneycoast_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_capricornbunkergroup_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_capricornbunkergroup_wave_delayed_qc",
                "aodn_radar_capricornbunkergroup_wind_delayed_qc",
                "aodn_radar_coffsharbour_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_coffsharbour_wave_delayed_qc",
                "aodn_radar_coffsharbour_wind_delayed_qc",
                "aodn_radar_coralcoast_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_newcastle_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_northwestshelf_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_rottnestshelf_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_rottnestshelf_wave_delayed_qc",
                "aodn_radar_rottnestshelf_wind_delayed_qc",
                "aodn_radar_southaustraliagulfs_velocity_hourly_averaged_delayed_qc",
                "aodn_radar_southaustraliagulfs_wave_delayed_qc",
                "aodn_radar_southaustraliagulfs_wind_delayed_qc",
                "aodn_radar_turquoisecoast_velocity_hourly_averaged_delayed_qc",
                "aodn_satellite_chlorophylla_carder_1day_aqua",
                "aodn_satellite_chlorophylla_gsm_1day_aqua",
                "aodn_satellite_chlorophylla_gsm_1day_noaa20",
                "aodn_satellite_chlorophylla_gsm_1day_snpp",
                "aodn_satellite_chlorophylla_oc3_1day_aqua",
                "aodn_satellite_chlorophylla_oc3_1day_noaa20",
                "aodn_satellite_chlorophylla_oc3_1day_snpp",
                "aodn_satellite_chlorophylla_oci_1day_aqua",
                "aodn_satellite_chlorophylla_oci_1day_noaa20",
                "aodn_satellite_chlorophylla_oci_1day_snpp",
                "aodn_satellite_diffuse_attenuation_coefficent_1day_aqua",
                "aodn_satellite_diffuse_attenuation_coefficent_1day_noaa20",
                "aodn_satellite_diffuse_attenuation_coefficent_1day_snpp",
                "aodn_satellite_ghrsst_l3c_1day_nighttime_himawari8",
                "aodn_satellite_ghrsst_l3s_1day_daynighttime_multi_sensor_australia",
                "aodn_satellite_ghrsst_l3s_1day_daynighttime_single_sensor_australia",
                "aodn_satellite_ghrsst_l3s_1day_daynighttime_single_sensor_southernocean",
                "aodn_satellite_ghrsst_l3s_1month_daytime_single_sensor_australia",
                "aodn_satellite_ghrsst_l3s_3day_daynighttime_multi_sensor_australia",
                "aodn_satellite_ghrsst_l3s_6day_daynighttime_single_sensor_australia",
                "aodn_satellite_ghrsst_l4_gamssa_1day_multi_sensor_world",
                "aodn_satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia",
                "aodn_satellite_nanoplankton_fraction_oc3_1day_aqua",
                "aodn_satellite_net_primary_productivity_gsm_1day_aqua",
                "aodn_satellite_net_primary_productivity_oc3_1day_aqua",
                "aodn_satellite_optical_water_type_1day_aqua",
                "aodn_satellite_picoplankton_fraction_oc3_1day_aqua",
                "aodn_slocum_glider_delayed_qc",
                "aodn_vessel_air_sea_flux_product_delayed",
                "aodn_vessel_air_sea_flux_sst_meteo_realtime",
                "aodn_vessel_co2_delayed_qc",
                "aodn_vessel_fishsoop_realtime_qc",
                "aodn_vessel_sst_delayed_qc",
                "aodn_vessel_trv_realtime_qc",
                "aodn_vessel_xbt_delayed_qc",
                "aodn_vessel_xbt_realtime_nonqc",
                "aodn_wave_buoy_realtime_nonqc",
                "apd_galaxymorph",
                "apd_galaxysegmentation",
                "apex",
                "argo-gdac-marinedata",
                "argoverse",
                "arpa-e-perform",
                "asem-project",
                "asf-event-data",
                "askap",
                "asr-error-robustness",
                "asset-data-igp-coal-plant",
                "aster-l1t",
                "aurora_msds",
                "australasian-genomics",
                "aws-covid19-lake",
                "aws-igenomes",
                "aws-public-blockchain",
                "bdsp-harvard-eeg",
                "bdsp-heedb",
                "bdsp-hsp",
                "bdsp-icare",
                "bdsp-sparcnet",
                "beataml",
                "bhl-open-data",
                "binding-db",
                "biolip",
                "black_marble_combustion",
                "blended-tropomi-gosat-methane",
                "blue_et",
                "bluebrain_opendata",
                "bobsrepository",
                "bodym",
                "boltz1",
                "boreas",
                "bossdb",
                "bps_microscopy",
                "bps_rnaseq",
                "brain-encoding-response-generator",
                "brainminds-marmoset-connectivity",
                "brazil-data-cubes",
                "broad-gnomad",
                "broad-pan-ukb",
                "broad-references",
                "busco-data",
                "c2smsfloods",
                "caendr",
                "caladapt-coproduced-climate-data",
                "caladapt-wildfire-dataset",
                "camelyon",
                "canelevation-dem",
                "canelevation-pointcloud",
                "capella_opendata",
                "carbonpdf",
                "cartostore",
                "catalyst-cooperative-pudl",
                "cbers",
                "ccic",
                "ccle",
                "cell-painting-image-collection",
                "cellpainting-gallery",
                "census-2010-amc-mdf-replicates",
                "census-2010-dhc-nmf",
                "census-2010-pl94-nmf",
                "census-2020-amc-mdf-replicates",
                "census-2020-dhc-nmf",
                "census-2020-pl94-gls",
                "census-2020-pl94-nmf",
                "census-dataworld-pums",
                "cesm-hr",
                "cgci",
                "cgiardata",
                "challenge-2021",
                "chembl",
                "chimera",
                "citrus-farm",
                "civic",
                "clay-model-v0-embeddings",
                "clay-v1-5-naip-2",
                "clay-v1-5-sentinel2",
                "clinical-ultrasound-image-data",
                "clinvar",
                "cmas-data-warehouse",
                "cmip6",
                "cmip6-era5-hybrid-southeast-asia",
                "cmsdesynpuf-omop",
                "coawst",
                "cobra",
                "code-mixed-ner",
                "colorado-elevation-data",
                "colorado-imagery",
                "commoncrawl",
                "comonscreens",
                "copernicus-dem",
                "coralreef-image-classification-training",
                "cord-19",
                "cornell-eas-data-lake",
                "cotonoha-dic",
                "covers-br",
                "cptac-2",
                "cptac-3",
                "craam-open-vlf",
                "cropland_partitioining",
                "cryoet-data-portal",
                "cse-cic-ids2018",
                "csiro-cafe60",
                "ctrees-amazon-canopy-height",
                "ctrees-california-vhr-tree-height",
                "ctsp-dlbcl",
                "cwa_opendata",
                "cwb_opendata",
                "czb-opencell",
                "czi-benchmarking",
                "czi-cellxgene-census",
                "czi-imaging-bsd",
                "czi-imagining-mit",
                "czi-transcriptomics-mit",
                "dandiarchive",
                "darpa-invisible-headlights",
                "dataforgood-fb-forests",
                "dataforgood-fb-hrsl",
                "daylight-osm",
                "dc-lidar",
                "dc-lidar-2015",
                "dcc",
                "deafrica-alos-jers",
                "deafrica-chirps",
                "deafrica-clgm-lwq",
                "deafrica-coastlines",
                "deafrica-crop-extent",
                "deafrica-fractional-cover",
                "deafrica-geomad",
                "deafrica-landsat",
                "deafrica-mangrove",
                "deafrica-ndvi_anomaly",
                "deafrica-ndvi_climatology_ls",
                "deafrica-sentinel-1",
                "deafrica-sentinel-1-mosaic",
                "deafrica-sentinel-2",
                "deafrica-sentinel-2-c1",
                "deafrica-waterbodies",
                "deafrica-wofs",
                "deepdrug-dpeb",
                "dendritic-consortium",
                "dep-coastlines",
                "dep-ls-geomads",
                "dep-mangroves",
                "dep-s1-annual-mosaics",
                "dep-s2-geomads",
                "dep-wofs",
                "depmap-omics-ccle",
                "deutsche-boerse-pds",
                "dharani-brain-dataset",
                "dialoglue",
                "dig-open-analysis-data",
                "digital-globe-open-data",
                "digitalcorpora",
                "dmi-danra-05",
                "dmi-opendata",
                "dmspssj",
                "dnastack-covid-19-sra-data",
                "e11bio-prism",
                "eai-essential-web-v1",
                "ebd-sentinel-1-global-coherence-backscatter",
                "ecmwf-era5",
                "ecmwf-forecasts",
                "eegdash",
                "elp-nouabale-landscape",
                "emearth",
                "emory-breast-imaging-dataset-embed",
                "encode-project",
                "enhance-pet-1-6k",
                "eot-web-archive",
                "epa-2022-modeling-platform",
                "epa-ccte-httr",
                "epa-edde-v1",
                "epa-edde-v2",
                "epa-equates-v1",
                "epa-rsei-pds",
                "epoch-of-reionization",
                "era5-for-wrf",
                "esa-worldcover",
                "esa-worldcover-vito",
                "esa-worldcover-vito-composites",
                "euclid-q1",
                "euro-cordex",
                "exceptional-responders",
                "fashionlocaltriplets",
                "fast-ai-coco",
                "fast-ai-imageclas",
                "fast-ai-imagelocal",
                "fast-ai-nlp",
                "fcp-indi",
                "fm-ad",
                "fmi-radar",
                "foldingathome-covid19",
                "ford-multi-av-seasonal",
                "fvcom_gom3",
                "gadal",
                "gaia-dr3",
                "galex",
                "gatk-sv-data",
                "gatk-test-data",
                "gbif",
                "gcbo-dataset",
                "gdelt",
                "gdr-data-lake",
                "genomeark",
                "genomekit",
                "geo_tide_geojsons",
                "geoglows-v2",
                "geonet",
                "geoschem-input-data",
                "geoschem-nested-input-data",
                "giab",
                "glad-landsat-ard",
                "glo-30-hand",
                "global-drought-flood-catalogue",
                "gmsdata",
                "gnomad-data-lakehouse-ready",
                "gnss-ro-opendata",
                "google-brain-genomics-public",
                "google-ngrams",
                "graf-reforecast",
                "green_et",
                "gretel-synthetic-safety-alignment-en-v1",
                "grillo-openeew",
                "gtgseq",
                "gulfwide-avian-monitoring",
                "guys-breast-cancer-lymph-nodes",
                "hail-vep-pipeline",
                "hcmi-cmdc",
                "hcp-openaccess",
                "hecatomb",
                "helpful-sentences-from-reviews",
                "hirlam",
                "hpgp-data",
                "hsip-lidar-us-cities",
                "hst",
                "human-microbiome-project",
                "humancellatlas",
                "humor-detection",
                "humor-patterns",
                "hycom-global-drifters",
                "hycom-gofs-3pt1-reanalysis",
                "ibl-autism",
                "ibl-behaviour",
                "ibl-brain-wide-map",
                "ibl-reproducible-ephys",
                "iceye-opendata",
                "icgc",
                "ichangemycity",
                "ideam-radares",
                "ihart",
                "ilmn-dragen-1kgp",
                "in-elevation",
                "in-imagery",
                "inaturalist-open-data",
                "indian-high-court-judgments",
                "indian-supreme-court-judgments",
                "inlab-covid-19-images-dataset",
                "intelinair_agriculture_vision",
                "intelinair_corn_kernel_counting",
                "intelinair_longitudinal_nutrient_deficiency",
                "io-lulc",
                "irs990",
                "isdasoil",
                "iserv",
                "isic-archive",
                "its-live-data",
                "janelia-cosem",
                "janelia-flylight",
                "janelia-mouselight",
                "jaxa-alos-palsar2-scansar",
                "jaxa-usgs-nasa-kaguya-tc",
                "jaxa-usgs-nasa-kaguya-tc-dtms",
                "jhu-indexes",
                "jwst",
                "k2",
                "kaiju-indexes",
                "kepler",
                "kids-first",
                "kitti",
                "klarna_productpage_dataset",
                "kraken2-ncbi-refseq-complete-v205",
                "kyfromabove",
                "lab41-sri-voices",
                "ladi",
                "landsat-8",
                "lei",
                "leopard",
                "loc-sanborn-maps",
                "lofar-elais-n1",
                "lowcontext-ner-gaz",
                "ltrf-cqa-dataset",
                "luad-eagle",
                "lwi-model-data",
                "m3ed",
                "maf-genome",
                "man-truckscenes",
                "mapping-africa",
                "marine-energy-data",
                "materials-project",
                "maxar-open-data",
                "mbers-open-data",
                "mcrpc",
                "megascenes",
                "met-office-global-deterministic",
                "met-office-global-ensemble",
                "met-office-global-ocean",
                "met-office-global-wave",
                "met-office-nws-ocean",
                "met-office-nws-wave",
                "met-office-uk-deterministic",
                "met-office-uk-ensemble",
                "met-office-uk-radar-observations",
                "met-office-ukesm1-arise",
                "metagraph",
                "meteo-france-models",
                "mevadata",
                "mimic-iv-demo",
                "mimic-iv-ecg",
                "mimiciii",
                "mirrulations",
                "mmid",
                "mmrf-commpass",
                "modis",
                "modis-astraea",
                "mogreps",
                "molssi-covid19-hub",
                "monkey",
                "mosaic",
                "motional-nuplan",
                "motional-nuscenes",
                "mp2prt",
                "mrkr",
                "msd",
                "multi-token-completion",
                "multiconer",
                "multimedia-commons",
                "mur",
                "mwis-vr-instances",
                "naip",
                "nanopore",
                "napierone",
                "nara-1940-census",
                "nara-1950-census",
                "nara-national-archives-catalog",
                "nasa-airibrad",
                "nasa-airicrad",
                "nasa-astl1t",
                "nasa-atl03",
                "nasa-atl08",
                "nasa-ega",
                "nasa-gcms",
                "nasa-gedi02a",
                "nasa-gedil4aagbdensityv212056",
                "nasa-gpm2adpr",
                "nasa-gpm3imergde",
                "nasa-gpm3imergdf",
                "nasa-gpm3imergdl",
                "nasa-gpm3imerghh",
                "nasa-gpm3imerghhe",
                "nasa-gpm3imerghhl",
                "nasa-gpm3imergm",
                "nasa-gpmimerglandseamask",
                "nasa-gpmmergir",
                "nasa-heasarc",
                "nasa-hlsl30",
                "nasa-hlss30",
                "nasa-imergprecipcanadaalaska2097",
                "nasa-lambda",
                "nasa-m2i3npasm",
                "nasa-m2i3nvaer",
                "nasa-m2i3nvasm",
                "nasa-m2t1nxslv",
                "nasa-mcd43a1",
                "nasa-mcd43a3",
                "nasa-mcd43a4",
                "nasa-mi1b2e",
                "nasa-mod02hkm",
                "nasa-mod09a1",
                "nasa-mod09ga",
                "nasa-mod09gq",
                "nasa-mod13q1",
                "nasa-mod16a2",
                "nasa-modis-t-jpl-l2p-v2019-0",
                "nasa-mur-jpl-l4-glob-v41",
                "nasa-myd09ga",
                "nasa-myd09gq",
                "nasa-operal2cslc-s1-staticv1",
                "nasa-operal2cslc-s1v1",
                "nasa-operal2rtc-s1-staticv1",
                "nasa-operal2rtc-s1v1",
                "nasa-operal3disp-s1v1",
                "nasa-operal3dist-alert-hlsprovisionalv0",
                "nasa-operal3dist-alert-hlsv1",
                "nasa-operal3dist-ann-hlsv1",
                "nasa-operal3dswx-hlsv1",
                "nasa-operal3dswx-s1v1",
                "nasa-osdr",
                "nasa-power",
                "nasa-psi",
                "nasa-sentinel-1adpgrdhigh",
                "nasa-sentinel-1aslc",
                "nasa-sentinel-1bdpgrdhigh",
                "nasa-sentinel-1bslc",
                "nasa-soho-comet-challenge-on-aws",
                "nasa-soteria-data",
                "nasa-usgs-controlled-mro-ctx-dtms",
                "nasa-usgs-europa-dtms",
                "nasa-usgs-europa-mosaics",
                "nasa-usgs-europa-observations",
                "nasa-usgs-lunar-orbiter-laser-altimeter",
                "nasa-usgs-mars-hirise",
                "nasa-usgs-mars-hirise-dtms",
                "nasa-usgs-themis-mosaics",
                "nasanex",
                "naturalearth",
                "ncar-cesm-lens",
                "ncar-cesm2-arise",
                "ncar-cesm2-lens",
                "ncar-dart-cam6",
                "ncar-na-cordex",
                "ncbi-blast-databases",
                "ncbi-covid-19",
                "ncbi-fcs-gx",
                "ncbi-pmc",
                "ncbi-sra",
                "ncei-wcsd-archive",
                "nci-imaging-data-commons",
                "nciccr-dlbcl",
                "ndui",
                "nex-gddp-cmip6",
                "nifs-lhd",
                "nj-imagery",
                "nj-lidar",
                "noaa-arl-hysplit",
                "noaa-bathymetry",
                "noaa-cdr-atmospheric",
                "noaa-cdr-fundamental",
                "noaa-cdr-oceanic",
                "noaa-cdr-terrestrial",
                "noaa-cfs",
                "noaa-climate-normals",
                "noaa-coastal-lidar",
                "noaa-dcdb-bathymetry-pds",
                "noaa-eri",
                "noaa-gefs",
                "noaa-gefs-reforecast",
                "noaa-gestofs",
                "noaa-gfs-bdp-pds",
                "noaa-gfs-pds",
                "noaa-ghcn",
                "noaa-ghe",
                "noaa-gk2a-pds",
                "noaa-gmgsi",
                "noaa-goes",
                "noaa-gsod",
                "noaa-himawari",
                "noaa-historicalcharts",
                "noaa-hrrr-pds",
                "noaa-isd",
                "noaa-jpss",
                "noaa-mrms-pds",
                "noaa-nam",
                "noaa-nbm",
                "noaa-nbm-parallel",
                "noaa-nclimgrid",
                "noaa-ncn",
                "noaa-ndfd",
                "noaa-nesdis-swfo-ccor-1-pds",
                "noaa-nesdis-tcprimed-pds",
                "noaa-nexrad",
                "noaa-nodd-kerchunk",
                "noaa-nos-cora",
                "noaa-nos-scuba-icesat2-pds",
                "noaa-nos-stofs3d",
                "noaa-nwm-pds",
                "noaa-nws-aorc",
                "noaa-nws-fourcastnetgfs",
                "noaa-nws-graphcastgfs-pds",
                "noaa-nws-hafs",
                "noaa-nws-naqfc-pds",
                "noaa-nws-wam-ipe",
                "noaa-oar-arl-nacc-pds",
                "noaa-oar-hourly-gdp",
                "noaa-oar-myrorss-pds",
                "noaa-ocean-climate-stations",
                "noaa-ocs-hydrodata",
                "noaa-ofs",
                "noaa-rap",
                "noaa-reanalyses-pds",
                "noaa-rrfs",
                "noaa-rtma",
                "noaa-rtofs",
                "noaa-s102",
                "noaa-s111",
                "noaa-space-weather",
                "noaa-swdi",
                "noaa-ufs-coastal-pds",
                "noaa-ufs-gdas-pds",
                "noaa-ufs-gefsv13replay-pds",
                "noaa-ufs-htf-pds",
                "noaa-ufs-land-da",
                "noaa-ufs-marinereanalysis",
                "noaa-ufs-regtests",
                "noaa-ufs-s2s",
                "noaa-ufs-shortrangeweather",
                "noaa-uwpd-cmip5",
                "noaa-wave-ensemble-reforecast",
                "noaa-wod",
                "noaa-wsa-enlil",
                "northern-california-earthquakes",
                "nrel-pds-building-stock",
                "nrel-pds-dsgrid",
                "nrel-pds-ncdb",
                "nrel-pds-nsrdb",
                "nrel-pds-porotomo",
                "nrel-pds-sup3rcc",
                "nrel-pds-windai",
                "nrel-pds-wtk",
                "nsd",
                "nsf-ncar-era5",
                "nsw-herbarium",
                "nwm-archive",
                "nyc-tlc-trip-records-pds",
                "nyu-fastmri",
                "nyumets-brain",
                "nz-coastal",
                "nz-elevation",
                "nz-imagery",
                "obis",
                "oceanomics",
                "ocmr_data",
                "oedi-data-lake",
                "ohsu-cnl",
                "oida",
                "ome-zarr-open-scivis",
                "omi-no2-nasa",
                "ons-opendata-portal",
                "ont-open-data",
                "ooni",
                "open-bio-ref-data",
                "open-ceda",
                "open-cravat",
                "open-lidar-data",
                "open-meteo",
                "open-neurodata",
                "openaerialmap",
                "openalex",
                "openaq",
                "opencitymodel",
                "openfold",
                "openfoodfacts-images",
                "openneuro",
                "opensurfaces",
                "opentargets",
                "openuniverse2024",
                "openwings",
                "orcasound",
                "organoid-pancreatic",
                "os-climate-physrisk",
                "osm",
                "osmlr",
                "overture",
                "pacbio-human-wgs-reference",
                "pacific-sound",
                "palsar-2-scansar-flooding-in-bangladesh",
                "palsar-2-scansar-flooding-in-rwanda",
                "palsar2-scansar-turkey-syria",
                "panstarrs",
                "paracrawl",
                "pass-summaries-fewsum",
                "pasteur-logan",
                "pcd",
                "pd12m",
                "pdb-3d-structural-biology-data",
                "person-path-22",
                "pgc-arcticdem",
                "pgc-earthdem",
                "pgc-rema",
                "physionet",
                "platinum-pedigree",
                "pmsp-lidar",
                "pohang-canal-dataset",
                "pre-post-purchase-questions",
                "prod-comp-shopping",
                "proj-datum-grids",
                "proteingym",
                "pubseq",
                "pyenvs-and-callargs",
                "pyfr-mtu-t161-dns-data",
                "qiime2",
                "racecar-dataset",
                "radarsat-1",
                "radiant-mlhub",
                "rapid-nrt-flood-maps",
                "rapid7-fdns-any",
                "rareplanes",
                "rcm-ceos-ard",
                "real-changesets",
                "recount",
                "redasa-covid-data",
                "refgenie",
                "registry-open-data",
                "roadmapepigenomics",
                "rsna-abdominal-trauma-detection",
                "rsna-cervical-spine-fracture-detection",
                "rsna-intracranial-hemorrhage-detection",
                "rsna-pulmonary-embolism-detection",
                "rsna-screening-mammography-breast-cancer-detection",
                "s1-orbits",
                "safecast",
                "satellogic-earthview",
                "sau-global-fisheries-catch-data",
                "sbn-css",
                "schweizer-haltestellen-oev",
                "scottish-lidar",
                "sdgstoday-mst",
                "sdoml-fdl",
                "seefar",
                "sentinel-1",
                "sentinel-1-rtc-indigo",
                "sentinel-2",
                "sentinel-2-l2a-cogs",
                "sentinel-3",
                "sentinel-products-ca-mirror",
                "sentinel-s2-l2a-mosaic-120",
                "sentinel1-slc",
                "sentinel1-slc-seasia-pds",
                "sentinel5p",
                "serratus-lovelywater",
                "sevir",
                "sgnex",
                "shopping-humor-generation",
                "short_peptides",
                "silam",
                "silo",
                "singlecellhumanbloodatlas",
                "sipecam",
                "sissa-forecast-database-dataset",
                "slacken",
                "smithsonian-open-access",
                "smn-ar-wrf-dataset",
                "socialgene",
                "sofar-spotter-archive",
                "software-heritage",
                "sondehub-telemetry",
                "sorel-20m",
                "southern-california-earthquakes",
                "spacenet",
                "sparc",
                "spartan-cloud",
                "spatial-ucr",
                "spatiam-nlra-iss-experiments",
                "speedtest-global-performance",
                "spherex-qr",
                "spitzer-seip",
                "ssl4eo-multi-product-data",
                "st-open-data",
                "stdpopsim_kern",
                "steineggerlab",
                "stoic2021-training",
                "sucho",
                "sudachi",
                "surface-pm2-5-v6gl",
                "surftemp-sst",
                "synthea-coherent-data",
                "synthea-omop",
                "tabula-muris",
                "tabula-muris-senis",
                "tabula-sapiens",
                "talend-covid19",
                "target",
                "targetepigenomics",
                "tcga",
                "terrafusion",
                "terrain-tiles",
                "tess",
                "tglc",
                "tgs-opendata-poseidon",
                "tiger",
                "topical-chat-enriched",
                "tsbench",
                "ubc-sunflower-genome",
                "ucsc-genome-browser",
                "ucsf-bmsr",
                "ucsf-rmac",
                "uk-met-office",
                "ukbb-ld",
                "ukbppp",
                "umbra-open-data",
                "uniprot",
                "us-hiring-rates-pandemic",
                "usearch-molecules",
                "usgs-landsat",
                "usgs-lidar",
                "usgs_aqr",
                "vadcr-crmp-aws",
                "venus-l2a-cogs",
                "vf-libraries",
                "virtual_shizuoka",
                "visa",
                "visym-cap",
                "vitaldb",
                "vt-opendata",
                "wb-light-every-night",
                "wbg-cckp",
                "whiffle-wins50",
                "wikisum",
                "wis2-global-cache",
                "wise-allsky",
                "wise-allwise",
                "wise-cryo-3band",
                "wise-neowiser",
                "wise-postcryo",
                "wise-unwise",
                "wizard-of-tasks",
                "wpto-pds-us-wave",
                "wrf-alaska-snap",
                "wrf-cmip6",
                "wrf-se-alaska-snap",
                "xiph-media",
                "ycb-benchmarks",
                "yt8m",
                "zinc15",
                "ztf"
            ]
            
            logger.warning(f"Using fallback dataset list with {len(fallback_datasets)} datasets")
            self._cache_set(cache_key, fallback_datasets)
            return fallback_datasets
            
        except Exception as e:
            logger.error(f"Failed to fetch dataset list: {e}")
            # Return empty list rather than failing completely
            return []
    
    async def fetch_dataset_yaml(self, dataset_name: str) -> Dict[str, Any]:
        """Fetch and parse individual dataset YAML file."""
        cache_key = f"dataset_{dataset_name}"
        cached_result = self._cache_get(cache_key)
        if cached_result is not None:
            logger.debug(f"Returning cached dataset: {dataset_name}")
            return cached_result
        
        try:
            url = f"{self.base_url}/datasets/{dataset_name}.yaml"
            yaml_content = await self._fetch_with_retry(url)
            
            # Parse YAML content
            try:
                dataset_data = yaml.safe_load(yaml_content)
                if not isinstance(dataset_data, dict):
                    raise ValueError(f"Invalid YAML structure for dataset {dataset_name}")
                
                # Add the dataset name to the data
                dataset_data['_name'] = dataset_name
                
                self._cache_set(cache_key, dataset_data)
                logger.debug(f"Successfully fetched and cached dataset: {dataset_name}")
                return dataset_data
                
            except yaml.YAMLError as e:
                logger.error(f"Failed to parse YAML for dataset {dataset_name}: {e}")
                raise ValueError(f"Invalid YAML format for dataset {dataset_name}: {e}")
        
        except FileNotFoundError:
            logger.warning(f"Dataset not found: {dataset_name}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch dataset {dataset_name}: {e}")
            raise
    
    async def search_datasets(
        self, 
        query: str = "", 
        category: str = "", 
        tags: List[str] = None
    ) -> List[Dict[str, Any]]:
        """Search datasets based on criteria."""
        if tags is None:
            tags = []
        
        try:
            # Get all available datasets
            dataset_names = await self.fetch_dataset_list()
            
            # Fetch all dataset details
            datasets = []
            for name in dataset_names:
                try:
                    dataset_data = await self.fetch_dataset_yaml(name)
                    datasets.append(dataset_data)
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} due to error: {e}")
                    continue
            
            # Filter datasets based on search criteria
            filtered_datasets = []
            
            for dataset in datasets:
                # Check query match (case-insensitive search in name and description)
                if query:
                    query_lower = query.lower()
                    name_match = query_lower in dataset.get('Name', '').lower()
                    desc_match = query_lower in dataset.get('Description', '').lower()
                    if not (name_match or desc_match):
                        continue
                
                # Check category match
                if category:
                    dataset_category = dataset.get('Tags', {}).get('Category', '')
                    if category.lower() not in dataset_category.lower():
                        continue
                
                # Check tags match
                if tags:
                    dataset_tags = []
                    # Extract tags from various fields
                    if 'Tags' in dataset:
                        for key, value in dataset['Tags'].items():
                            if isinstance(value, list):
                                dataset_tags.extend([str(v).lower() for v in value])
                            else:
                                dataset_tags.append(str(value).lower())
                    
                    # Check if any of the requested tags match
                    tag_match = any(tag.lower() in dataset_tags for tag in tags)
                    if not tag_match:
                        continue
                
                filtered_datasets.append(dataset)
            
            logger.info(f"Search returned {len(filtered_datasets)} datasets")
            return filtered_datasets
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    async def get_categories(self) -> List[Dict[str, Any]]:
        """Get list of available ADXCategories with counts.
        
        ADXCategories are high-level business categories (0-2 per dataset).
        Many datasets may not have ADXCategories assigned.
        """
        try:
            dataset_names = await self.fetch_dataset_list()
            category_counts: Dict[str, int] = {}
            
            # ADX Category descriptions
            category_descriptions: Dict[str, str] = {
                "Financial Services Data": "Financial and banking sector datasets",
                "Retail, Location & Marketing Data": "Retail, geospatial, and marketing datasets",
                "Public Sector Data": "Government and public sector datasets",
                "Healthcare & Life Sciences Data": "Medical, health, and life sciences datasets",
                "Resources Data": "Natural resources and energy datasets",
                "Media & Entertainment Data": "Media, entertainment, and content datasets",
                "Telecommunications Data": "Telecom and network datasets",
                "Environmental Data": "Environmental and climate datasets",
                "Automotive Data": "Automotive and transportation datasets",
                "Manufacturing Data": "Manufacturing and industrial datasets",
                "Gaming Data": "Gaming and interactive entertainment datasets"
            }
            
            # Count datasets without categories
            uncategorized_count = 0
            
            # Process datasets to count ADXCategories
            for name in dataset_names:
                try:
                    dataset_data = await self.fetch_dataset_yaml(name)
                    
                    # Get ADXCategories (optional field, can have 0-2 categories)
                    adx_categories = dataset_data.get('ADXCategories', [])
                    
                    if not adx_categories:
                        uncategorized_count += 1
                    else:
                        # Handle both single value and list
                        if not isinstance(adx_categories, list):
                            adx_categories = [adx_categories]
                        
                        for category in adx_categories:
                            category = str(category).strip()
                            category_counts[category] = category_counts.get(category, 0) + 1
                    
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} for category counting: {e}")
                    uncategorized_count += 1
                    continue
            
            # Create category list with descriptions
            categories = []
            for name, count in sorted(category_counts.items()):
                categories.append({
                    'name': name,
                    'count': count,
                    'description': category_descriptions.get(name, f"Datasets in {name} category")
                })
            
            # Add uncategorized count if any
            if uncategorized_count > 0:
                categories.append({
                    'name': 'Uncategorized',
                    'count': uncategorized_count,
                    'description': 'Datasets without assigned ADXCategories'
                })
            
            logger.info(f"Found {len(category_counts)} ADXCategories across {sum(category_counts.values())} categorized datasets ({uncategorized_count} uncategorized)")
            return categories
            
        except Exception as e:
            logger.error(f"Failed to get categories: {e}")
            return [
                {'name': 'Error', 'count': 0, 'description': f'Failed to fetch categories: {str(e)}'}
            ]
    
    async def get_tags(self) -> List[Dict[str, Any]]:
        """Get list of available Tags with counts.
        
        Tags are granular classification labels (required field).
        Each dataset must have at least one tag.
        """
        try:
            dataset_names = await self.fetch_dataset_list()
            tag_counts: Dict[str, int] = {}
            
            # Process datasets to count tags
            for name in dataset_names:
                try:
                    dataset_data = await self.fetch_dataset_yaml(name)
                    
                    # Get Tags (required field)
                    tags = dataset_data.get('Tags', [])
                    
                    if not tags:
                        logger.warning(f"Dataset {name} has no tags (invalid)")
                        continue
                    
                    # Handle both single value and list
                    if not isinstance(tags, list):
                        tags = [tags]
                    
                    for tag in tags:
                        tag = str(tag).strip().lower()
                        tag_counts[tag] = tag_counts.get(tag, 0) + 1
                    
                except Exception as e:
                    logger.warning(f"Skipping dataset {name} for tag counting: {e}")
                    continue
            
            # Create tag list sorted by count (descending)
            tags = []
            for name, count in sorted(tag_counts.items(), key=lambda x: (-x[1], x[0])):
                tags.append({
                    'name': name,
                    'count': count
                })
            
            logger.info(f"Found {len(tags)} unique tags across {len(dataset_names)} datasets")
            return tags
            
        except Exception as e:
            logger.error(f"Failed to get tags: {e}")
            return [
                {'name': 'error', 'count': 0, 'description': f'Failed to fetch tags: {str(e)}'}
            ]
