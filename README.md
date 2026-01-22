# SAT-ML

## Overview

**SAT-ML** is a Python-based software tool designed to automate the
retrieval, preprocessing, and preparation of satellite imagery for
machine learning workflows. This software supports the SMART project's
goal of integrating remote sensing and in situ sensor data (e.g., soil
moisture) to derive vegetation cover indices and other geospatial
products.

## Features

-   Automated download of satellite scenes from open repositories.
-   Clipping of multispectral imagery to a defined Area of Interest
    (AOI).
-   Organization of spectral bands into analysis-ready datasets.
-   Preparation of satellite imagery for machine learning pipelines.
-   Modular and extensible Python architecture.

## Prerequisites

-   Python 3.8 or higher
-   pip
-   Virtual environment (recommended)

Install dependencies:

pip install --upgrade pip pip install -r requirements.txt

If `requirements.txt` is not present, install: rasterio, geopandas,
shapely, requests, numpy

## Installation

git clone https://github.com/heideker/SAT-ML.git cd SAT-ML

python -m venv venv source venv/bin/activate

## Usage

### 1. Define the Area of Interest (AOI)

Create a GeoJSON file containing the polygon of your study area.

### 2. Download Satellite Data

python download_s2.py --aoi setores_wgs84.geojson --output-dir data/raw

### 3. Clip Imagery

python clip_s2_to_aoi.py --input-dir data/raw --aoi setores.geojson
--output-dir data/processed

## License

MIT
