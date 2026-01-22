#!/usr/bin/env python3

# Author: Alexandre Heideker - FEI/UFABC - Brazil
# Date: 2025-november
# FAPESP - SMART Project - Download Sentinel-2 Data

# Search and download Sentinel-2 data from Copernicus Data Space Ecosystem (CDSE)
# using the OData API.
# Obs.: GeoJSON must be in WGS84 (EPSG:4326) format.

# Requirements:
#     pip install requests pandas shapely

# Authentication:
#     You can either:
#       - pass --user and --password on the command line, OR
#       - set environment variables:
#             CDSE_USERNAME=your_login
#             CDSE_PASSWORD=your_password

# Example:
#     python download_s2.py \
#         --aoi teste.geojson \
#         --start 2025-10-01 \
#         --end 2025-10-31 \
#         --cloud 20 \
#         --level L2A \
#         --maxitems 4 \
#         --output ./teste

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
from shapely.geometry import shape as shapely_shape
from shapely.ops import unary_union

CATALOGUE_ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
AUTH_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
    "protocol/openid-connect/token"
)


# Authentication
def get_token(username: Optional[str], password: Optional[str]) -> str:
    """
    Obtain an access token from Copernicus Data Space Ecosystem.

    Username/password can come from CLI args or environment variables:
        CDSE_USERNAME, CDSE_PASSWORD
    """
    if not username:
        username = os.getenv("CDSE_USERNAME")
    if not password:
        password = os.getenv("CDSE_PASSWORD")

    if not username or not password:
        raise RuntimeError(
            "Missing CDSE credentials. Provide --user/--password or "
            "CDSE_USERNAME/CDSE_PASSWORD environment variables."
        )

    data = {
        "client_id": "cdse-public",
        "grant_type": "password",
        "username": username,
        "password": password,
    }

    resp = requests.post(AUTH_URL, data=data)
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print("❌ Token request failed")
        print("Status:", resp.status_code)
        print("Body:", resp.text)
        raise

    token = resp.json()["access_token"]
    return token


# AOI handling

from shapely.geometry import shape as shapely_shape
from shapely.ops import unary_union

def load_aoi_wkt(aoi_path: str) -> str:
    # Load AOI from a GeoJSON file and return a SINGLE WKT polygon (bounding box).

    # Steps:
    #   1) Read all geometries from the GeoJSON (FeatureCollection, Feature, or Geometry).
    #   2) Compute the union of all geometries.
    #   3) Take the envelope (bounding box) of the union.
    #   4) Return the WKT of that simple rectangle.

    # This keeps the OData filter string short and avoids HTTP 414 (URI too large).

    import json

    with open(aoi_path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    geoms = []

    if gj.get("type") == "FeatureCollection":
        for feat in gj["features"]:
            geoms.append(shapely_shape(feat["geometry"]))
    elif gj.get("type") == "Feature":
        geoms.append(shapely_shape(gj["geometry"]))
    else:
        geoms.append(shapely_shape(gj))

    # Union of all polygons to shrink query URI
    union_geom = unary_union(geoms)

    # Bounding box (envelope) of the union → simple rectangle
    bbox_geom = union_geom.envelope  # still a Polygon

    wkt = bbox_geom.wkt
    # Small normalization (removing extra spaces after geometry type)
    wkt = wkt.replace("POLYGON ", "POLYGON").replace("MULTIPOLYGON ", "MULTIPOLYGON")
    return wkt


# ---------------------------------------------------------------------------
# Data search
# ---------------------------------------------------------------------------


def normalize_level(level: str) -> str:
    """
    Map level string to Sentinel-2 productType:
        L1C  -> S2MSI1C
        L2A  -> S2MSI2A
    Accepts variants like 'Level-2A', 'level2a', etc.
    """
    s = level.strip().upper()
    s = s.replace("LEVEL-", "").replace("LEVEL", "").replace(" ", "")
    if s in ("L1C", "1C"):
        return "S2MSI1C"
    if s in ("L2A", "2A"):
        return "S2MSI2A"
    raise ValueError(f"Unsupported level: {level!r}. Use L1C or L2A.")


# def extract_cloud_cover(product: Dict[str, Any]) -> Optional[float]:
#     for attr in product.get("Attributes", []):
#         if attr.get("@odata.type", "").endswith("DoubleAttribute") and attr.get("Name") == "cloudCover":
#             return attr.get("Value")
#     return None

def extract_cloud_cover(product: Dict[str, Any]) -> Optional[float]:
    """
    Extract cloud cover (%) from product Attributes list.
    Returns None if not present.

    CDSE typically stores this as a DoubleAttribute with Name 'cloudCover',
    but we also check for some common variants just in case.
    """
    attrs = product.get("Attributes") or []
    for attr in attrs:
        name = attr.get("Name")
        if name not in ("cloudCover", "cloudcoverpercentage", "cloudCoverPercentage"):
            continue

        # DoubleAttribute style: Value is a float
        if attr.get("@odata.type", "").endswith("DoubleAttribute"):
            val = attr.get("Value")
            try:
                return float(val)
            except (TypeError, ValueError):
                continue

    return None

def search_products(
    aoi_wkt: str,
    start_date: str,
    end_date: str,
    max_cloud: float,
    product_type: str,
    max_items: int,
) -> List[Dict[str, Any]]:
    # Search Sentinel-2 products that match AOI, time range, cloud cover and product type.

    # Dates must be strings 'YYYY-MM-DD'; they will be converted to ISO with Z.

    start_iso = f"{start_date}T00:00:00.000Z"
    end_iso = f"{end_date}T23:59:59.999Z"

    odata_filter = (
        "Collection/Name eq 'SENTINEL-2' "
        "and Attributes/OData.CSC.StringAttribute/any("
        "att:att/Name eq 'productType' and "
        f"att/OData.CSC.StringAttribute/Value eq '{product_type}'"
        ") "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt}') "
        f"and ContentDate/Start ge {start_iso} "
        f"and ContentDate/Start le {end_iso} "
        "and Attributes/OData.CSC.DoubleAttribute/any("
        "att:att/Name eq 'cloudCover' and "
        f"att/OData.CSC.DoubleAttribute/Value le {float(max_cloud):.2f}"
        ")"
    )

    products: List[Dict[str, Any]] = []
    page_size = min(max_items, 100) if max_items > 0 else 100
    skip = 0

    while True:
        # params = {
        #     "$filter": odata_filter,
        #     "$top": page_size,
        #     "$skip": skip,
        #     # Ask OData to include Attributes in the response so we can read cloudCover
        #     "$expand": "Attributes",
        #     # Optional: reduce payload size but keep everything we need
        #     "$select": (
        #         "Id,Name,ContentDate,Collection,Online,ContentLength,"
        #         "S3Path,Attributes"
        #     ),
        # }

        params = {
            "$filter": odata_filter,
            "$top": page_size,
            "$skip": skip,
            # We only expand Attributes to get cloudCover; no $select, to avoid 400 Bad Request
            "$expand": "Attributes",
        }


        resp = requests.get(f"{CATALOGUE_ODATA_URL}/Products", params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("value", [])
        print(f"DEBUG: page skip={skip}, got {len(batch)} products")
        if not batch:
            break

        products.extend(batch)
        if 0 < max_items <= len(products):
            products = products[:max_items]
            break

        if len(batch) < page_size:
            break
        skip += page_size

    return products


def products_to_dataframe(products: List[Dict[str, Any]]) -> pd.DataFrame:
    # Convert list of product dicts to a pandas DataFrame with a few useful columns.

    rows: List[Dict[str, Any]] = []
    for p in products:
        content_date = p.get("ContentDate") or {}
        collection_obj = p.get("Collection") or {}
        rows.append(
            {
                "id": p.get("Id"),
                "name": p.get("Name"),
                "start": content_date.get("Start"),
                "end": content_date.get("End"),
                "collection": (
                    collection_obj.get("Name") if isinstance(collection_obj, dict) else None
                ),
                "cloud": extract_cloud_cover(p),
                "online": p.get("Online"),
                "size_bytes": p.get("ContentLength"),
                "s3_path": p.get("S3Path"),
            }
        )
    df = pd.DataFrame(rows)
    return df


def save_metadata(df: pd.DataFrame, output_dir: str) -> Path:
    # Save products metadata to results.csv in the given output directory.
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    csv_path = outdir / "results.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


# Download
def download_product(
    product_id: str,
    access_token: str,
    output_dir: str,
    max_attempts: int = 3,
) -> bool:
    # Download a single product using OData $value endpoint
    # (full Sentinel-2 SAFE product in ZIP).

    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    path_suffix = f"Products({product_id})/$value"
    url = f"https://download.dataspace.copernicus.eu/odata/v1/{path_suffix}"

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})

    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, stream=True)
            if resp.status_code == 401:
                print(f"  HTTP 401 Unauthorized on attempt {attempt} for {product_id}")
                print("  Response text:", resp.text)
                return False
            if resp.status_code != 200:
                print(
                    f"  Attempt {attempt}/{max_attempts} for {product_id} "
                    f"failed: {resp.status_code}"
                )
                print("  Response text:", resp.text)
                continue

            filename = f"{product_id}.zip"
            outpath = outdir / filename
            with open(outpath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"  ✅ Downloaded: {product_id} -> {outpath}")
            return True

        except requests.RequestException as e:
            print(f"  Attempt {attempt}/{max_attempts} for {product_id} failed: {e}")

    print(f"  ⚠️ Failed to download: {product_id}")
    return False


# CLI parameters

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search and download Sentinel-2 products from CDSE via OData API."
    )
    parser.add_argument(
        "--aoi",
        required=True,
        help="Path to GeoJSON file with AOI (can contain multiple polygons).",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD, sensing date).",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD, sensing date).",
    )
    parser.add_argument(
        "--cloud",
        type=float,
        default=100.0,
        help="Maximum cloud cover percentage (e.g. 20 for 20%%).",
    )
    parser.add_argument(
        "--level",
        default="L2A",
        help="Processing level: L1C or L2A. (default: L2A)",
    )
    parser.add_argument(
        "--maxitems",
        type=int,
        default=10,
        help="Maximum number of products to process.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for metadata and downloads.",
    )
    parser.add_argument(
        "--list_only",
        action="store_true",
        help="Only list products and save results.csv, do not download.",
    )
    parser.add_argument(
        "--user",
        default=None,
        help="CDSE username (optional if CDSE_USERNAME env var is set).",
    )
    parser.add_argument(
        "--password",
        default=None,
        help="CDSE password (optional if CDSE_PASSWORD env var is set).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # 1) AOI (union of all polygons)
    print(f"📂 Reading AOI from: {args.aoi}")
    aoi_wkt = load_aoi_wkt(args.aoi)

    # 2) Level / product type
    product_type = normalize_level(args.level)
    print(f"🛰️  Searching Sentinel-2 products, productType={product_type}")

    # 3) Search
    products = search_products(
        aoi_wkt=aoi_wkt,
        start_date=args.start,
        end_date=args.end,
        max_cloud=args.cloud,
        product_type=product_type,
        max_items=args.maxitems,
    )

    if not products:
        print("No products found for given filters.")
        return

    df = products_to_dataframe(products)
    csv_path = save_metadata(df, args.output)
    print(f"{len(products)} products found. Saved metadata to {csv_path}")

    # Show preview
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print("\nPreview of found products:")
        print(df.head(min(5, len(df))))

    if args.list_only:
        print("\n--list_only specified, skipping download.")
        return

    # 4) Auth
    print("\n🔐 Getting access token...")
    token = get_token(args.user, args.password)
    print("Access token obtained.")

    # 5) Download
    print(f"\n⬇️ Downloading up to {args.maxitems} products into: {args.output}")
    downloaded = 0

    for pid in df["id"].tolist()[: args.maxitems]:
        ok = download_product(
            product_id=pid,
            access_token=token,
            output_dir=args.output,
            max_attempts=3,
        )

        if not ok:
            print("  Refreshing access token and retrying once...")
            token = get_token(args.user, args.password)
            ok = download_product(
                product_id=pid,
                access_token=token,
                output_dir=args.output,
                max_attempts=3,
            )

        if ok:
            downloaded += 1

    print(f"\n✅ Finished. {downloaded} products downloaded successfully.")


if __name__ == "__main__":
    main()
