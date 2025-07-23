#!/usr/bin/env python3
"""
Core scraping functions extracted from the original script
"""

import os
import csv
import json
import time
import uuid
import re
import h3
from functools import lru_cache
from urllib.parse import urlparse, urljoin, urlunparse
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Tuple, Optional, Any
import tldextract
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from collections import deque
from multiprocessing import Manager, Lock, Semaphore
import math
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
import threading
import queue

# Constants from original script
EMAIL_REGEX = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', re.IGNORECASE | re.DOTALL)
INVALID_TLDS = {
    'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp', 'ico', 'tiff',
    'css', 'js', 'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'zip'
}
SOCIAL_REGEX = {
    'facebook': re.compile(r'https?://(?:www\.)?(?:facebook\.com|fb\.com|fb\.me)/[^\s\'"<>]+', re.IGNORECASE),
    'instagram': re.compile(r'https?://(?:www\.)?(?:instagram\.com|instagr\.am)/[^\s\'"<>]+', re.IGNORECASE),
    'linkedin': re.compile(r'https?://(?:www\.)?(?:linkedin\.com|lnkd\.in)/[^\s\'"<>]+', re.IGNORECASE),
    'twitter_x': re.compile(r'https?://(?:www\.)?(?:twitter\.com|x\.com|t\.co)/[^\s\'"<>]+', re.IGNORECASE)
}

DETAIL_FIELDS = (
    "place_id,name,business_status,formatted_address,website,formatted_phone_number,"
    "address_components,adr_address,geometry,icon,icon_mask_base_uri,icon_background_color,"
    "permanently_closed,photos,plus_code,types,url,utc_offset,vicinity,wheelchair_accessible_entrance,"
    "current_opening_hours,international_phone_number,opening_hours,secondary_opening_hours,"
    "curbside_pickup,delivery,dine_in,editorial_summary,price_level,rating,reservable,reviews,"
    "serves_beer,serves_breakfast,serves_brunch,serves_dinner,serves_lunch,serves_vegetarian_food,serves_wine,takeout,user_ratings_total"
)

COLUMNS = [
    "place_id", "name", "business_status", "formatted_address", "website", "formatted_phone_number",
    "address_components", "adr_address", "geometry", "icon", "icon_mask_base_uri", "icon_background_color",
    "permanently_closed", "photos", "plus_code", "types", "url", "utc_offset", "vicinity", "wheelchair_accessible_entrance",
    "current_opening_hours", "international_phone_number", "opening_hours", "secondary_opening_hours",
    "curbside_pickup", "delivery", "dine_in", "editorial_summary", "price_level", "rating",
    "reservable", "reviews", "serves_beer", "serves_breakfast", "serves_brunch", "serves_dinner",
    "serves_lunch", "serves_vegetarian_food", "serves_wine", "takeout", "user_ratings_total",
    "emails", "facebook", "twitter_x", "instagram", "linkedin", "h3_index", "search_lat", "search_lng", "city"
]

H3_RESOLUTIONS = {
    5:  {"name": "Vast (Res 5)", "avg_edge_length_km": 9.854090990, "search_radius": 9855},
    6:  {"name": "Large (Res 6)", "avg_edge_length_km": 3.724532667, "search_radius": 3725},
    7:  {"name": "Medium (Res 7)", "avg_edge_length_km": 1.406475763, "search_radius": 1407},
    8:  {"name": "Small (Res 8)", "avg_edge_length_km": 0.531414010, "search_radius": 532},
    9:  {"name": "Very Small (Res 9)", "avg_edge_length_km": 0.200786148, "search_radius": 201},
    10: {"name": "Tiny (Res 10)", "avg_edge_length_km": 0.075863783, "search_radius": 76},
    11: {"name": "Micro (Res 11)", "avg_edge_length_km": 0.028663897, "search_radius": 29},
}

DEFAULT_H3_RESOLUTION = 7
MAX_H3_RESOLUTION = 11

# Web scraping functions
@lru_cache(maxsize=2048)
def is_hex_like_local_part(email: str, min_length: int = 16) -> bool:
    local = email.split('@')[0]
    return len(local) >= min_length and all(c in "0123456789abcdefABCDEF" for c in local)

def extract_emails_from_text(text: str) -> Set[str]:
    if not text: return set()
    found = set(EMAIL_REGEX.findall(text))
    quoted_emails = re.findall(r'[\'"]([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})[\'"]', text, re.IGNORECASE)
    found.update(quoted_emails)
    valid_emails = {email for email in found if email.split('@')[-1].split('.')[-1].lower() not in INVALID_TLDS and not is_hex_like_local_part(email)}
    return valid_emails

def extract_social_links_from_text(text: str) -> Dict[str, Set[str]]:
    if not text: return {key: set() for key in SOCIAL_REGEX}
    return {key: set(pattern.findall(text)) for key, pattern in SOCIAL_REGEX.items()}

@lru_cache(maxsize=256)
def convert_subdomain_to_www(url: str) -> str:
    if not url or not isinstance(url, str): return ""
    if not url.startswith(('http://', 'https://')): url = 'http://' + url
    parsed = urlparse(url)
    ext = tldextract.extract(url)
    if ext.subdomain and ext.subdomain != "www":
        new_netloc = f"www.{ext.domain}.{ext.suffix}"
        return urlunparse((parsed.scheme, new_netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    return url

def fetch_page_advanced(url: str, timeout: int = 15) -> Tuple[Optional[str], Optional[str]]:
    headers_list = [
        {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'},
        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'},
        {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
    ]
    for headers in headers_list:
        try:
            response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            response.raise_for_status()
            return response.text, None
        except requests.exceptions.RequestException:
            continue
    return None, f"Failed to fetch {url} with all user agents."

def get_all_internal_links(root_url: str, html_content: str) -> Set[str]:
    internal_links = set()
    parsed_root = urlparse(root_url)
    base_domain = parsed_root.netloc
    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in soup.find_all('a', href=True):
        href = tag['href'].strip()
        if href.lower().startswith(('mailto:', 'javascript:', 'tel:')): continue
        full_url = urljoin(root_url, href)
        parsed_link = urlparse(full_url)
        if parsed_link.scheme in ['http', 'https'] and parsed_link.netloc == base_domain:
            internal_links.add(full_url)
    return internal_links

def process_single_url(url: str) -> Tuple[str, Set[str], Dict[str, Set[str]], Optional[str]]:
    content, error = fetch_page_advanced(url)
    if error: return url, set(), {key: set() for key in SOCIAL_REGEX}, error
    
    emails = extract_emails_from_text(content)
    soup = BeautifulSoup(content, 'html.parser')
    for tag in soup.find_all('a', href=True):
        if tag['href'].lower().startswith("mailto:"): emails.add(tag['href'][7:].split('?')[0])
    
    social_links = extract_social_links_from_text(content)
    return url, emails, social_links, None

def scrape_website_data(url: str, max_links: int = 25) -> Tuple[List[str], Dict[str, List[str]]]:
    try:
        url = convert_subdomain_to_www(url)
        parsed = urlparse(url)
        root_url = f"{parsed.scheme}://{parsed.netloc}/"
        
        urls_to_process = {url, root_url}
        root_content, root_error = fetch_page_advanced(root_url)
        if not root_error and root_content:
            all_links = get_all_internal_links(root_url, root_content)
            contact_links = {u for u in all_links if 'contact' in u.lower()}
            other_links = all_links - contact_links
            urls_to_process.update(list(contact_links) + list(other_links))
        
        unique_urls = list(urls_to_process)[:max_links]
        emails_found = set()
        social_links_found = {key: set() for key in SOCIAL_REGEX}
        
        with ThreadPoolExecutor(max_workers=min(10, len(unique_urls) or 1)) as executor:
            results = executor.map(process_single_url, unique_urls)
        
        for _, emails, social_links, error in results:
            if not error:
                emails_found.update(emails)
                for platform, links in social_links.items():
                    social_links_found[platform].update(links)
        
        return list(emails_found), {p: list(l) for p, l in social_links_found.items()}
    except Exception:
        return [], {key: [] for key in SOCIAL_REGEX}

# Google Places API functions
class APIRateLimiter:
    def __init__(self, max_calls_per_second=10):
        self.max_calls_per_second = max_calls_per_second
        self.calls = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # Remove calls older than 1 second
            self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            if len(self.calls) >= self.max_calls_per_second:
                sleep_time = 1.0 - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls if now - call_time < 1.0]
            
            self.calls.append(now)

def get_nearby_search_places_page(api_key: str, keyword: str, business_type: str, latitude: float, longitude: float, 
                               radius: int, rate_limiter: APIRateLimiter, pagetoken: Optional[str] = None) -> Tuple[List[Dict], Optional[str], str]:
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"key": api_key, "location": f"{latitude},{longitude}", "radius": radius}
    
    # Add keyword only if it's not empty
    if keyword and keyword.strip(): 
        params["keyword"] = keyword
    
    # Add business type only if it's not empty
    if business_type and business_type.strip(): 
        params["type"] = business_type
    
    if pagetoken:
        params["pagetoken"] = pagetoken
        time.sleep(2)
    
    rate_limiter.wait_if_needed()
    
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        status = data.get("status", "UNKNOWN_ERROR")
        if status == "OK":
            return data.get("results", []), data.get("next_page_token"), status
        elif status == "OVER_QUERY_LIMIT":
            print("    🚦 Rate limit hit. Pausing for a moment...")
            time.sleep(5)
            return [], None, status
        return [], None, status
    except requests.exceptions.RequestException:
        return [], None, "REQUEST_EXCEPTION"

def get_place_details(api_key: str, place_id: str, detail_fields: str, rate_limiter: APIRateLimiter) -> Optional[Dict]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {"key": api_key, "place_id": place_id, "fields": detail_fields}
    
    rate_limiter.wait_if_needed()
    
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        return data.get("result") if data.get("status") == "OK" else None
    except requests.exceptions.RequestException:
        return None

def flatten_value(value: Any) -> str:
    if isinstance(value, (dict, list)): return json.dumps(value, ensure_ascii=False)
    return str(value) if value is not None else ""

# H3 functions
def get_search_radius_for_resolution(resolution):
    return H3_RESOLUTIONS.get(resolution, {}).get("search_radius", 1000)

def generate_hexagons_from_geojson_enhanced(geometry_dict: dict, h3_resolution: int) -> List[str]:
    geom_type = geometry_dict.get("type")
    coordinates = geometry_dict.get("coordinates")

    if not geom_type or not coordinates:
        print("    Warning: Invalid geometry - missing type or coordinates")
        return []

    shapely_geom = None
    try:
        if geom_type == 'Polygon':
            shapely_geom = Polygon(coordinates[0], holes=coordinates[1:] if len(coordinates) > 1 else None)
        elif geom_type == 'MultiPolygon':
            polygons = []
            for poly_coords in coordinates:
                poly = Polygon(poly_coords[0], holes=poly_coords[1:] if len(poly_coords) > 1 else None)
                polygons.append(poly)
            shapely_geom = MultiPolygon(polygons)
        else:
            print(f"    Warning: Unsupported geometry type: {geom_type}")
            return []

        if not shapely_geom.is_valid:
            print("    Warning: Invalid polygon geometry, attempting to fix...")
            shapely_geom = shapely_geom.buffer(0)

    except Exception as e:
        print(f"    Warning: Failed to create Shapely geometry: {e}")
        return []

    minx, miny, maxx, maxy = shapely_geom.bounds
    edge_length_km = H3_RESOLUTIONS.get(h3_resolution, {}).get("avg_edge_length_km", 1.0)
    edge_length_degrees = edge_length_km / 111.32
    buffer = edge_length_degrees * 2
    minx -= buffer
    miny -= buffer
    maxx += buffer
    maxy += buffer

    step = edge_length_degrees * 0.5
    seed_points = []
    lat = miny
    while lat <= maxy:
        lng = minx
        while lng <= maxx:
            seed_points.append((lat, lng))
            lng += step
        lat += step

    all_hexagons = set()
    for lat, lng in seed_points:
        try:
            hex_id = h3.geo_to_h3(lat, lng, h3_resolution)
            if hex_id:
                all_hexagons.add(hex_id)
                neighbors = h3.hex_ring(hex_id, 1)
                all_hexagons.update(neighbors)
        except Exception:
            continue

    intersecting_hexagons = []
    for hex_id in all_hexagons:
        try:
            hex_boundary = h3.h3_to_geo_boundary(hex_id, geo_json=True)
            hex_polygon = Polygon(hex_boundary)
            if shapely_geom.intersects(hex_polygon):
                intersecting_hexagons.append(hex_id)
        except Exception:
            continue
            
    return intersecting_hexagons

def generate_hexagons_honeycomb_pattern(geometry_dict: dict, h3_resolution: int) -> List[str]:
    geom_type = geometry_dict.get("type")
    coordinates = geometry_dict.get("coordinates")

    if not geom_type or not coordinates:
        return []

    try:
        if geom_type == 'Polygon':
            shapely_geom = Polygon(coordinates[0], holes=coordinates[1:] if len(coordinates) > 1 else None)
        elif geom_type == 'MultiPolygon':
            polygons = [Polygon(poly_coords[0], holes=poly_coords[1:] if len(poly_coords) > 1 else None)
                        for poly_coords in coordinates]
            shapely_geom = MultiPolygon(polygons)
        else:
            return []

        if not shapely_geom.is_valid:
            shapely_geom = shapely_geom.buffer(0)

    except Exception as e:
        print(f"    Error creating geometry: {e}")
        return []

    minx, miny, maxx, maxy = shapely_geom.bounds
    edge_length_km = H3_RESOLUTIONS.get(h3_resolution, {}).get("avg_edge_length_km", 1.0)
    edge_length_degrees = edge_length_km / 111.32
    hex_height = edge_length_degrees * 2
    hex_width = edge_length_degrees * math.sqrt(3)
    vertical_spacing = hex_height * 0.75
    buffer = max(hex_width, hex_height)
    minx -= buffer
    miny -= buffer
    maxx += buffer
    maxy += buffer

    hexagon_centers = []
    row = 0
    lat = miny
    while lat <= maxy:
        lng_offset = (hex_width / 2) if row % 2 == 1 else 0
        lng = minx + lng_offset
        while lng <= maxx:
            hexagon_centers.append((lat, lng))
            lng += hex_width
        lat += vertical_spacing
        row += 1

    intersecting_hexagons = []
    for lat, lng in hexagon_centers:
        try:
            hex_id = h3.geo_to_h3(lat, lng, h3_resolution)
            if not hex_id:
                continue
            hex_boundary = h3.h3_to_geo_boundary(hex_id, geo_json=True)
            hex_polygon = Polygon(hex_boundary)
            if shapely_geom.intersects(hex_polygon):
                intersecting_hexagons.append(hex_id)
        except Exception:
            continue
    return intersecting_hexagons

def validate_hexagon_coverage(geometry_dict: dict, hex_indices: List[str]) -> Dict[str, float]:
    if not hex_indices:
        return {"coverage_ratio": 0.0, "overlap_ratio": 0.0, "hex_count": 0}

    try:
        if geometry_dict.get("type") == 'Polygon':
            original_poly = Polygon(geometry_dict["coordinates"][0])
        elif geometry_dict.get("type") == 'MultiPolygon':
            polys = [Polygon(coords[0]) for coords in geometry_dict["coordinates"]]
            original_poly = MultiPolygon(polys)
        else:
            return {"coverage_ratio": 0.0, "overlap_ratio": 0.0, "hex_count": len(hex_indices)}

        if not original_poly.is_valid:
            original_poly = original_poly.buffer(0)

        hex_polygons = [Polygon(h3.h3_to_geo_boundary(h, geo_json=True)) for h in hex_indices]
        hex_union = unary_union(hex_polygons)

        original_area = original_poly.area
        covered_area = original_poly.intersection(hex_union).area
        total_hex_area = hex_union.area

        coverage_ratio = covered_area / original_area if original_area > 0 else 0
        overlap_ratio = total_hex_area / original_area if original_area > 0 else 0

        return {
            "coverage_ratio": coverage_ratio,
            "overlap_ratio": overlap_ratio,
            "hex_count": len(hex_indices)
        }
    except Exception as e:
        print(f"    Error validating coverage: {e}")
        return {"coverage_ratio": 0.0, "overlap_ratio": 0.0, "hex_count": len(hex_indices)}

def generate_hexagons_from_geojson(geometry_dict: dict, h3_resolution: int) -> List[str]:
    hexagons_enhanced = generate_hexagons_from_geojson_enhanced(geometry_dict, h3_resolution)
    coverage_stats = validate_hexagon_coverage(geometry_dict, hexagons_enhanced)

    if coverage_stats['coverage_ratio'] < 0.95 and len(hexagons_enhanced) > 0:
        hexagons_honeycomb = generate_hexagons_honeycomb_pattern(geometry_dict, h3_resolution)
        coverage_stats_honeycomb = validate_hexagon_coverage(geometry_dict, hexagons_honeycomb)
        
        if coverage_stats_honeycomb['coverage_ratio'] > coverage_stats['coverage_ratio']:
            return hexagons_honeycomb
            
    return hexagons_enhanced

# Core processing functions
def process_place(api_key, place_id, h3_index, lat, lng, boundary_name, max_links, rate_limiter):
    details = get_place_details(api_key, place_id, DETAIL_FIELDS, rate_limiter)
    if not details: return None, []
    
    details.update({"h3_index": h3_index, "search_lat": lat, "search_lng": lng, "city": boundary_name})
    
    website = details.get("website")
    emails, social_links = [], {key: [] for key in SOCIAL_REGEX}
    
    if website:
        emails, social_links = scrape_website_data(website, max_links=max_links)
    
    details.update({"emails": emails, **social_links})
    return details, emails

def get_all_place_ids_for_hex(api_key, search_query, business_type, lat, lng, radius, rate_limiter):
    all_results, pagetoken = [], None
    api_calls = 0
    
    for _ in range(3):
        api_calls += 1
        page_results, pagetoken, status = get_nearby_search_places_page(
            api_key, search_query, business_type, lat, lng, radius, rate_limiter, pagetoken
        )
        if status not in ["OK", "ZERO_RESULTS"]:
            print(f"    API Warning for hex at ({lat:.4f}, {lng:.4f}): {status}")
        if page_results: all_results.extend(page_results)
        if not pagetoken: break
    
    return all_results, api_calls

def load_boundaries_from_geojson_data(geojson_data):
    """Load boundaries from GeoJSON data"""
    boundaries = []
    for feature in geojson_data.get('features', []):
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        if props and geom:
            # Try different property names for the boundary name
            name = (props.get('DISTRICT') or 
                   props.get('name') or 
                   props.get('NAME') or 
                   props.get('district') or
                   props.get('region') or
                   props.get('area') or
                   'Unnamed Boundary')
            boundaries.append({'name': name, 'geometry': geom})
    return boundaries

# Scraping Manager Class
class ScrapingManager:
    def __init__(self, api_key, boundaries, keywords, business_type, h3_resolution, 
                 target_results, max_concurrency, max_links, csv_with_emails, csv_without_emails):
        self.api_key = api_key
        self.boundaries = boundaries
        self.keywords = keywords
        self.business_type = business_type
        self.h3_resolution = h3_resolution
        self.target_results = target_results
        self.max_concurrency = max_concurrency
        self.max_links = max_links
        self.csv_with_emails = csv_with_emails
        self.csv_without_emails = csv_without_emails
        self.rate_limiter = APIRateLimiter(max_concurrency)
        self.stop_flag = threading.Event()
        self.existing_place_ids = set()
        self.stats = {
            'businesses_with_email': 0,
            'businesses_without_email': 0,
            'api_calls': 0,
            'places_processed': 0
        }
        self.stats_lock = threading.Lock()
        self.file_lock = threading.Lock()
    
    def stop_scraping(self):
        self.stop_flag.set()
    
    def get_stats(self):
        with self.stats_lock:
            return self.stats.copy()
    
    def update_stats(self, **kwargs):
        with self.stats_lock:
            for key, value in kwargs.items():
                if key in self.stats:
                    self.stats[key] += value
    
    def write_to_csv(self, row_data, has_emails):
        """Write a row to the appropriate CSV file"""
        csv_file = self.csv_with_emails if has_emails else self.csv_without_emails
        
        with self.file_lock:
            try:
                with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=COLUMNS)
                    writer.writerow(row_data)
                
                # Update stats
                if has_emails:
                    self.update_stats(businesses_with_email=1)
                else:
                    self.update_stats(businesses_without_email=1)
                self.update_stats(places_processed=1)
                
            except Exception as e:
                print(f"Error writing to CSV: {e}")
    
    def run_scraping(self):
        """Main scraping loop"""
        try:
            print(f"Starting scraper with {len(self.boundaries)} boundaries and {len(self.keywords)} keywords...")
            
            for boundary in self.boundaries:
                if self.stop_flag.is_set() or self.stats['businesses_with_email'] >= self.target_results:
                    break
                
                self.process_boundary(boundary)
            
            print("Scraping completed!")
            
        except Exception as e:
            print(f"Scraping error: {e}")
    
    def process_boundary(self, boundary):
        """Process a single boundary"""
        boundary_name = boundary['name']
        print(f"Starting boundary '{boundary_name}'")
        
        for search_query in self.keywords:
            if self.stop_flag.is_set() or self.stats['businesses_with_email'] >= self.target_results:
                break
            
            print(f"  Processing keyword: '{search_query}'")
            self.process_keyword_in_boundary(boundary, search_query)
    
    def process_keyword_in_boundary(self, boundary, search_query):
        """Process a keyword within a boundary"""
        processing_queue = deque()
        
        # Generate hexagons
        print(f"    Generating hexagons for boundary at resolution {self.h3_resolution}...")
        initial_hexagons = generate_hexagons_from_geojson(boundary['geometry'], self.h3_resolution)
        print(f"    Generated {len(initial_hexagons)} hexagons.")
        
        if not initial_hexagons:
            print(f"    No hexagons generated for boundary '{boundary['name']}'. Skipping.")
            return
        
        for h3_index in initial_hexagons:
            processing_queue.append((h3_index, self.h3_resolution))
        
        processed_hex_count = 0
        while processing_queue and not self.stop_flag.is_set():
            if self.stats['businesses_with_email'] >= self.target_results:
                break
            
            h3_index, current_res = processing_queue.popleft()
            processed_hex_count += 1
            lat, lng = h3.h3_to_geo(h3_index)
            radius = get_search_radius_for_resolution(current_res)
            
            # Display appropriate search info
            search_info = f"type: {self.business_type}" if self.business_type and not search_query.strip() else f"keyword: {search_query}"
            print(f"    [{boundary['name']}] Q:{len(processing_queue)} Hex {processed_hex_count}: {h3_index} (Res {current_res}) - {search_info}")
            
            results, api_calls = get_all_place_ids_for_hex(
                self.api_key, search_query, self.business_type, lat, lng, radius, self.rate_limiter
            )
            
            # Update API call stats
            self.update_stats(api_calls=api_calls)
            
            # Check if we need to subdivide
            if len(results) >= 58 and current_res < MAX_H3_RESOLUTION:
                print(f"      -> Dense area found. Subdividing hex {h3_index} from res {current_res} to {current_res + 1}.")
                child_hexes = h3.h3_to_children(h3_index, current_res + 1)
                for child_hex in child_hexes:
                    processing_queue.append((child_hex, current_res + 1))
                continue
            
            # Filter unique places
            unique_places = [p for p in results if p.get("place_id") and p["place_id"] not in self.existing_place_ids]
            if not unique_places: 
                continue
            
            print(f"      -> Found {len(results)} results, {len(unique_places)} new places to process.")
            
            # Add to existing place IDs
            for place in unique_places:
                self.existing_place_ids.add(place["place_id"])
            
            # Process places
            self.process_places(unique_places, h3_index, lat, lng, boundary['name'])
    
    def process_places(self, places, h3_index, lat, lng, boundary_name):
        """Process a list of places"""
        with ThreadPoolExecutor(max_workers=min(10, len(places))) as executor:
            future_to_place = {
                executor.submit(
                    process_place, self.api_key, p['place_id'], h3_index, 
                    lat, lng, boundary_name, self.max_links, self.rate_limiter
                ): p
                for p in places
            }
            
            for future in as_completed(future_to_place):
                if self.stop_flag.is_set() or self.stats['businesses_with_email'] >= self.target_results:
                    break
                
                try:
                    details, emails = future.result()
                    if not details: 
                        continue
                    
                    # Flatten the data
                    row = {col: flatten_value(details.get(col, "")) for col in COLUMNS}
                    
                    # Write to CSV
                    has_emails = bool(emails)
                    self.write_to_csv(row, has_emails)
                    
                except Exception as e:
                    print(f"        ❌ Error processing place future: {e}")

def process_boundary_streamlit(*args):
    """Legacy function for compatibility"""
    pass
