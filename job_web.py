import streamlit as st
import pandas as pd
import json
import time
import uuid
import os
import csv
import threading
import queue
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import Manager, Lock, Semaphore
import io
import glob
from streamlit_autorefresh import st_autorefresh

# Import all the scraping functions
from scraper_core import (
    process_boundary_streamlit, load_boundaries_from_geojson_data, 
    H3_RESOLUTIONS, DEFAULT_H3_RESOLUTION, COLUMNS,
    ScrapingManager, flatten_value
)

# Initialize session state
def initialize_session_state():
    if 'scraping_active' not in st.session_state:
        st.session_state.scraping_active = False
    if 'scraping_stats' not in st.session_state:
        st.session_state.scraping_stats = {
            'businesses_with_email': 0,
            'businesses_without_email': 0,
            'api_calls': 0,
            'places_processed': 0
        }
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())[:8]
    if 'scraping_manager' not in st.session_state:
        st.session_state.scraping_manager = None
    if 'csv_files' not in st.session_state:
        st.session_state.csv_files = {
            'with_emails': None,
            'without_emails': None
        }
    if 'selected_columns' not in st.session_state:
        st.session_state.selected_columns = [
            'name', 'formatted_address', 'website', 'emails', 
            'formatted_phone_number', 'rating', 'city'
        ]

def main():
    st.set_page_config(
        page_title="LeadXBusiness",
        page_icon="🏢",
        layout="wide"
    )
    
    initialize_session_state()
    
    # Auto-refresh every 2 seconds (always active for real-time updates)
    count = st_autorefresh(interval=2000, key="data_refresh")

    # Always update stats from manager if it exists
    if st.session_state.scraping_manager:
        update_stats_from_manager()
    
    st.title("LeadXBusiness")
    st.markdown("*Professional business data collection platform*")
    st.markdown("---")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # API Key Input
        api_key = st.text_input(
            "Google Places API Key",
            type="password",
            help="Enter your Google Places API key",
            placeholder="AIza..."
        )
        
        # GeoJSON Upload
        st.subheader("Geographic Boundaries")
        uploaded_file = st.file_uploader(
            "Upload GeoJSON file",
            type=['geojson', 'json'],
            help="Upload a GeoJSON file containing the boundaries to search"
        )
        
        boundaries = []
        if uploaded_file is not None:
            try:
                geojson_data = json.load(uploaded_file)
                boundaries = load_boundaries_from_geojson_data(geojson_data)
                st.success(f"Loaded {len(boundaries)} boundaries")
                
                # Show boundary names
                with st.expander("View Boundaries"):
                    for i, boundary in enumerate(boundaries):
                        st.write(f"{i}: {boundary['name']}")
                        
            except Exception as e:
                st.error(f"Error loading GeoJSON: {str(e)}")
        
        # Boundary Selection
        selected_boundaries = []
        if boundaries:
            st.subheader("Select Boundaries")
            
            # Select All checkbox
            select_all = st.checkbox("Select All Boundaries")
            
            if select_all:
                selected_boundaries = boundaries
                st.info(f"Selected all {len(boundaries)} boundaries")
            else:
                boundary_names = [f"{i}: {b['name']}" for i, b in enumerate(boundaries)]
                selected_indices = st.multiselect(
                    "Choose boundaries:",
                    options=list(range(len(boundaries))),
                    format_func=lambda x: boundary_names[x],
                    help="Select one or more boundaries to search"
                )
                selected_boundaries = [boundaries[i] for i in selected_indices]
        
        # Search Parameters
        st.subheader("Search Parameters")
        
        keywords_input = st.text_area(
            "Keywords (one per line)",
            placeholder="software companies\ntechnology\nstartups\nrestaurants",
            help="Enter search keywords, one per line",
            height=100
        )
        keywords = [k.strip() for k in keywords_input.split('\n') if k.strip()]
        
        business_type = st.text_input(
            "Business Type (Optional)",
            placeholder="restaurant, store, etc.",
            help="Specify a business type from Google Places API types"
        )
        
        # Column Selection
        st.subheader("Column Selection")
        
        # Select All Columns checkbox
        select_all_columns = st.checkbox("Select All Columns")
        
        if select_all_columns:
            st.session_state.selected_columns = COLUMNS.copy()
            st.info(f"Selected all {len(COLUMNS)} columns")
        else:
            st.session_state.selected_columns = st.multiselect(
                "Choose columns to include:",
                options=COLUMNS,
                default=st.session_state.selected_columns,
                help="Select the columns you want to include in your data export"
            )
        
        st.info(f"Selected {len(st.session_state.selected_columns)} columns")
        
        # Technical Parameters
        st.subheader("Settings")
        
        target_results = st.number_input(
            "Target businesses with emails",
            min_value=1,
            max_value=10000,
            value=100,
            help="Stop collection after finding this many businesses with emails"
        )
        
        # H3 Resolution Selection
        st.write("Search Resolution")
        resolution_options = {}
        for res, info in H3_RESOLUTIONS.items():
            resolution_options[f"{res}: {info['name']} (~{info['avg_edge_length_km']:.1f}km edge)"] = res
        
        selected_resolution_text = st.selectbox(
            "Choose resolution:",
            list(resolution_options.keys()),
            index=list(resolution_options.values()).index(DEFAULT_H3_RESOLUTION),
            help="Higher resolution = smaller search areas = more detailed results"
        )
        h3_resolution = resolution_options[selected_resolution_text]
        
        max_concurrency = st.slider(
            "API Concurrency",
            min_value=1,
            max_value=50,
            value=10,
            help="Maximum concurrent API calls"
        )
        
        max_links = st.slider(
            "Max Links per Website",
            min_value=5,
            max_value=50,
            value=25,
            help="Maximum number of pages to crawl per website for email extraction"
        )
    
    # Main content area
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.header("Data Collection")
        
        # Validation - Allow either keywords OR business_type OR both
        can_start = (
            api_key and 
            selected_boundaries and 
            (keywords or business_type.strip()) and
            st.session_state.selected_columns
        )
        
        if not can_start:
            missing = []
            if not api_key: missing.append("API Key")
            if not selected_boundaries: missing.append("Boundaries")
            if not keywords and not business_type.strip(): missing.append("Keywords or Business Type")
            if not st.session_state.selected_columns: missing.append("Column Selection")
            st.warning(f"Missing: {', '.join(missing)}")
        
        # Control buttons
        col_start, col_stop = st.columns(2)
        
        with col_start:
            if st.button(
                "Start Collection",
                disabled=not can_start or st.session_state.scraping_active,
                use_container_width=True,
                type="primary"
            ):
                start_scraping(
                    api_key, selected_boundaries, keywords if keywords else [""], business_type,
                    h3_resolution, target_results, max_concurrency, max_links
                )
        
        with col_stop:
            if st.button(
                "Stop Collection",
                disabled=not st.session_state.scraping_active,
                use_container_width=True,
                type="secondary"
            ):
                stop_scraping()
        
        # Status display
        if st.session_state.scraping_active:
            st.success("Collection in progress... (Auto-refreshing every 2 seconds)")
        elif st.session_state.csv_files['with_emails'] or st.session_state.csv_files['without_emails']:
            # Check if we have any data
            total_businesses = st.session_state.scraping_stats['places_processed']
            if total_businesses > 0:
                st.info(f"Collection completed! Found {total_businesses} businesses total.")
            else:
                st.warning("Collection stopped - No data collected yet")
        else:
            st.info("Ready to start collection")
        
        # Progress metrics
        stats = st.session_state.scraping_stats
        col_metric1, col_metric2, col_metric3, col_metric4 = st.columns(4)
        
        with col_metric1:
            st.metric("With Emails", stats['businesses_with_email'])
        with col_metric2:
            st.metric("Without Emails", stats['businesses_without_email'])
        with col_metric3:
            st.metric("Total Processed", stats['places_processed'])
        with col_metric4:
            st.metric("API Calls", stats['api_calls'])
        
        # Progress bar
        if target_results > 0:
            progress = min(stats['businesses_with_email'] / target_results, 1.0)
            st.progress(progress)
            st.write(f"Progress: {stats['businesses_with_email']}/{target_results} businesses with emails")
    
    with col2:
        st.header("Session Info")
        
        st.write(f"**Session ID:** {st.session_state.session_id}")
        st.write(f"**Selected Boundaries:** {len(selected_boundaries)}")
        st.write(f"**Keywords:** {len(keywords)}")
        st.write(f"**Selected Columns:** {len(st.session_state.selected_columns)}")
        if keywords:
            st.write("**Keywords:** " + ", ".join(keywords[:3]) + ("..." if len(keywords) > 3 else ""))
    
    # Real-time data view with auto-refresh
    st.header("Data Preview")
    current_time = time.strftime("%H:%M:%S")
    if st.session_state.scraping_active:
        st.write(f"*Auto-refreshing every 2 seconds - Latest entries appear at the top | Current time: {current_time}*")
    else:
        st.write(f"*Data is read directly from files on disk | Last refresh: {current_time}*")
    
    # Tabs for different data views
    tab1, tab2, tab3 = st.tabs(["With Emails", "Without Emails", "All Data"])
    
    with tab1:
        display_real_time_csv_data('with_emails', "Businesses with emails")
    
    with tab2:
        display_real_time_csv_data('without_emails', "Businesses without emails")
    
    with tab3:
        display_combined_csv_data()

def display_real_time_csv_data(csv_type, title):
    """Display real-time CSV data for a specific type"""
    csv_file = st.session_state.csv_files[csv_type]
    
    if csv_file and os.path.exists(csv_file):
        df = load_csv_with_selected_columns(csv_file)
        if not df.empty:
            display_dataframe_with_live_updates(df, title)
        else:
            st.info(f"File exists but no data yet...")
    else:
        st.info(f"No {title.lower()} found yet...")

def display_combined_csv_data():
    """Display combined data from both CSV files"""
    all_data = []
    
    # Load data from both files
    if st.session_state.csv_files['with_emails'] and os.path.exists(st.session_state.csv_files['with_emails']):
        df_with = load_csv_with_selected_columns(st.session_state.csv_files['with_emails'])
        if not df_with.empty:
            df_with['has_emails'] = True
            all_data.append(df_with)
    
    if st.session_state.csv_files['without_emails'] and os.path.exists(st.session_state.csv_files['without_emails']):
        df_without = load_csv_with_selected_columns(st.session_state.csv_files['without_emails'])
        if not df_without.empty:
            df_without['has_emails'] = False
            all_data.append(df_without)
    
    if all_data:
        df_all = pd.concat(all_data, ignore_index=True)
        # Sort by index in descending order to show newest first
        df_all = df_all.sort_index(ascending=False)
        display_dataframe_with_live_updates(df_all, "All businesses")
    else:
        st.info("No data collected yet. Start collection to see results here.")

def load_csv_with_selected_columns(csv_file_path):
    """Load CSV file and return only selected columns"""
    try:
        if not os.path.exists(csv_file_path):
            return pd.DataFrame()
        
        # Read the CSV file
        df = pd.read_csv(csv_file_path)
        
        if df.empty:
            return df
        
        # Filter to only selected columns that exist in the dataframe
        available_columns = [col for col in st.session_state.selected_columns if col in df.columns]
        
        if available_columns:
            return df[available_columns]
        else:
            return df
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

def display_dataframe_with_live_updates(df, title):
    """Display a dataframe with live updates and newest entries first"""
    st.write(f"**{title}** ({len(df)} entries)")
    
    if not df.empty:
        # Show latest entries first (newest at top)
        df_display = df.tail(100).iloc[::-1].copy()
        
        # Format emails column for better display if it exists
        if 'emails' in df_display.columns:
            df_display['emails'] = df_display['emails'].apply(
                lambda x: format_emails_for_display(x)
            )
        
        # Format other list columns
        for col in df_display.columns:
            if col in ['facebook', 'twitter_x', 'instagram', 'linkedin']:
                df_display[col] = df_display[col].apply(
                    lambda x: format_list_for_display(x)
                )
        
        # Add a timestamp indicator
        current_time = time.strftime("%H:%M:%S")
        if len(df_display) > 0:
            st.caption(f"Last updated: {current_time} | Showing latest 100 entries")
        else:
            st.caption(f"Last checked: {current_time} | No data yet")
        
        st.dataframe(
            df_display,
            use_container_width=True,
            height=400
        )
        
        # Show summary info
        st.caption(f"Total entries: {len(df)} | Displayed columns: {', '.join(df_display.columns[:5])}{'...' if len(df_display.columns) > 5 else ''}")
        
        # Show latest entry details if available
        if len(df_display) > 0:
            with st.expander("Latest Entry Details"):
                latest_entry = df_display.iloc[0]
                for col, value in latest_entry.items():
                    if value and str(value) != 'nan':
                        st.write(f"**{col}:** {value}")
    else:
        st.warning("No data to display")

def format_emails_for_display(emails_value):
    """Format emails for better display"""
    try:
        if isinstance(emails_value, str):
            if emails_value.startswith('[') and emails_value.endswith(']'):
                emails_list = eval(emails_value)
                return ', '.join(emails_list) if emails_list else 'None'
            else:
                return emails_value if emails_value else 'None'
        elif isinstance(emails_value, list):
            return ', '.join(emails_value) if emails_value else 'None'
        else:
            return str(emails_value) if emails_value else 'None'
    except:
        return str(emails_value) if emails_value else 'None'

def format_list_for_display(list_value):
    """Format list values for better display"""
    try:
        if isinstance(list_value, str):
            if list_value.startswith('[') and list_value.endswith(']'):
                parsed_list = eval(list_value)
                return ', '.join(parsed_list) if parsed_list else ''
            else:
                return list_value if list_value else ''
        elif isinstance(list_value, list):
            return ', '.join(list_value) if list_value else ''
        else:
            return str(list_value) if list_value else ''
    except:
        return str(list_value) if list_value else ''

def start_scraping(api_key, boundaries, keywords, business_type, h3_resolution, target_results, max_concurrency, max_links):
    """Start the scraping process"""
    st.session_state.scraping_active = True
    st.session_state.scraping_stats = {
        'businesses_with_email': 0,
        'businesses_without_email': 0,
        'api_calls': 0,
        'places_processed': 0
    }
    
    # Create new session ID and CSV files
    st.session_state.session_id = str(uuid.uuid4())[:8]
    
    # Create CSV file paths
    csv_with_emails = f"businesses_with_emails_{st.session_state.session_id}.csv"
    csv_without_emails = f"businesses_without_emails_{st.session_state.session_id}.csv"
    
    st.session_state.csv_files = {
        'with_emails': csv_with_emails,
        'without_emails': csv_without_emails
    }
    
    # Create CSV files with headers
    for csv_file in [csv_with_emails, csv_without_emails]:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
    
    # Handle empty keywords - if no keywords provided, use empty list or single empty string
    processed_keywords = keywords if keywords else [""]
    
    # Create scraping manager
    scraping_manager = ScrapingManager(
        api_key=api_key,
        boundaries=boundaries,
        keywords=processed_keywords,
        business_type=business_type.strip() if business_type else "",
        h3_resolution=h3_resolution,
        target_results=target_results,
        max_concurrency=max_concurrency,
        max_links=max_links,
        csv_with_emails=csv_with_emails,
        csv_without_emails=csv_without_emails
    )
    
    # Store the manager in session state
    st.session_state.scraping_manager = scraping_manager
    
    # Start scraping in a separate thread
    scraping_thread = threading.Thread(
        target=scraping_manager.run_scraping,
        daemon=True
    )
    scraping_thread.start()

def update_stats_from_manager():
    """Update stats from the scraping manager if it exists"""
    if hasattr(st.session_state, 'scraping_manager') and st.session_state.scraping_manager:
        try:
            current_stats = st.session_state.scraping_manager.get_stats()
            st.session_state.scraping_stats.update(current_stats)
            
            # Check if scraping should stop
            if (current_stats['businesses_with_email'] >= st.session_state.scraping_manager.target_results or 
                st.session_state.scraping_manager.stop_flag.is_set()):
                if st.session_state.scraping_active:  # Only show completion message once
                    st.session_state.scraping_active = False
                    st.success("Collection completed! Target reached or stopped.")
                
        except Exception as e:
            print(f"Error updating stats: {e}")
            if st.session_state.scraping_active:
                st.session_state.scraping_active = False
                st.error(f"Collection stopped due to error: {e}")

def stop_scraping():
    """Stop the scraping process"""
    st.session_state.scraping_active = False
    if st.session_state.scraping_manager:
        st.session_state.scraping_manager.stop_scraping()
    st.success("Collection stopped by user")

if __name__ == "__main__":
    main()
