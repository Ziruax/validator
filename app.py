import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search
from urllib.parse import urlparse # Keep for domain display
import io # Keep for downloads
# zipfile and unicodedata not needed for this simplified version

# --- Streamlit Page Configuration (from your working code) ---
st.set_page_config(
    page_title="WhatsApp Link Validator (Stable Base)", # Adjusted title
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Constants (from your working code) ---
WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" # Added for consistency

# EMOJI_PATTERN from your working code (use if desired)
# EMOJI_PATTERN = re.compile(...) # Keep commented unless explicitly wanted


# --- Custom CSS (from your working code, minimal adjustments) ---
st.markdown("""
    <style>
    .main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
    .subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; margin-bottom: 15px; } /* Added margin */
    .stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 18px; } /* Adjusted padding */
    .stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }
    .stProgress > div > div > div > div { background-color: #25D366; } /* Adjusted progress bar selector */
    .metric-card { background-color: #F5F6F5; padding: 15px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); color: #333333; text-align: center; } /* Adjusted padding/radius */
    .stTextInput input, .stTextArea textarea, .stFileUploader section div[data-testid="stFileDropzone"] { border: 1px solid #25D366 !important; border-radius: 5px !important; } /* Target inner elements */
    .sidebar .sidebar-content { background-color: #F5F6F5; padding-top: 1rem;}
    .stExpander { border: 1px solid #E0E0E0; border-radius: 8px; } /* Adjusted radius */
    </style>
""", unsafe_allow_html=True)


# --- Core Functions (EXACTLY from your working code) ---

# @st.cache_data(ttl=1800) # Add caching later if needed
def validate_link(link):
    """Validate a WhatsApp group link and return details if active."""
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
        response = requests.get(link, headers=headers, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result
        # Check final URL after redirects
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid Link (Redirected)"
            return result
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = unescape(meta_title['content']).strip()
            # --- Optional: Uncomment below to re-enable emoji/non-ASCII removal ---
            # if 'EMOJI_PATTERN' in globals(): # Check if pattern defined
            #     group_name = EMOJI_PATTERN.sub('', group_name)
            #     group_name = ''.join(c for c in group_name if ord(c) < 128)
            result["Group Name"] = group_name if group_name else "Unnamed Group"
        else:
            result["Group Name"] = "Unnamed Group"
        img_tags = soup.find_all('img', src=True)
        logo_found = False
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN.match(src):
                result["Logo URL"] = src
                result["Status"] = "Active"
                logo_found = True
                break
        if not logo_found and result["Status"] not in ["Error", f"HTTP Error {response.status_code}", "Invalid Link (Redirected)"]: # Avoid overwriting previous error
             result["Status"] = "Expired" # Assume expired if active status wasn't set

    except requests.exceptions.RequestException as e:
        result["Status"] = f"Network Error: {str(e)[:50]}" # Shorter error
    except Exception as e:
        result["Status"] = f"Error: {str(e)[:50]}"
    return result

# @st.cache_data(ttl=3600) # Add caching later if needed
def scrape_whatsapp_links(url):
    """Scrape WhatsApp group links from a webpage (Original working logic)."""
    links = set() # Use set internally for efficiency
    try:
        headers = {"User-Agent": DEFAULT_USER_AGENT}
        response = requests.get(url, headers=headers, timeout=10) # Original timeout, no raise_for_status
        # Removed encoding force, rely on requests default or apparent_encoding
        soup = BeautifulSoup(response.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a.get('href', '') # Use .get for safety
            if href.startswith(WHATSAPP_DOMAIN):
                # Basic cleaning: remove query params
                clean_link = href.split('?')[0]
                # Basic validation of invite code structure might be useful here later
                if re.match(r"https?://chat\.whatsapp\.com/([a-zA-Z0-9_-]{18,25})$", clean_link):
                    links.add(clean_link)
                else:
                    links.add(href) # Add original if format is unexpected but starts right

        # Use original text finding logic but with the stricter regex first, fallback to broader
        # This might be slightly slower but tries to be accurate then broad
        text_content = soup.get_text(separator=" ") # Use get_text for consistency
        strict_matches = re.finditer(WHATSAPP_LINK_REGEX, text_content)
        for match in strict_matches:
            links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")

        # Fallback using original broader regex on text if WHATSAPP_DOMAIN is present
        if WHATSAPP_DOMAIN in text_content:
             broad_matches = re.findall(r'https?://chat\.whatsapp\.com/[^\s\'\"<>]+', text_content) # Avoid trailing punctuation
             for b_link in broad_matches:
                 clean_b_link = b_link.split('?')[0]
                 links.add(clean_b_link) # Add cleaned broad match

        return list(links)
    except Exception as e:
        # Optional: Log the error for debugging
        # print(f"Scraping error on {url}: {e}")
        return [] # Return empty list on any error, like original

# @st.cache_data(ttl=1800) # Add caching later if needed
def google_search(query, top_n=5):
    """Fetch URLs from Google's top N search results (Original working logic)."""
    try:
        # Original call, added pause as it's generally needed now
        urls = list(search(query, num_results=top_n, lang="en", pause=2.0, user_agent=DEFAULT_USER_AGENT))
        if not urls:
            st.warning(f"No Google search results found for '{query}'.")
            return []
        return urls
    except Exception as e:
        st.error(f"Google Search error: {str(e)}. Google might be blocking requests. Try again later or with fewer results.")
        return []

# --- Loading Functions (Adapted for TXT/CSV/Excel) ---
def load_links_from_file(uploaded_file) -> list:
    """Load links from TXT or CSV, attempting more robust parsing."""
    links = set()
    file_name = uploaded_file.name.lower()
    try:
        if file_name.endswith('.csv'):
            # Try reading CSV, look for links in all columns
            uploaded_file.seek(0)
            df_csv = pd.read_csv(uploaded_file, header=None)
            for col in df_csv.columns:
                for item in df_csv[col].dropna().astype(str):
                    if WHATSAPP_DOMAIN in item:
                        # Extract all possible links from the cell
                        cell_links = re.findall(r'https?://chat\.whatsapp\.com/[^\s\'\"<>]+', item)
                        for link in cell_links:
                            clean_link = link.split('?')[0].rstrip('.?,!;"') # Clean trailing punctuation
                            if re.match(WHATSAPP_LINK_REGEX, clean_link): # Validate format
                                links.add(clean_link)
        else: # Assume TXT
            uploaded_file.seek(0)
            lines = [line.decode("utf-8", errors="replace").strip() for line in uploaded_file.readlines()]
            for line in lines:
                 if WHATSAPP_DOMAIN in line:
                    line_links = re.findall(r'https?://chat\.whatsapp\.com/[^\s\'\"<>]+', line)
                    for link in line_links:
                        clean_link = link.split('?')[0].rstrip('.?,!;"')
                        if re.match(WHATSAPP_LINK_REGEX, clean_link):
                             links.add(clean_link)
    except Exception as e:
        st.error(f"Error reading file '{uploaded_file.name}': {e}")
    if not links:
        st.warning(f"No valid WhatsApp links found in '{uploaded_file.name}'.")
    return list(links)

def load_keywords_from_excel(uploaded_file) -> list:
    keywords = []
    try:
        df_excel = pd.read_excel(uploaded_file, header=None, sheet_name=0)
        keywords = [kw for kw in df_excel.iloc[:, 0].dropna().astype(str).str.strip().tolist() if kw]
        if not keywords: st.warning("No keywords found in the first column of the Excel file.")
    except Exception as e: st.error(f"Error reading Excel file '{uploaded_file.name}': {e}")
    return keywords

# --- Main Application ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Validator (Stable Base) 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Validate links using various input methods.</p>', unsafe_allow_html=True)

    # Initialize session state if keys don't exist
    if 'results_list' not in st.session_state: st.session_state.results_list = []
    if 'process_counter' not in st.session_state: st.session_state.process_counter = 0 # To force re-run

    with st.sidebar:
        st.header("⚙️ Settings")
        input_method_options = [ # Added new methods
            "Google Search (Single Keyword)",
            "Google Search (Excel Keywords)",
            "Scrape Specific Webpage(s)",
            "Scrape Entire Website (Domain)", # To be implemented carefully later
            "Validate Manual Links",
            "Validate Links from File (TXT/CSV)"
        ]
        input_method = st.selectbox("Input Method", input_method_options, index=0, key="sb_input_method_stable")
        
        top_n_google = 5 # Default
        if "Google Search" in input_method:
            top_n_google = st.slider("Google Results/Query:", 1, 10, 5, key="sb_google_results_stable")
        
        # Placeholder for Domain Crawl settings if we add it back
        # if "Entire Website" in input_method:
        #     max_crawl_pages = st.slider(...)

        max_workers = st.slider("Max Validation Workers:", 1, 8, 4, key="sb_workers_stable", help="Concurrent link checks.")

    if st.sidebar.button("🗑️ Clear Results", use_container_width=True, key="sb_clear_results_stable"):
        st.session_state.results_list = []
        st.session_state.process_counter += 1 # Increment to trigger UI update below
        st.success("Results cleared!")
        st.experimental_rerun() # Force rerun to clear display


    # --- Input Area ---
    input_area = st.container()
    run_processing = False # Flag to trigger processing block
    links_to_process = []   # List to hold links for validation/processing
    source_info = "Unknown" # To store where links came from

    with input_area:
        action_label = "🚀 Process" # Default button label

        if input_method == "Google Search (Single Keyword)":
            keyword_gs_single = st.text_input("Google Search Query:", key="input_gs_keyword")
            if st.button(f"🔍 Search & Validate '{keyword_gs_single[:20]}...'", use_container_width=True, key="btn_gs_single"):
                if not keyword_gs_single: st.warning("Please enter a search query."); st.stop()
                run_processing = True
                source_info = f"Google Keyword: {keyword_gs_single}"
                with st.spinner(f"Searching Google for '{keyword_gs_single}'..."):
                    search_result_urls = google_search(keyword_gs_single, top_n=top_n_google)
                if search_result_urls:
                    st.success(f"Found {len(search_result_urls)} pages. Now scraping links...")
                    scrape_progress = st.progress(0.0, text=f"Scraping 0/{len(search_result_urls)}")
                    temp_links = set()
                    for i, url in enumerate(search_result_urls):
                        temp_links.update(scrape_whatsapp_links(url)) # Use original scraper
                        scrape_progress.progress((i+1)/len(search_result_urls), text=f"Scraped {i+1}/{len(search_result_urls)}")
                    scrape_progress.empty()
                    links_to_process = list(temp_links)
                    if not links_to_process: st.warning("No WhatsApp links found on the searched pages.")
                    else: st.info(f"Found {len(links_to_process)} unique links to validate.")
                else: st.info("No pages found from Google search.")


        elif input_method == "Validate Manual Links":
            manual_links_text = st.text_area("Enter WhatsApp Links (one per line):", height=150, key="input_manual_links")
            if st.button("✍️ Validate Manual Links", use_container_width=True, key="btn_manual"):
                if not manual_links_text: st.warning("Please enter links."); st.stop()
                links_to_process = [line.strip() for line in manual_links_text.split('\n') if line.strip().startswith(WHATSAPP_DOMAIN)]
                source_info = "Manual Entry"
                if not links_to_process: st.warning("No valid WhatsApp links entered.")
                else: run_processing = True

        elif input_method == "Validate Links from File (TXT/CSV)":
            uploaded_file = st.file_uploader("Upload TXT or CSV File:", type=["txt", "csv"], key="input_file_upload")
            if uploaded_file and st.button(f"📤 Validate File '{uploaded_file.name}'", use_container_width=True, key="btn_file"):
                 links_to_process = load_links_from_file(uploaded_file) # Use adapted loader
                 source_info = f"File: {uploaded_file.name}"
                 if not links_to_process: st.warning("No valid WhatsApp links found in the file.")
                 else: run_processing = True
        
        # Placeholders for other input methods
        elif input_method == "Google Search (Excel Keywords)":
            st.info("Excel keyword search feature is planned for a future update.")
            # excel_file = st.file_uploader(...)
            # if st.button(...): run_processing = True # Add logic here later
        elif input_method == "Scrape Specific Webpage(s)":
             st.info("Specific page scraping feature is planned for a future update.")
             # specific_urls_text = st.text_area(...)
             # if st.button(...): run_processing = True # Add logic here later
        elif input_method == "Scrape Entire Website (Domain)":
             st.info("Full domain crawling feature is planned for a future update.")
             # domain_to_crawl = st.text_input(...)
             # if st.button(...): run_processing = True # Add logic here later


    # --- Processing Block ---
    if run_processing and links_to_process:
        st.session_state.results_list = [] # Clear previous results for this run
        st.info(f"Validating {len(links_to_process)} links from '{source_info}'...")
        progress_bar_val = st.progress(0.0)
        status_text_val = st.empty()
        status_text_val.text(f"Validated 0/{len(links_to_process)} links...")

        temp_results = [] # Collect results for this run
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Use original validate_link
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_process}
            for i, future in enumerate(as_completed(future_to_link)):
                try:
                    result_item = future.result()
                    # Add source info AFTER validation if needed, or handle differently
                    # For now, keep results simple as per original code
                    temp_results.append(result_item)
                except Exception as e_future:
                     link = future_to_link[future] # Get original link
                     temp_results.append({"Group Name": "Validation Error", "Group Link": link, "Logo URL": "", "Status": f"Thread Error: {e_future}", "Source": source_info})

                progress_bar_val.progress((i + 1) / len(links_to_process))
                status_text_val.text(f"Validated {i + 1}/{len(links_to_process)} links...")
        
        st.session_state.results_list = temp_results # Update session state with new results
        st.session_state.process_counter += 1 # Increment to trigger UI update
        status_text_val.success(f"Validation complete for {len(links_to_process)} links!")
        progress_bar_val.empty() # Remove progress bar
        st.experimental_rerun() # Rerun to display results immediately

    # --- Display Results Area ---
    st.markdown("---")
    st.subheader("📊 Validation Results")
    # Use process_counter to ensure results display updates after processing
    display_key = f"results_display_{st.session_state.process_counter}"

    if st.session_state.results_list: # Check if there are results to display
        df_results = pd.DataFrame(st.session_state.results_list)
        active_df = df_results[df_results['Status'] == 'Active'].copy()
        expired_df = df_results[df_results['Status'] == 'Expired'].copy() # Original simple filter
        error_df = df_results[~df_results['Status'].isin(['Active', 'Expired'])].copy() # Crude error capture

        col1, col2, col3 = st.columns(3)
        with col1: st.markdown(f'<div class="metric-card">Total Processed<br><b>{len(df_results)}</b></div>', unsafe_allow_html=True)
        with col2: st.markdown(f'<div class="metric-card">✅ Active<br><b>{len(active_df)}</b></div>', unsafe_allow_html=True)
        with col3: st.markdown(f'<div class="metric-card">⚠️ Expired<br><b>{len(expired_df)}</b></div>', unsafe_allow_html=True)
        # Maybe add 4th column for errors later

        with st.expander("🔎 View, Filter & Download Results", expanded=True):
            status_options = ["All"] + sorted(df_results['Status'].dropna().unique().tolist())
            status_filter_sel = st.multiselect("Filter by Status:", status_options[1:], default=["Active"], key=f"filter_status_{display_key}") # Default Active

            filtered_df = df_results[df_results['Status'].isin(status_filter_sel)] if status_filter_sel else df_results

            st.dataframe(
                filtered_df,
                column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Group"),
                    # Display logo as image if URL exists, else show text URL
                    "Logo URL": st.column_config.ImageColumn("Logo", width="small"), # Try ImageColumn first
                    # Fallback needed if ImageColumn fails on empty strings
                    # "Logo URL": st.column_config.LinkColumn("Logo Link"), # Original fallback
                },
                height=400,
                use_container_width=True,
                key=f"dataframe_{display_key}",
                hide_index=True
            )

        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            if not active_df.empty:
                csv_active_data = active_df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Active Groups (CSV)", csv_active_data, "active_groups.csv", "text/csv", use_container_width=True, key=f"dl_active_{display_key}")
            else: st.caption("No active groups to download.")
        with col_dl2:
            if not df_results.empty:
                 csv_all_data = df_results.to_csv(index=False).encode('utf-8')
                 st.download_button("📥 Download All Results (CSV)", csv_all_data, "all_results.csv", "text/csv", use_container_width=True, key=f"dl_all_{display_key}")
            else: st.caption("No results to download.")

    elif st.session_state.process_clicked: # Processed but no results
         st.info("🏁 Processing finished, but no results were generated.", icon="🤷")
    else: # Initial state
        st.info("✨ Select an input method and provide input to start validating links!", icon="👋")

if __name__ == "__main__":
    main()
