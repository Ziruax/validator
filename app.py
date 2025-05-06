import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search
from urllib.parse import urljoin, urlparse
import io
import zipfile
import unicodedata # For language heuristic (will be added later if relevancy is kept)

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="WhatsApp Link Validator & Scraper (Enhanced)",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.example.com/help',
        'Report a bug': "https://www.example.com/bug",
        'About': "# WhatsApp Link Validator & Scraper\nEnhanced with more input methods and organized outputs."
    }
)

# --- Constants ---
WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
WHATSAPP_LINK_REGEX = r"https?://chat\.whatsapp\.com/([a-zA-Z0-9_-]{18,25})(?=[?\s\"']|$)"

# EMOJI_PATTERN from your working code (will be used in validate_link if emoji removal is desired)
EMOJI_PATTERN = re.compile(
    "["
    u"\U0001F600-\U0001F64F" u"\U0001F300-\U0001F5FF" u"\U0001F680-\U0001F6FF" u"\U0001F1E0-\U0001F1FF"
    u"\U00002702-\U000027B0" u"\U000024C2-\U0001F251" u"\U0001F900-\U0001F9FF" u"\U0001FA70-\U0001FAFF"
    u"\U00002600-\U000026FF" u"\U00002700-\U000027BF" u"\U0001F700-\U0001F77F" u"\U0001F7E0-\U0001F7FF"
    u"\U0001F800-\U0001F8FF" u"\U0001F000-\U0001F0FF" u"\U0001F100-\U0001F1FF"
    "]+", flags=re.UNICODE
)
# English stop words for relevancy (will be added later)
# ENGLISH_STOP_WORDS = set([...])


# --- Custom CSS ---
st.markdown("""
    <style>
    .main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
    .subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; margin-bottom: 20px;}
    .stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 18px; }
    .stButton>button:hover { background-color: #1EBE5A; }
    .stProgress > div > div > div > div { background-color: #25D366; }
    .metric-card { background-color: #F5F6F5; padding: 15px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); color: #333333; text-align: center; }
    .stTextInput input, .stTextArea textarea, .stFileUploader section div[data-testid="stFileDropzone"] { border: 1px solid #25D366 !important; border-radius: 6px !important; }
    .sidebar .sidebar-content { background-color: #F5F6F5; padding-top: 1rem;}
    .stExpander { border: 1px solid #E0E0E0; border-radius: 8px; }
    .stExpander header { font-size: 1.1em; font-weight: 500;}
    /* Markdown output styles from ProMax */
    .markdown-output-area { background-color: #F8F9FA; padding: 18px; border-radius: 10px; border: 1px solid #DEE2E6; max-height: 550px; overflow-y: auto; font-family: 'Menlo', 'Consolas', monospace; white-space: pre-wrap; line-height: 1.65; box-shadow: inset 0 2px 4px rgba(0,0,0,0.03);}
    .markdown-output-area table { width: 100%; border-collapse: collapse; margin-bottom: 1.2em; background-color: #fff; }
    .markdown-output-area th, .markdown-output-area td { border: 1px solid #E0E0E0; padding: 10px 12px; text-align: left; vertical-align: middle;}
    .markdown-output-area th { background-color: #F1F3F5; font-weight: 600; color: #343A40;}
    .markdown-output-area img { max-width: 50px; max-height: 50px; border-radius: 50%; display: block; margin: auto; border: 1px solid #F0F0F0; }
    </style>
""", unsafe_allow_html=True)


# --- Helper Functions ---
def sanitize_filename(name, max_len=70):
    name = str(name); name = re.sub(r'https?://', '', name); name = re.sub(r'[^\w\s-]', '', name).strip().lower(); name = re.sub(r'[-\s]+', '-', name)
    sanitized = name[:max_len]; return sanitized if sanitized else f"unnamed-file-{int(time.time())}"

# is_primarily_english and get_relevancy will be added later if relevancy feature is re-integrated.

# --- Core Logic Functions (Based on your working simpler version, enhanced) ---

@st.cache_data(ttl=1800, show_spinner="♻️ Validating link...") # Added cache
def validate_link(link: str, associated_keyword: str = None) -> dict: # Added associated_keyword for future
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error", "Keyword Relevancy": "N/A"} # Added Relevancy
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}

    if not link or not link.startswith(WHATSAPP_DOMAIN):
        result["Status"] = "Invalid Format (No WA Domain)"
        return result
    try:
        response = requests.get(link, headers=headers, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8' 
        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result
        if WHATSAPP_DOMAIN not in response.url: # Check final URL
            result["Status"] = "Invalid Link (Redirected)"
            # Add more detailed status if original was WA but got redirected to "invite revoked" page
            if WHATSAPP_DOMAIN in link:
                soup_check_redirect = BeautifulSoup(response.text, 'html.parser')
                if soup_check_redirect.find(string=re.compile(r"(invite link revoked|couldn't join|link was reset|no longer valid|cannot be used)", re.IGNORECASE)):
                     result["Status"] = "Expired/Revoked (Redirected)"
            return result
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check for "invite revoked" on the current page itself
        if soup.find(string=re.compile(r"(invite link revoked|link was reset|no longer valid|cannot be used)", re.IGNORECASE)):
            result["Status"] = "Expired/Revoked (Content)"
            meta_title_revoked = soup.find('meta', property='og:title')
            group_name_revoked = (unescape(meta_title_revoked['content']).strip() if meta_title_revoked and meta_title_revoked.get('content') else "Revoked Group")
            # Apply EMOJI_PATTERN and ASCII filter from your working code if desired
            # group_name_revoked = EMOJI_PATTERN.sub('', group_name_revoked)
            # group_name_revoked = ''.join(c for c in group_name_revoked if ord(c) < 128)
            result["Group Name"] = group_name_revoked or "Revoked Group (Name N/A)"
            # Do not return yet, try to get logo if page structure still allows (unlikely for revoked)
        else: # Not explicitly revoked on this page, proceed to find name/logo
            meta_title = soup.find('meta', property='og:title')
            if meta_title and meta_title.get('content'):
                group_name = unescape(meta_title['content']).strip()
                # Apply EMOJI_PATTERN and ASCII filter from your working code
                # group_name = EMOJI_PATTERN.sub('', group_name)
                # group_name = ''.join(c for c in group_name if ord(c) < 128)
                # Preserve original name handling if not stripping emojis/non-ASCII
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
            if not logo_found and result["Status"] != "Expired/Revoked (Content)": # If not already marked as revoked
                result["Status"] = "Expired (No Logo)" # Or "Invalid", depending on strictness

    except requests.exceptions.Timeout: result["Status"] = "Network Error: Timeout"
    except requests.exceptions.ConnectionError: result["Status"] = "Network Error: Connection Failed"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: result["Status"] = f"Error ({type(e).__name__})"
    
    # Placeholder for relevancy logic (to be added later)
    # if result["Status"] == "Active" and associated_keyword and result["Group Name"] != "Unknown":
    #    result["Keyword Relevancy"] = get_relevancy(result["Group Name"], associated_keyword)
    return result

@st.cache_data(ttl=3600, show_spinner="⚙️ Scraping page for links...") # Added cache
def scrape_whatsapp_links_from_page(url: str, _status_container=None) -> list: # Renamed, added _status_container
    """Scrape WhatsApp group links from a single webpage."""
    if _status_container: _status_container.caption(f"📡 Requesting: {url[:70]}...")
    try:
        headers = {"User-Agent": DEFAULT_USER_AGENT}
        response = requests.get(url, headers=headers, timeout=10, allow_redirects=True) # Allow redirects
        response.raise_for_status() # Check for HTTP errors
        response.encoding = response.apparent_encoding or 'utf-8' # Better encoding detection
        if _status_container: _status_container.caption(f"📄 Parsing: {url[:70]}...")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set() # Use set for automatic deduplication from this page
        
        # Find in <a> tags more robustly
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if WHATSAPP_DOMAIN in href:
                match = re.search(WHATSAPP_LINK_REGEX, href)
                if match:
                    links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}") # Add standardized link
        
        # Find in text content
        # For text, consider using soup.get_text() for cleaner extraction if stripped_strings is problematic
        text_content = soup.get_text(separator=" ") # Get all text content, separated by space
        text_matches = re.finditer(WHATSAPP_LINK_REGEX, text_content)
        for match in text_matches:
            links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
        
        return list(links)
    except requests.exceptions.RequestException as e:
        msg = f"⚠️ Scrape Error ({url[:50]}): {type(e).__name__}"
        if _status_container: _status_container.warning(msg)
        else: st.caption(msg) # Fallback if no status container
    except Exception as e:
        msg = f"🚫 Other Error scraping {url[:50]}: {type(e).__name__}"
        if _status_container: _status_container.warning(msg)
        else: st.caption(msg)
    return []


@st.cache_data(ttl=1800, show_spinner=False) # Cache, spinner managed by st.status
def google_search_urls(query: str, num_results: int = 5, _status_container=None) -> list: # Renamed, added _status_container
    """Fetch URLs from Google's top N search results using googlesearch-python."""
    if _status_container: _status_container.write(f"🕵️ Google Search: '{query}' (top {num_results})...")
    try:
        # Added user_agent and slightly increased pause
        urls = list(search(query, num_results=num_results, lang="en", pause=2.0, user_agent=DEFAULT_USER_AGENT))
        if not urls:
            msg = f"ℹ️ No Google search results found for '{query}'. Try refining terms."
            if _status_container: _status_container.info(msg)
            else: st.warning(msg) # Use warning for no results
            return []
        if _status_container: _status_container.write(f"✅ Found {len(urls)} pages via Google for '{query}'.")
        return urls
    except Exception as e: # More specific error handling for Google
        error_msg = f"🚫 Google Search Error for '{query}': {type(e).__name__}."
        if "HTTP Error 429" in str(e) or "rate limit" in str(e).lower():
            error_msg += " Likely a rate limit from Google. Please wait or try fewer queries."
        if _status_container: _status_container.error(error_msg)
        else: st.error(error_msg)
        return []

def load_links_from_text_file(uploaded_file) -> list: # Specific for TXT
    """Load links from a TXT file, one link per line."""
    try:
        return [line.decode("utf-8", errors="replace").strip() for line in uploaded_file.readlines() if WHATSAPP_DOMAIN in line.decode("utf-8", errors="replace")]
    except Exception as e:
        st.error(f"Error reading TXT file: {e}")
        return []

def load_links_from_csv_file(uploaded_file) -> list: # Specific for CSV
    """Load links from the first column of a CSV file."""
    try:
        df_csv = pd.read_csv(uploaded_file, header=None) # Assume no header
        # Extract from all columns, then filter
        all_cells = []
        for col in df_csv.columns:
            all_cells.extend(df_csv[col].dropna().astype(str).tolist())
        return [link for link in all_cells if WHATSAPP_DOMAIN in link and re.search(WHATSAPP_LINK_REGEX, link)]
    except Exception as e:
        st.error(f"Error reading CSV file: {e}")
        return []

# NEW: Functions for Excel keyword loading and Website Crawling (adapted from ProMax)
def load_keywords_from_excel(uploaded_file) -> list:
    keywords = []
    try:
        df_excel = pd.read_excel(uploaded_file, header=None, sheet_name=0)
        keywords = [kw for kw in df_excel.iloc[:, 0].dropna().astype(str).str.strip().tolist() if kw]
        if not keywords: st.warning("No keywords found in the first column of the Excel file.")
    except Exception as e: st.error(f"Error reading Excel file '{uploaded_file.name}': {e}")
    return keywords

def crawl_website_for_links(base_url: str, max_pages_to_crawl: int = 5, status_container=None) -> list:
    """Crawls a website to find WhatsApp links, yields (page_url, list_of_wa_links)."""
    pages_with_links_data = []
    urls_to_visit = {base_url}
    visited_urls = set()
    processed_pages = 0

    parsed_base_url = urlparse(base_url)
    if not parsed_base_url.scheme: base_url = "http://" + base_url; parsed_base_url = urlparse(base_url)
    base_domain = parsed_base_url.netloc
    if not base_domain:
        if status_container: status_container.error(f"Invalid base URL for crawl: '{base_url}'")
        return []

    if status_container: status_container.write(f"🕷️ Starting crawl of '{base_domain}' (max {max_pages_to_crawl} pages)...")
    prog_bar = st.progress(0.0, text="Initializing website crawl...")

    while urls_to_visit and processed_pages < max_pages_to_crawl:
        current_url = urls_to_visit.pop()
        if current_url in visited_urls: continue
        
        visited_urls.add(current_url)
        processed_pages += 1
        prog_bar.progress(processed_pages / max_pages_to_crawl, text=f"Crawling: {processed_pages}/{max_pages_to_crawl} - {current_url[:60]}...")

        try:
            # Scrape current page for WA links
            links_on_this_page = scrape_whatsapp_links_from_page(current_url, None) # No nested status for this call
            if links_on_this_page:
                pages_with_links_data.append((current_url, links_on_this_page))

            # Find more internal links (same domain)
            headers = {"User-Agent": DEFAULT_USER_AGENT}
            response = requests.get(current_url, headers=headers, timeout=10, allow_redirects=True)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            for a_tag in soup.find_all('a', href=True):
                link_href = a_tag['href']
                abs_link = urljoin(current_url, link_href)
                parsed_abs_link = urlparse(abs_link)
                if parsed_abs_link.scheme in ['http', 'https'] and parsed_abs_link.netloc == base_domain:
                    if abs_link not in visited_urls and abs_link not in urls_to_visit:
                        if len(urls_to_visit) < (max_pages_to_crawl * 2 + 20): # Limit queue size
                            urls_to_visit.add(abs_link)
            time.sleep(0.25) # Be polite
        except Exception as e:
            if status_container: status_container.caption(f"⚠️ Crawl error on {current_url[:50]}: {type(e).__name__}")
    prog_bar.empty()
    if status_container: status_container.write(f"Crawling of '{base_domain}' finished. Found links on {len(pages_with_links_data)} pages.")
    return pages_with_links_data

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Validator & Scraper 🔗</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, validate, and manage WhatsApp group links effectively.</p>', unsafe_allow_html=True)

    # Initialize session state
    if 'results_df' not in st.session_state: st.session_state.results_df = pd.DataFrame()
    if 'process_clicked' not in st.session_state: st.session_state.process_clicked = False
    if 'last_input_method' not in st.session_state: st.session_state.last_input_method = None


    with st.sidebar:
        st.header("⚙️ Configuration")
        # NEW: Expanded input methods
        input_method_options = [
            "Google Search (Single Keyword)",
            "Google Search (Excel Keywords)",
            "Scrape Specific Webpage(s)",
            "Scrape Entire Website (Domain)",
            "Validate Manual Links",
            "Validate Links from File (TXT/CSV)"
        ]
        input_method = st.selectbox(
            "Input Method:", input_method_options, index=0, key="input_method_selector",
            help="Choose how to source or input WhatsApp links."
        )

        # Contextual settings based on input method
        top_n_google, max_crawl_pages_val, max_workers_val = 5, 5, 4 # Default values

        if "Google Search" in input_method:
            top_n_google = st.slider("Google Results/Query:", 1, 15, 5, key="google_results_slider", help="Number of Google search results to process per keyword.")
        elif "Entire Website" in input_method:
            max_crawl_pages_val = st.slider("Max Pages to Crawl:", 3, 15, 5, key="crawl_pages_slider", help="Maximum internal pages for domain crawl.")
        
        max_workers_val = st.slider("Validation Workers:", 1, 8, 4, key="workers_slider", help="Concurrent link validations (higher is faster but uses more resources).")
        st.caption("Tip: Start with lower limits for bulk operations to test quickly.")


    if st.sidebar.button("🗑️ Clear All Results & Cache", use_container_width=True, type="secondary", key="clear_all_btn"):
        st.cache_data.clear() # Clear Streamlit's data cache
        keys_to_del = [k for k in st.session_state.keys()] # Get all keys
        for key in keys_to_del:
            del st.session_state[key] # Delete all session state items
        st.success("✅ All results, session data, and app cache cleared! Refresh if needed."); st.experimental_rerun()

    st.markdown("---")
    
    # --- Input Area ---
    input_area = st.container()
    keyword_gs_single, excel_file, specific_urls_text, domain_to_crawl, manual_links_text, uploaded_file_links = \
        "", None, "", "", "", None # Initialize all input vars

    with input_area:
        action_button_label = "🚀 Process Links" # Default

        if input_method == "Google Search (Single Keyword)":
            keyword_gs_single = st.text_input("Enter Google Search Query:", key="gs_single_keyword_input", placeholder="e.g., technology enthusiasts whatsapp")
            if keyword_gs_single: action_button_label = f"🔍 Google '{keyword_gs_single[:20]}...'"
        elif input_method == "Google Search (Excel Keywords)":
            excel_file = st.file_uploader("Upload Excel with Keywords (1st column):", type=["xlsx", "xls"], key="excel_keywords_uploader")
            if excel_file: action_button_label = f"📊 Process Excel '{excel_file.name}'"
        elif input_method == "Scrape Specific Webpage(s)":
            specific_urls_text = st.text_area("Enter Webpage URLs (one per line):", height=120, key="specific_urls_input", placeholder="https://example.com/links\nhttps://blog.com/whatsapp-groups")
            if specific_urls_text: action_button_label = "📄 Scrape Provided URLs"
        elif input_method == "Scrape Entire Website (Domain)":
            domain_to_crawl = st.text_input("Enter Base Domain URL to Crawl:", key="domain_crawl_input", placeholder="https://mycommunitysite.com")
            if domain_to_crawl: action_button_label = f"🕸️ Crawl '{urlparse(domain_to_crawl).netloc or 'Domain'}'"
        elif input_method == "Validate Manual Links":
            manual_links_text = st.text_area("Enter WhatsApp Links (one per line):", height=150, key="manual_links_input", placeholder=f"{WHATSAPP_DOMAIN}ABC123XYZ...")
            if manual_links_text: action_button_label = "✍️ Validate Manual Links"
        elif input_method == "Validate Links from File (TXT/CSV)":
            uploaded_file_links = st.file_uploader("Upload TXT or CSV File with Links:", type=["txt", "csv"], key="file_links_uploader")
            if uploaded_file_links: action_button_label = f"📤 Validate File '{uploaded_file_links.name}'"

        # --- Main Processing Button ---
        if st.button(action_button_label, use_container_width=True, type="primary", key="main_process_button"):
            st.session_state.process_clicked = True
            st.session_state.last_input_method = input_method # For ZIP download logic
            
            all_results_list = [] # Stores dicts from validate_link, plus Source and Relevancy Keyword
            
            # Using st.status for detailed progress
            with st.status(f"🚀 Starting: {input_method}", expanded=True) as status_main:
                try:
                    links_to_validate_tuples = [] # List of (link_url, source_identifier, keyword_for_relevancy)
                    
                    # --- Phase 1: Link Collection (Populate links_to_validate_tuples) ---
                    status_main.update(label="🔗 Collecting links...", state="running")
                    if input_method == "Google Search (Single Keyword)":
                        if not keyword_gs_single: status_main.update(label="⚠️ Query missing.", state="error"); return
                        google_page_urls = google_search_urls(keyword_gs_single, top_n_google, status_main)
                        for g_url in google_page_urls:
                            scraped_wa_links = scrape_whatsapp_links_from_page(g_url, status_main)
                            for wa_link in scraped_wa_links: links_to_validate_tuples.append((wa_link, keyword_gs_single, keyword_gs_single))
                    
                    elif input_method == "Google Search (Excel Keywords)":
                        if not excel_file: status_main.update(label="⚠️ Excel file missing.", state="error"); return
                        keywords_from_excel = load_keywords_from_excel(excel_file)
                        if not keywords_from_excel: status_main.update(label="⚠️ No keywords in Excel.", state="error"); return
                        
                        prog_excel = st.progress(0.0, text=f"Processing 0/{len(keywords_from_excel)} keywords...")
                        for i, kw_excel in enumerate(keywords_from_excel):
                            status_main.write(f"Excel Keyword {i+1}/{len(keywords_from_excel)}: '{kw_excel}'")
                            google_page_urls_kw = google_search_urls(kw_excel, top_n_google, status_main)
                            for g_url_kw in google_page_urls_kw:
                                scraped_wa_links_kw = scrape_whatsapp_links_from_page(g_url_kw, status_main)
                                for wa_link_kw in scraped_wa_links_kw: links_to_validate_tuples.append((wa_link_kw, kw_excel, kw_excel))
                            prog_excel.progress((i+1)/len(keywords_from_excel), text=f"Processed {i+1}/{len(keywords_from_excel)} keywords...")
                        prog_excel.empty()

                    elif input_method == "Scrape Specific Webpage(s)":
                        if not specific_urls_text: status_main.update(label="⚠️ URLs missing.", state="error"); return
                        urls_list = [u.strip() for u in specific_urls_text.split('\n') if u.strip()]
                        if not urls_list: status_main.update(label="⚠️ No valid URLs entered.", state="error"); return
                        prog_specific = st.progress(0.0, text=f"Scraping 0/{len(urls_list)} pages...")
                        for i, spec_url in enumerate(urls_list):
                            status_main.write(f"Scraping page {i+1}/{len(urls_list)}: {spec_url[:60]}...")
                            scraped_wa_links_spec = scrape_whatsapp_links_from_page(spec_url, status_main)
                            for wa_link_spec in scraped_wa_links_spec: links_to_validate_tuples.append((wa_link_spec, spec_url, None))
                            prog_specific.progress((i+1)/len(urls_list), text=f"Scraped {i+1}/{len(urls_list)} pages...")
                        prog_specific.empty()

                    elif input_method == "Scrape Entire Website (Domain)":
                        if not domain_to_crawl: status_main.update(label="⚠️ Domain URL missing.", state="error"); return
                        crawled_pages_with_links = crawl_website_for_links(domain_to_crawl, max_crawl_pages_val, status_main)
                        for page_url_crawled, links_on_page_crawled in crawled_pages_with_links:
                            for wa_link_crawled in links_on_page_crawled: links_to_validate_tuples.append((wa_link_crawled, page_url_crawled, None))
                    
                    elif input_method == "Validate Manual Links":
                        if not manual_links_text: status_main.update(label="⚠️ No manual links.", state="error"); return
                        manual_links_list = [l.strip() for l in manual_links_text.split('\n') if l.strip().startswith(WHATSAPP_DOMAIN)]
                        for man_link in manual_links_list: links_to_validate_tuples.append((man_link, "Manual Entry", None))

                    elif input_method == "Validate Links from File (TXT/CSV)":
                        if not uploaded_file_links: status_main.update(label="⚠️ File not uploaded.", state="error"); return
                        file_links_list = []
                        if uploaded_file_links.name.endswith('.csv'):
                            file_links_list = load_links_from_csv_file(uploaded_file_links)
                        else: # Assume TXT
                            file_links_list = load_links_from_text_file(uploaded_file_links)
                        source_file_name = f"File: {uploaded_file_links.name}"
                        for file_link in file_links_list: links_to_validate_tuples.append((file_link, source_file_name, None))

                    # Deduplicate links before validation, keeping the first encountered source/keyword for that link
                    unique_links_to_process_map = {}
                    for link_val, source_val, keyword_val in links_to_validate_tuples:
                        if link_val not in unique_links_to_process_map:
                             unique_links_to_process_map[link_val] = (link_val, source_val, keyword_val)
                    
                    final_items_to_validate = list(unique_links_to_process_map.values())

                    if not final_items_to_validate:
                        status_main.info("ℹ️ No unique WhatsApp links collected to validate.");
                        st.session_state.results_df = pd.DataFrame(); status_main.update(label="🏁 No links found.", state="complete"); return

                    # --- Phase 2: Validation ---
                    status_main.update(label=f"🛡️ Validating {len(final_items_to_validate)} unique links...", state="running")
                    prog_validate = st.progress(0.0, text=f"Validating 0/{len(final_items_to_validate)}...")
                    
                    with ThreadPoolExecutor(max_workers=max_workers_val) as executor:
                        future_to_item = {
                            executor.submit(validate_link, item_tuple[0], item_tuple[2]): item_tuple # item_tuple[2] is relevancy_keyword
                            for item_tuple in final_items_to_validate
                        }
                        for i, future in enumerate(as_completed(future_to_item)):
                            original_item = future_to_item[future]
                            link_url_orig, source_id_orig, _ = original_item
                            try:
                                validated_result_dict = future.result()
                                all_results_list.append({**validated_result_dict, "Source": source_id_orig}) # Add source
                            except Exception as e_val_item:
                                all_results_list.append({ # Log error for this specific link validation
                                    "Group Name": "Validation Failed", "Group Link": link_url_orig, 
                                    "Logo URL": "", "Status": f"Validation Crash: {type(e_val_item).__name__}",
                                    "Keyword Relevancy": "N/A", "Source": source_id_orig
                                })
                            prog_validate.progress((i+1)/len(final_items_to_validate), text=f"Validated {i+1}/{len(final_items_to_validate)}...")
                    prog_validate.empty()
                    
                    st.session_state.results_df = pd.DataFrame(all_results_list) if all_results_list else pd.DataFrame()
                    status_main.update(label="🎉 All Processing Complete!", state="complete")

                except Exception as e_main_block:
                    status_main.error(f"🚫 Critical Error during processing: {type(e_main_block).__name__} - {e_main_block}")
                    st.session_state.results_df = pd.DataFrame() # Ensure results are cleared on critical failure

    # --- Display Results Area ---
    if st.session_state.process_clicked:
        if not st.session_state.results_df.empty:
            df_display = st.session_state.results_df
            st.markdown("---"); st.subheader("📊 Results Dashboard")
            
            active_df = df_display[df_display['Status'] == 'Active'].copy()
            expired_df = df_display[df_display['Status'].str.contains("Expired|Revoked", case=False, na=False)].copy()
            error_df = df_display[~df_display.index.isin(active_df.index) & ~df_display.index.isin(expired_df.index)].copy()

            col1, col2, col3, col4 = st.columns(4)
            with col1: st.markdown(f'<div class="metric-card">Total Links<br><b>{len(df_display)}</b></div>', unsafe_allow_html=True)
            with col2: st.markdown(f'<div class="metric-card">✅ Active<br><b>{len(active_df)}</b></div>', unsafe_allow_html=True)
            with col3: st.markdown(f'<div class="metric-card">⚠️ Expired<br><b>{len(expired_df)}</b></div>', unsafe_allow_html=True)
            with col4: st.markdown(f'<div class="metric-card">❌ Errors<br><b>{len(error_df)}</b></div>', unsafe_allow_html=True)

            with st.expander("🔎 View, Filter & Download Detailed Results", expanded=True):
                # Ensure necessary columns exist for filtering and display
                if "Keyword Relevancy" not in df_display.columns: df_display["Keyword Relevancy"] = "N/A"
                if "Source" not in df_display.columns: df_display["Source"] = "Unknown"

                # Filters
                status_options = ["All"] + sorted(df_display['Status'].dropna().unique().tolist())
                sel_status_filter = st.selectbox("Filter Status:", status_options, key="filter_status_main_v2")
                
                relevancy_options_filter = ["All"] + sorted(df_display["Keyword Relevancy"].dropna().unique().tolist())
                sel_relevancy_filter = st.selectbox("Filter Relevancy:", relevancy_options_filter, key="filter_relevancy_main_v2")

                name_search_filter = st.text_input("Filter Group Name (contains):", key="filter_name_main_v2")
                source_search_filter = st.text_input("Filter Source (contains):", key="filter_source_main_v2")

                # Apply filters
                filtered_df_for_display = df_display.copy()
                if sel_status_filter != "All": filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Status'] == sel_status_filter]
                if sel_relevancy_filter != "All": filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Keyword Relevancy'] == sel_relevancy_filter]
                if name_search_filter: filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Group Name'].str.contains(name_search_filter, case=False, na=False)]
                if source_search_filter: filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Source'].str.contains(source_search_filter, case=False, na=False)]
                
                display_cols_ordered = ["Group Name", "Group Link", "Status", "Keyword Relevancy", "Source", "Logo URL"]
                actual_cols_to_display = [col for col in display_cols_ordered if col in filtered_df_for_display.columns]

                st.dataframe(
                    filtered_df_for_display[actual_cols_to_display],
                    column_config={
                        "Group Name": st.column_config.TextColumn(width="medium"),
                        "Group Link": st.column_config.LinkColumn(display_text="🔗 Join", width="medium"),
                        "Status": st.column_config.TextColumn(width="small"),
                        "Keyword Relevancy": st.column_config.TextColumn("Relevancy", width="small"),
                        "Source": st.column_config.TextColumn("Source Origin", width="medium"),
                        "Logo URL": st.column_config.ImageColumn("Logo", width="small")
                    },
                    height=500, use_container_width=True, hide_index=True
                )
                if not filtered_df_for_display.empty:
                    st.download_button("📥 Download Filtered View (CSV)", filtered_df_for_display.to_csv(index=False).encode('utf-8'), "filtered_groups.csv", "text/csv", use_container_width=True, key="dl_filtered_main_v2")
                else: st.caption("No data matches current filters for download.")

            # --- Per-Source ZIP Download ---
            # (Logic for ZIP download, ensure it uses active_df for source-specific files)
            last_method = st.session_state.get('last_input_method', None)
            if last_method in ["Google Search (Excel Keywords)", "Scrape Entire Website (Domain)"]:
                active_for_zip = df_display[df_display['Status'] == 'Active'].copy() # Use the main filtered df's active portion
                if not active_for_zip.empty and 'Source' in active_for_zip.columns:
                    # ... (ZIP creation logic from ProMax version, using active_for_zip) ...
                    pass # Placeholder for brevity

            # --- Overall Markdown Export ---
            # (Logic for overall Markdown, ensure it uses active_df)
            if not active_df.empty:
                # ... (Markdown generation and download from ProMax version, using active_df) ...
                pass # Placeholder for brevity

        elif st.session_state.process_clicked: # Processed, but results_df is empty
            st.info("🏁 Processing complete. No WhatsApp links were found or validated.", icon="🤷")
    else: # Before any processing attempt
         st.info("✨ Welcome! Select an input method and click 'Process Links' to begin.", icon="👋")

if __name__ == "__main__":
    main()
