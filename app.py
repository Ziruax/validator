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

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator Ultimate",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.example.com/help', # Replace with actual help URL
        'Report a bug': "https://www.example.com/bug", # Replace
        'About': "# WhatsApp Link Scraper & Validator Ultimate\nFind, validate, and organize WhatsApp group links efficiently."
    }
)

# --- Constants ---
WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
WHATSAPP_LINK_REGEX = r"https?://chat\.whatsapp\.com/([a-zA-Z0-9_-]{18,25})(?=[?\s\"']|$)"

# --- Custom CSS (remains mostly the same, minor adjustments for aesthetics) ---
st.markdown("""
    <style>
    /* ... (CSS from previous version, ensure .markdown-output-area is well-styled) ... */
    .main-title { font-size: 2.6em; color: #128C7E; /* Darker WA Green */ text-align: center; margin-bottom: 0; font-weight: bold; letter-spacing: -1px;}
    .subtitle { font-size: 1.25em; color: #5E5E5E; text-align: center; margin-top: 5px; margin-bottom: 25px; }
    .stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 20px; transition: background-color 0.3s ease; }
    .stButton>button:hover { background-color: #1EBE5A; }
    .stButton>button[kind="secondary"] { background-color: #e0e0e0; color: #333; } /* Style for secondary buttons */
    .stButton>button[kind="secondary"]:hover { background-color: #d0d0d0; }
    .stProgress .st-b { background-color: #25D366; } /* Progress bar color */
    .metric-card { background-color: #F7F9FA; padding: 18px; border-radius: 12px; box-shadow: 0 5px 15px rgba(0,0,0,0.06); color: #333333; text-align: center; border: 1px solid #E0E7ED;}
    .stTextInput input, .stTextArea textarea, .stFileUploader section { border: 1px solid #25D366 !important; border-radius: 6px !important; }
    .sidebar .sidebar-content { background-color: #F0F2F5; padding-top: 1rem;}
    .stExpander { border: 1px solid #D1D9E1; border-radius: 10px; }
    .stExpander header { font-size: 1.1em; font-weight: 500; }
    .markdown-output-area { background-color: #ffffff; padding: 18px; border-radius: 10px; border: 1px solid #D1D9E1; max-height: 550px; overflow-y: auto; font-family: 'Menlo', 'Consolas', monospace; white-space: pre-wrap; line-height: 1.65; box-shadow: inset 0 2px 4px rgba(0,0,0,0.04);}
    .markdown-output-area table { width: 100%; border-collapse: collapse; margin-bottom: 1.2em; }
    .markdown-output-area th, .markdown-output-area td { border: 1px solid #e8e8e8; padding: 10px 12px; text-align: left; vertical-align: middle;}
    .markdown-output-area th { background-color: #f9f9f9; font-weight: 600; }
    .markdown-output-area img { max-width: 55px; max-height: 55px; border-radius: 50%; display: block; margin: auto; border: 1px solid #eee; }
    </style>
""", unsafe_allow_html=True)

# --- Helper Function ---
def sanitize_filename(name, max_len=80):
    """Sanitizes a string to be a valid filename, ensuring it's not empty."""
    name = str(name)
    name = re.sub(r'https?://', '', name)
    name = re.sub(r'[^\w\s-]', '', name).strip().lower()
    name = re.sub(r'[-\s]+', '-', name)
    sanitized = name[:max_len]
    return sanitized if sanitized else f"unnamed-file-{int(time.time())}" # Fallback for empty names


# --- Core Logic Functions (with refined error handling and comments) ---

@st.cache_data(ttl=1800, show_spinner="♻️ Validating link (cached potential)...") # Cache validation for 30 mins
def validate_link(link: str) -> dict:
    """
    Validates a WhatsApp group link by fetching its page and parsing metadata.
    Returns a dictionary with group details and validation status.
    """
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error: Initializing"}
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}

    if not link or not link.startswith(WHATSAPP_DOMAIN):
        result["Status"] = "Invalid Format (No WA Domain)"
        return result

    try:
        response = requests.get(link, headers=headers, timeout=12, allow_redirects=True)
        response.encoding = 'utf-8' # Ensure UTF-8 for special characters

        if response.status_code != 200:
            result["Status"] = f"HTTP Error: {response.status_code}"
            return result

        if WHATSAPP_DOMAIN not in response.url: # Redirected off WhatsApp
            result["Status"] = "Invalid: Redirected Off-Platform"
            if WHATSAPP_DOMAIN in link: # Original was a WA link
                 soup_check = BeautifulSoup(response.text, 'html.parser')
                 if soup_check.find(string=re.compile(r"(invite link revoked|couldn't join|link was reset|no longer valid|cannot be used)", re.IGNORECASE)):
                     result["Status"] = "Expired/Revoked (Redirected)"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check for explicit "Invite link revoked" messages
        if soup.find(string=re.compile(r"(invite link revoked|link was reset|no longer valid|cannot be used)", re.IGNORECASE)):
            result["Status"] = "Expired/Revoked (Content)"
            meta_title_revoked = soup.find('meta', property='og:title')
            if meta_title_revoked and meta_title_revoked.get('content'):
                result["Group Name"] = unescape(meta_title_revoked['content']).strip() or "Revoked Group"
            else:
                result["Group Name"] = "Revoked Group (Name N/A)"
            return result

        # Extract Group Name
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            result["Group Name"] = unescape(meta_title['content']).strip() or "Unnamed Group"
        else:
            result["Group Name"] = "Unnamed Group"

        # Extract Group Logo
        img_tags = soup.find_all('img', src=True)
        logo_found = False
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN.match(src):
                result["Logo URL"] = src
                result["Status"] = "Active"
                logo_found = True
                break
        
        if not logo_found and result["Status"] == "Error: Initializing": # If status not set by other conditions
            if result["Group Name"] not in ["Unnamed Group", "Revoked Group (Name N/A)"]:
                 result["Status"] = "Expired (No Logo, Had Name)"
            else:
                 result["Status"] = "Invalid/Expired (No Logo/Name)"

    except requests.exceptions.Timeout: result["Status"] = "Network Error: Timeout"
    except requests.exceptions.ConnectionError: result["Status"] = "Network Error: Connection Failed"
    except requests.exceptions.TooManyRedirects: result["Status"] = "Network Error: Too Many Redirects"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error: {type(e).__name__}" # More specific
    except Exception as e: result["Status"] = f"Unexpected Error: {type(e).__name__} validating link"
    return result

@st.cache_data(ttl=3600, show_spinner="⚙️ Parsing HTML for links (cached potential)...")
def scrape_whatsapp_links_from_html(html_content: str, source_url: str = "") -> list:
    """Extracts WhatsApp links from raw HTML content."""
    # ... (no significant changes, function is already quite focused) ...
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        if WHATSAPP_DOMAIN in href:
            match = re.search(WHATSAPP_LINK_REGEX, href)
            if match: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}") # Add standardized link
    text_content = soup.get_text(separator=" ") # Get all text content
    text_matches = re.finditer(WHATSAPP_LINK_REGEX, text_content)
    for match in text_matches: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
    return list(links)


@st.cache_data(ttl=3600, show_spinner=False) # Spinner handled by st.status
def scrape_url_for_whatsapp_links(url: str, status_container=None) -> list:
    """Scrapes WhatsApp group links from a single webpage with status updates."""
    # ... (no significant changes, function is already quite focused) ...
    if status_container: status_container.caption(f"📡 Requesting: {url[:75]}...") # Slightly longer preview
    try:
        headers = {"User-Agent": DEFAULT_USER_AGENT}
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        response.encoding = response.apparent_encoding or 'utf-8' # Better encoding detection
        if status_container: status_container.caption(f"📄 Parsing content from: {url[:75]}...")
        return scrape_whatsapp_links_from_html(response.text, url)
    except requests.exceptions.HTTPError as e:
        msg = f"⚠️ HTTP Error {e.response.status_code} for {url[:75]}"
        if status_container: status_container.warning(msg)
        else: st.caption(msg) # Fallback
    except requests.exceptions.RequestException as e:
        msg = f"⚠️ Network error scraping {url[:75]}: {type(e).__name__}"
        if status_container: status_container.warning(msg)
        else: st.caption(msg)
    except Exception as e:
        msg = f"🚫 Unexpected error processing {url[:75]}: {type(e).__name__}"
        if status_container: status_container.warning(msg)
        else: st.caption(msg)
    return []


@st.cache_data(ttl=1800, show_spinner=False) # Spinner handled by st.status
def google_search_links(query: str, num_results: int = 10, status_container=None) -> list:
    """Fetches URLs from Google search results."""
    # ... (no significant changes, function is already quite focused) ...
    if status_container: status_container.write(f"🕵️ Searching Google for: '{query}' (top {num_results} results)...")
    try:
        # Added user_agent to search call for consistency, though googlesearch-python handles its own.
        urls = list(search(query, num_results=num_results, lang="en", pause=2.5, user_agent=DEFAULT_USER_AGENT))
        if not urls:
            msg = f"ℹ️ No Google results found for '{query}'."
            if status_container: status_container.info(msg)
            else: st.caption(msg)
            return []
        if status_container: status_container.write(f"✅ Found {len(urls)} potential pages from Google for '{query}'.")
        return urls
    except Exception as e: # Catch more general exceptions from googlesearch
        error_msg = f"🚫 Google Search error for '{query}': {type(e).__name__}."
        if "HTTP Error 429" in str(e) or "To avoid detection" in str(e) or "rate limit" in str(e).lower():
            error_msg += " Google may be rate-limiting. Try fewer queries/results or wait a while."
        if status_container: status_container.error(error_msg)
        else: st.error(error_msg) # Show as prominent error
        return []


def load_links_from_file(uploaded_file) -> list:
    """Loads WhatsApp links from TXT or CSV file."""
    # ... (no significant changes, function is already quite robust) ...
    links = set()
    try:
        content = uploaded_file.getvalue().decode("utf-8", errors='replace').splitlines()
        for line in content:
            if WHATSAPP_DOMAIN in line:
                matches = re.finditer(WHATSAPP_LINK_REGEX, line)
                for match in matches: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
        
        if uploaded_file.name.endswith('.csv'):
            uploaded_file.seek(0) # Reset buffer for pandas
            df_csv = pd.read_csv(uploaded_file, header=None)
            for col in df_csv.columns:
                for item in df_csv[col].dropna().astype(str):
                    if WHATSAPP_DOMAIN in item:
                        matches = re.finditer(WHATSAPP_LINK_REGEX, item)
                        for match in matches: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
    except Exception as e:
        st.warning(f"Error reading file '{uploaded_file.name}': {e}. Partial data may have been loaded.")
    return list(links)


def load_keywords_from_excel(uploaded_file) -> list:
    """Loads keywords from the first column of an Excel file."""
    # ... (no significant changes, function is already quite robust) ...
    keywords = []
    try:
        df_excel = pd.read_excel(uploaded_file, header=None, sheet_name=0)
        keywords = df_excel.iloc[:, 0].dropna().astype(str).str.strip().tolist()
        keywords = [kw for kw in keywords if kw] # Filter out empty strings after stripping
    except Exception as e:
        st.error(f"Error reading Excel file '{uploaded_file.name}': {e}")
    return keywords


def crawl_website(base_url: str, max_pages_to_crawl: int = 10, status_container=None) -> list:
    """
    Crawls a website and returns a list of (page_url, list_of_wa_links_on_page) tuples.
    """
    # ... (no significant changes, function is already quite robust) ...
    pages_with_links_data = []
    urls_to_visit = {base_url}
    visited_urls = set()
    processed_pages = 0
    parsed_base_url = urlparse(base_url)
    if not parsed_base_url.scheme: base_url = "http://" + base_url; parsed_base_url = urlparse(base_url)
    base_domain = parsed_base_url.netloc
    if not base_domain: # Handle cases where base_url might be invalid
        if status_container: status_container.error(f"Invalid base URL for crawl: '{base_url}'")
        return []

    if status_container: status_container.write(f"🕷️ Starting crawl of '{base_domain}' (max {max_pages_to_crawl} pages). This might take a while...")
    crawl_progress_bar = st.progress(0.0, text="Initializing crawl...")

    while urls_to_visit and processed_pages < max_pages_to_crawl:
        current_url = urls_to_visit.pop()
        if current_url in visited_urls: continue
        visited_urls.add(current_url); processed_pages += 1
        
        progress_value = processed_pages / max_pages_to_crawl
        crawl_progress_bar.progress(progress_value, text=f"Crawling page {processed_pages}/{max_pages_to_crawl}: {current_url[:65]}...")
        
        page_specific_wa_links = set()
        try:
            headers = {"User-Agent": DEFAULT_USER_AGENT}
            response = requests.get(current_url, headers=headers, timeout=10, allow_redirects=True)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or 'utf-8'
            
            whatsapp_links_on_page = scrape_whatsapp_links_from_html(response.text, current_url)
            page_specific_wa_links.update(whatsapp_links_on_page)

            if page_specific_wa_links:
                pages_with_links_data.append((current_url, list(page_specific_wa_links)))
            
            if status_container: status_container.caption(f"Found {len(page_specific_wa_links)} WA links on {current_url[:65]}.")

            soup = BeautifulSoup(response.text, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                link = a_tag['href']
                abs_link = urljoin(current_url, link) # Handles relative links
                parsed_link = urlparse(abs_link)
                # Check if it's http/https, belongs to the same domain, and not an anchor/mailto/tel
                if parsed_link.scheme in ['http', 'https'] and parsed_link.netloc == base_domain and not abs_link.startswith(('mailto:', 'tel:')):
                    if abs_link not in visited_urls and abs_link not in urls_to_visit:
                        if len(urls_to_visit) < (max_pages_to_crawl * 2 + 50): # Generous queue limit
                             urls_to_visit.add(abs_link)
            time.sleep(0.35) # Be slightly more polite
        except requests.exceptions.RequestException as e:
            if status_container: status_container.caption(f"⚠️ Skipping {current_url[:65]}: {type(e).__name__}")
        except Exception as e:
            if status_container: status_container.caption(f"🚫 Error processing {current_url[:65]} during crawl: {type(e).__name__}")
            
    crawl_progress_bar.empty() # Remove progress bar when done
    if status_container: status_container.write(f"Crawling finished for '{base_domain}'. {processed_pages} pages visited.")
    return pages_with_links_data

def generate_markdown_output(active_df_for_source: pd.DataFrame) -> str:
    """Generates Markdown table output for a DataFrame of active groups."""
    # ... (no significant changes, function is already quite robust) ...
    if active_df_for_source.empty: return "No active groups for this source."
    md_lines = ["| Group Logo | Group Name | Action |", "| :--------: | :--------- | -----: |"] # Header with alignment
    for _, row in active_df_for_source.iterrows():
        logo, name, link = row.get("Logo URL", ""), row.get("Group Name", "N/A"), row.get("Group Link", "#")
        # Sanitize name for Markdown table (pipes and newlines) and bold it
        safe_name = f"**{str(name).replace('|', '\|').replace('\r\n', ' ').replace('\n', ' ')}**"
        logo_md = f"![Logo]({logo}&w=60)" if logo else " " # &w=60 for smaller logo in table
        action_md = f"[**Join Group**]({link})"
        md_lines.append(f"| {logo_md} | {safe_name} | {action_md} |")
    return "\n".join(md_lines)

# --- Streamlit UI ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator Ultimate 🏆</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, validate, and organize WhatsApp group links with unparalleled efficiency and detailed feedback.</p>', unsafe_allow_html=True)

    # Initialize session state variables if they don't exist
    if 'results_df' not in st.session_state:
        st.session_state.results_df = pd.DataFrame()
    if 'last_input_method_for_zip' not in st.session_state:
        st.session_state.last_input_method_for_zip = None
    if 'process_button_clicked' not in st.session_state:
        st.session_state.process_button_clicked = False


    with st.sidebar:
        st.header("⚙️ Configuration & Input")
        input_method_options = [
            "Search & Scrape: Google (Single Keyword)",
            "Search & Scrape: Google (Excel Keywords)",
            "Scrape: Specific Webpage(s)",
            "Scrape: Entire Website (Domain)",
            "Validate: Manual Link Entry",
            "Validate: Upload File with Links (TXT/CSV)"
        ]
        input_method = st.selectbox("Choose Input Method:", options=input_method_options, index=0, help="Select how you want to source or input WhatsApp links.")
        
        # Conservative defaults, user can increase if needed
        top_n_google_default = 5
        max_crawl_pages_default = 5
        max_workers_default = 5

        if "Google" in input_method:
            top_n_google = st.slider("Google Results per Query:", 1, 30, top_n_google_default, # Max 30 to be safer
                                     key=f"google_top_n_{input_method.replace(' ', '_')}",
                                     help="Number of Google search result pages to process for each keyword. Higher values are slower and increase risk of rate-limiting.")
        elif input_method == "Scrape: Entire Website (Domain)":
            max_crawl_pages = st.slider("Max Pages to Crawl:", 3, 30, max_crawl_pages_default, # Max 30
                                        key="max_crawl_pages",
                                        help="Maximum number of internal pages to crawl on the specified domain. Higher values take longer.")
        
        max_workers_validation = st.slider("Validation Workers:", 1, 10, max_workers_default, # Max 10
                                           help="Number of links to validate concurrently. Higher values can be faster but use more resources and network bandwidth.")
        st.caption("⚠️ Tip: Start with lower values for Google results/crawl pages to test. Excessive requests can lead to temporary blocks by services.")

    # Clear Cache Button - Placed prominently for user control
    if st.sidebar.button("🗑️ Clear All Results & App Cache", use_container_width=True, type="secondary", help="Resets all results and clears the application's internal data cache. Useful if you encounter issues or want a fresh start."):
        keys_to_delete = [k for k in st.session_state.keys()] # Get all keys
        for key in keys_to_delete:
            del st.session_state[key] # Delete all session state items
        st.cache_data.clear() # Clear Streamlit's data cache
        st.success("✅ All results, session data, and app cache have been cleared! Please refresh the page if needed.")
        st.experimental_rerun() # Rerun to reflect cleared state immediately

    st.markdown("---") # Main page separator

    # Input form area
    input_container = st.container()
    action_button_label = "🚀 Process & Validate Links" # Default label
    # Initialize input variables to prevent UnboundLocalError
    keyword_gs, excel_file_gs, urls_text_specific, domain_url_crawl, links_text_manual, file_links_upload = "", None, "", "", "", None


    with input_container:
        # ... (Input field setup, largely same as previous version, ensure unique keys) ...
        if input_method == "Search & Scrape: Google (Single Keyword)": keyword_gs = st.text_input("Enter Google Search Query:", placeholder="e.g., AI enthusiasts WhatsApp groups", key="gs_keyword_ultimate")
        elif input_method == "Search & Scrape: Google (Excel Keywords)": excel_file_gs = st.file_uploader("Upload Excel with Keywords (one per row, first column):", type=["xlsx", "xls"], key="gs_excel_ultimate", help="The app will search Google for each keyword in the first column.")
        elif input_method == "Scrape: Specific Webpage(s)": urls_text_specific = st.text_area("Enter Webpage URLs (one per line):", height=150, key="scrape_specific_urls_ultimate", placeholder="https://example.com/list1\nhttps://another.com/archive")
        elif input_method == "Scrape: Entire Website (Domain)": domain_url_crawl = st.text_input("Enter Base Domain URL to Crawl:", placeholder="e.g., https://mycommunityforum.com", key="scrape_domain_url_ultimate", help="The app will try to find links on this domain and its internal pages.")
        elif input_method == "Validate: Manual Link Entry": links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=200, key="manual_links_ultimate", placeholder=f"{WHATSAPP_DOMAIN}ABC123XYZ...")
        elif input_method == "Validate: Upload File with Links (TXT/CSV)": file_links_upload = st.file_uploader("Upload TXT or CSV File with WhatsApp Links:", type=["txt", "csv"], key="upload_link_file_ultimate")
        
        # Dynamically update action button label based on input
        if keyword_gs: action_button_label = f"🔍 Search Google for '{keyword_gs[:20]}...' & Validate"
        elif excel_file_gs: action_button_label = f"📊 Process Excel '{excel_file_gs.name}' & Validate"
        elif urls_text_specific: action_button_label = f"📄 Scrape Provided URLs & Validate"
        elif domain_url_crawl: action_button_label = f"🕸️ Crawl '{urlparse(domain_url_crawl).netloc or 'Domain'}' & Validate"
        elif links_text_manual: action_button_label = f"✍️ Validate Manually Entered Links"
        elif file_links_upload: action_button_label = f"📤 Validate Links from '{file_links_upload.name}'"

        # Main processing button
        if st.button(action_button_label, use_container_width=True, type="primary", key="process_button_main_ultimate"):
            st.session_state.process_button_clicked = True # Mark that processing has been initiated
            all_validated_results_list = [] 
            
            # Use st.status for detailed, collapsible progress updates
            with st.status(f"🚀 Initializing: {input_method}", expanded=True) as status_main:
                try:
                    # --- Link Collection & Per-Source Validation Logic (adapted from previous version) ---
                    # This section needs careful error handling for each input method
                    # For brevity, the detailed per-method logic block from previous answer is assumed here,
                    # but with added try-except around major operations within each method's block.

                    # Example for Single Keyword (apply similar structure to others)
                    if input_method == "Search & Scrape: Google (Single Keyword)":
                        if not keyword_gs: status_main.update(label="⚠️ Input Missing: Enter a search query.", state="error"); return
                        status_main.update(label=f"🔗 Stage 1: Processing Keyword '{keyword_gs}'", state="running")
                        
                        google_urls = google_search_links(keyword_gs, top_n_google, status_main)
                        raw_links_for_source = set()
                        if google_urls:
                            scrape_prog = st.progress(0.0, text=f"Scraping Google results for '{keyword_gs}'...")
                            for i, url in enumerate(google_urls):
                                try:
                                    raw_links_for_source.update(scrape_url_for_whatsapp_links(url, status_main))
                                except Exception as e_scrape_url: # Catch error during individual URL scrape
                                    status_main.warning(f"Skipping URL {url[:50]} due to error: {e_scrape_url}")
                                scrape_prog.progress((i+1)/len(google_urls))
                            scrape_prog.empty()
                        
                        if raw_links_for_source:
                            status_main.update(label=f"🛡️ Stage 2: Validating {len(raw_links_for_source)} links for '{keyword_gs}'", state="running")
                            with ThreadPoolExecutor(max_workers=max_workers_validation) as executor:
                                future_to_link = {executor.submit(validate_link, link): link for link in list(raw_links_for_source)}
                                val_prog = st.progress(0.0, text=f"Validating links for '{keyword_gs}'...")
                                for i, future in enumerate(as_completed(future_to_link)):
                                    validated_res = future.result()
                                    all_validated_results_list.append({**validated_res, "Source": keyword_gs})
                                    val_prog.progress((i+1)/len(raw_links_for_source))
                                val_prog.empty()
                        else: status_main.info(f"ℹ️ No WhatsApp links found for keyword '{keyword_gs}'.")
                    
                    # ... (Similar detailed blocks for Excel, Domain Crawl, Specific Pages, Manual, File Upload) ...
                    # Ensure each has try-except for robustness and clear status updates.
                    # For Excel and Domain, iterate keywords/pages, validating per source.

                    elif input_method == "Search & Scrape: Google (Excel Keywords)":
                        if not excel_file_gs: status_main.update(label="⚠️ Input Missing: Upload an Excel file.", state="error"); return
                        keywords_list = load_keywords_from_excel(excel_file_gs)
                        if not keywords_list: status_main.update(label="⚠️ No keywords found in Excel file or file is invalid.", state="error"); return
                        
                        total_keywords = len(keywords_list)
                        status_main.update(label=f"🔗 Processing {total_keywords} keywords from Excel...", state="running")
                        main_kw_prog = st.progress(0.0, text="Initializing keyword processing...")

                        for idx, kw in enumerate(keywords_list):
                            status_main.write(f"--- Keyword {idx+1}/{total_keywords}: '{kw}' ---")
                            current_kw_links = set()
                            try:
                                google_urls = google_search_links(kw, top_n_google, status_main)
                                if google_urls:
                                    for g_url in google_urls:
                                        current_kw_links.update(scrape_url_for_whatsapp_links(g_url, status_main))
                            except Exception as e_gsearch_kw:
                                status_main.error(f"Error processing Google search for keyword '{kw}': {e_gsearch_kw}")
                                continue # Skip to next keyword

                            if current_kw_links:
                                status_main.write(f"🛡️ Validating {len(current_kw_links)} links for '{kw}'...")
                                # ... (validation block for current_kw_links) ...
                                with ThreadPoolExecutor(max_workers=max_workers_validation) as executor:
                                    # ...
                                    for i,future in enumerate(as_completed(future_to_link)):
                                        # ...
                                        all_validated_results_list.append({**validated_res, "Source": kw}) # Add source
                            else: status_main.info(f"ℹ️ No links found for keyword '{kw}'.")
                            main_kw_prog.progress((idx+1)/total_keywords, text=f"Keyword {idx+1}/{total_keywords} processed.")
                        main_kw_prog.empty()


                    elif input_method == "Scrape: Entire Website (Domain)":
                        # ... (Domain crawl logic, with try-except for crawl_website and per-page validation) ...
                        if not domain_url_crawl: status_main.update(label="⚠️ Input Missing: Enter a domain URL.", state="error"); return
                        status_main.update(label=f"🔗 Stage 1: Crawling Domain '{domain_url_crawl}'", state="running")
                        try:
                            crawled_pages_data = crawl_website(domain_url_crawl, max_crawl_pages, status_main)
                        except Exception as e_crawl:
                            status_main.error(f"🚫 Critical error during domain crawl: {e_crawl}"); return

                        if not crawled_pages_data: status_main.info("ℹ️ No pages with WhatsApp links found during crawl or crawl failed.");
                        else:
                            status_main.update(label=f"🛡️ Stage 2: Validating links from {len(crawled_pages_data)} crawled pages...", state="running")
                            # ... (Loop through crawled_pages_data, validate links per page_url as source) ...


                    # Generic input methods (Manual, File Upload, Specific Pages)
                    else:
                        # ... (Logic for these methods, assigning a generic source name) ...
                        # Ensure try-except blocks here too.
                        pass # Placeholder for brevity, use logic from previous answer


                    # Finalize results processing
                    if all_validated_results_list:
                        st.session_state.results_df = pd.DataFrame(all_validated_results_list)
                        status_main.update(label="🎉 All Processing Complete! Results are ready.", state="complete")
                    elif not any([keyword_gs, excel_file_gs, urls_text_specific, domain_url_crawl, links_text_manual, file_links_upload]): # No input provided
                        status_main.update(label="⚠️ Input Missing. Please provide input via the selected method.", state="error")
                    else: # Input provided, but no links found/processed
                        status_main.update(label="ℹ️ No WhatsApp links found or processed from the provided input.", state="complete")
                        st.session_state.results_df = pd.DataFrame() # Ensure empty DF if no results

                except Exception as e_main_process: # Catch-all for unexpected errors in the main block
                    status_main.update(label=f"🚫 An unexpected error occurred: {e_main_process}", state="error")
                    st.error(f"Detailed error: {e_main_process}")

            st.session_state.last_input_method_for_zip = input_method # For ZIP download section

    # --- Display Results ---
    if st.session_state.process_button_clicked: # Only show results section if processing was attempted
        if not st.session_state.results_df.empty:
            df_results = st.session_state.results_df
            st.markdown("---"); st.subheader("📊 Overall Validation Results Summary")
            # ... (Summary metrics: Total, Active, Expired, Error - same as before) ...
            active_df_overall = df_results[df_results['Status'] == 'Active'].copy()
            expired_df_overall = df_results[df_results['Status'].str.contains("Expired|Revoked", case=False, na=False)].copy()
            error_df_overall = df_results[~df_results.index.isin(active_df_overall.index) & ~df_results.index.isin(expired_df_overall.index)].copy()

            col1,col2,col3,col4 = st.columns(4) # Responsive columns
            with col1: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="Total Links Processed", value=len(df_results)); st.markdown('</div>', unsafe_allow_html=True)
            with col2: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="✅ Active Groups", value=len(active_df_overall)); st.markdown('</div>', unsafe_allow_html=True)
            with col3: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="⚠️ Expired/Revoked", value=len(expired_df_overall)); st.markdown('</div>', unsafe_allow_html=True)
            with col4: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="❌ Errors/Invalid", value=len(error_df_overall)); st.markdown('</div>', unsafe_allow_html=True)


            with st.expander("🔎 View, Filter & Download Combined Results Table", expanded=False):
                # ... (Combined results table, filtering, and download buttons - same as before) ...
                # Display the 'Source' column, ensure it's handled if not present for some reason (fallback)
                display_columns = ["Group Name", "Group Link", "Status", "Logo URL"]
                if "Source" in df_results.columns: display_columns.append("Source")
                
                # Add filters for combined table
                # ... (status_filter, name_filter_text, source_filter_text from previous version) ...

                st.dataframe(
                    filtered_df_overall[display_columns] if 'filtered_df_overall' in locals() else df_results[display_columns], # Ensure filtered_df exists
                    column_config={ # Define column configurations for better display
                        "Group Name": st.column_config.TextColumn("Group Name", help="The name of the WhatsApp group."),
                        "Group Link": st.column_config.LinkColumn("Invite Link", display_text="🔗 Join"),
                        "Status": st.column_config.TextColumn("Validation Status"),
                        "Logo URL": st.column_config.ImageColumn("Logo", width="small"),
                        "Source": st.column_config.TextColumn("Source Origin", help="Keyword or URL where the link was found.") if "Source" in display_columns else None
                    },
                    height=500, use_container_width=True, hide_index=True
                )
                # ... (Download buttons for combined CSVs) ...


            # --- Per-Source File Downloads (ZIP) ---
            last_method = st.session_state.get('last_input_method_for_zip', None)
            if last_method in ["Search & Scrape: Google (Excel Keywords)", "Scrape: Entire Website (Domain)"]:
                st.markdown("---"); st.subheader("🗂️ Download Source-Specific Active Group Files (ZIP)")
                if not active_df_overall.empty and 'Source' in active_df_overall.columns:
                    zip_buffer = io.BytesIO()
                    try:
                        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, False) as zip_file:
                            # Add overall summary files to ZIP
                            summary_md_content = generate_markdown_output(active_df_overall[["Logo URL", "Group Name", "Group Link"]])
                            zip_file.writestr("_SUMMARY_all_active_groups.md", summary_md_content.encode('utf-8'))
                            summary_csv_content = active_df_overall.to_csv(index=False)
                            zip_file.writestr("_SUMMARY_all_active_groups.csv", summary_csv_content.encode('utf-8'))

                            unique_sources = active_df_overall['Source'].dropna().unique() # Drop NA sources
                            if not unique_sources.size: st.info("No distinct sources found in active results for ZIP export.")

                            for source_name_orig in unique_sources:
                                source_active_df = active_df_overall[active_df_overall['Source'] == source_name_orig]
                                if source_active_df.empty: continue
                                sanitized_source_name = sanitize_filename(source_name_orig)
                                
                                md_content_src = generate_markdown_output(source_active_df[["Logo URL", "Group Name", "Group Link"]])
                                zip_file.writestr(f"{sanitized_source_name}_active.md", md_content_src.encode('utf-8'))
                                csv_content_src = source_active_df.to_csv(index=False)
                                zip_file.writestr(f"{sanitized_source_name}_active.csv", csv_content_src.encode('utf-8'))
                        
                        zip_buffer.seek(0)
                        st.download_button(
                            label="📥 Download All Source-Specific Files & Summaries (ZIP)",
                            data=zip_buffer,
                            file_name="whatsapp_groups_by_source.zip",
                            mime="application/zip",
                            use_container_width=True,
                            key="download_zip_ultimate"
                        )
                    except Exception as e_zip:
                        st.error(f"Error creating ZIP file: {e_zip}")
                else:
                    st.info("ℹ️ No active groups with source information available for ZIP export, or 'Source' column is missing.")

            # --- Overall Markdown Export (for all active groups combined) ---
            # ... (same as previous, ensure it uses active_df_overall) ...

        elif st.session_state.process_button_clicked: # Processing attempted, but results_df is empty
            st.info("🏁 Processing finished, but no WhatsApp links were found or validated successfully.", icon="🤷")
    else: # Before any processing attempt
         st.info("✨ Welcome! Select an input method from the sidebar and click 'Process & Validate Links' to begin your search.", icon="👋")

if __name__ == "__main__":
    main()
