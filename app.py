import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_library
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io
from fake_useragent import UserAgent

# --- Initialize UserAgent (for non-Google-result scraping and validation) ---
try:
    ua_general = UserAgent()
except Exception as e:
    st.error(f"Could not initialize Fake UserAgent for general scraping, using a default. Error: {e}")
    class FallbackUserAgent:
        def random(self): # Method name should be 'random'
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ua_general = FallbackUserAgent()

# --- Streamlit Configuration & Constants ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN_SHARED = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
MAX_VALIDATION_WORKERS = 10

# --- Custom CSS ---
st.markdown("""
<style>
/* General App Styles */
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; margin-bottom: 20px; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; }
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }

/* Metric Card Styles */
.metric-card {
    background-color: #f0f2f6;
    padding: 15px;
    border-radius: 8px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
    margin-bottom:10px;
}
.metric-card .stMetric { /* Target Streamlit's metric component */
    border-bottom: none !important; /* Remove default bottom border if any */
    padding-bottom: 0 !important;
}
.metric-card .stMetric label { /* Metric label */
    font-weight: bold;
    color: #333;
}
.metric-card .stMetric p { /* Metric value */
    font-size: 1.8em !important;
    color: #25D366;
}


/* --- CSS for Markdown Table --- */
.markdown-table-container table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    margin-bottom: 1.5em;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji";
    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    border-radius: 8px; /* Rounded corners for the table */
    overflow: hidden; /* Ensures border-radius clips content */
}

.markdown-table-container th,
.markdown-table-container td {
    padding: 12px 15px;
    border-bottom: 1px solid #e9ecef; /* Horizontal lines between rows */
    text-align: left;
    vertical-align: middle;
}
.markdown-table-container td:not(:last-child),
.markdown-table-container th:not(:last-child) {
    border-right: 1px solid #e9ecef; /* Vertical lines between cells */
}


.markdown-table-container th {
    background-color: #f8f9fa;
    font-weight: 600; /* Semibold */
    color: #495057;
    text-align: center;
    font-size: 0.95em;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

/* Logo column */
.markdown-table-container td:nth-child(1) {
    text-align: center;
    width: 70px; /* Fixed width for logo */
}
/* Group Name column */
.markdown-table-container td:nth-child(2) {
    text-align: center; /* Center group name */
    font-weight: 500; /* Medium weight for name */
    color: #343a40;
    font-size: 1.0em;
}
/* Join Link column */
.markdown-table-container td:nth-child(3) {
    text-align: center;
    width: 130px; /* Approx width for join button */
}

img.group-logo-markdown {
    width: 48px; /* Matched demo image size */
    height: 48px;
    border-radius: 50%;
    object-fit: cover;
    display: inline-block;
    vertical-align: middle;
    border: 2px solid #f0f0f0; /* Optional: subtle border for logo */
}

a.join-button-md {
    display: inline-block;
    padding: 9px 20px; /* Slightly larger padding */
    background-color: #006621; /* Darker, richer green */
    color: white !important;
    text-decoration: none;
    border-radius: 25px; /* Pill shape */
    font-weight: bold;
    font-size: 0.9em;
    border: none;
    box-shadow: 0 3px 6px rgba(0,0,0,0.15);
    transition: background-color 0.2s ease-in-out, transform 0.15s ease, box-shadow 0.15s ease;
}

a.join-button-md:hover, a.join-button-md:focus {
    background-color: #00521a; /* Slightly darker on hover */
    color: white !important;
    transform: translateY(-2px); /* More pronounced lift */
    box-shadow: 0 5px 10px rgba(0,0,0,0.2);
}
a.join-button-md:active {
    transform: translateY(-1px);
    box-shadow: 0 2px 5px rgba(0,0,0,0.15);
}


/* Responsive adjustments for Markdown Table */
@media (max-width: 768px) {
    .markdown-table-container th,
    .markdown-table-container td {
        padding: 10px 8px;
        font-size: 0.9em;
    }
    .markdown-table-container td:nth-child(2) { /* Group Name */
        font-size: 0.95em;
    }
    a.join-button-md {
        padding: 7px 14px;
        font-size: 0.85em;
    }
    img.group-logo-markdown {
        width: 40px;
        height: 40px;
    }
    .markdown-table-container td:nth-child(1) { width: 60px; }
    .markdown-table-container td:nth-child(3) { width: 110px; }
}
@media (max-width: 480px) {
    .main-title { font-size: 2em; }
    .subtitle { font-size: 1em; }
    .markdown-table-container {
        /* On very small screens, allow horizontal scroll for the table container */
        overflow-x: auto;
    }
    .markdown-table-container table {
        min-width: 400px; /* Prevent extreme squishing */
    }
    .markdown-table-container th,
    .markdown-table-container td {
        font-size: 0.85em;
        padding: 8px 5px;
    }
    img.group-logo-markdown {
        width: 35px;
        height: 35px;
    }
     a.join-button-md {
        padding: 6px 10px;
        font-size: 0.8em;
    }
}

</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def get_random_headers_for_general_use():
    """Returns headers with a random User-Agent for general scraping/validation."""
    return {
        "User-Agent": ua_general.random(), # Corrected: call the method
        "Accept-Language": "en-US,en;q=0.9"
    }

def append_query_param(url, param_name, param_value):
    if not url: return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return parsed_url._replace(query=new_query_string).geturl()


# --- Functions directly from USER'S WORKING EXAMPLE (for Google Search path) ---
def google_search_user_original(query, top_n=5, pause_duration=2.0):
    """Fetch URLs from Google's top N search results."""
    try:
        st.sidebar.info(f"Googling '{query}' (top {top_n}, pause: {pause_duration}s)...")
        # The googlesearch library uses 'num_results' not 'top_n' for its parameter.
        urls = list(google_search_library(query, num_results=top_n, lang="en", pause=pause_duration))
        if not urls:
            st.sidebar.warning(f"No search results from Google for '{query}'. Try different terms or increase pause.")
        return urls
    except requests.exceptions.HTTPError as http_err: # Specific catch for HTTP errors from underlying requests
        st.sidebar.error(f"Google Search HTTP error: {http_err}")
        st.sidebar.error(f"This often means Google is rate-limiting. Try increasing the 'Google Search Pause'.")
        return []
    except Exception as e:
        st.sidebar.error(f"Google Search failed for '{query}'. Error: {str(e)}")
        st.sidebar.error(f"Exception type: {type(e).__name__}. Check logs for details.") # More specific error
        return []

def scrape_whatsapp_links_user_original(url):
    """Scrape WhatsApp group links from a webpage. (User's original function)"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links.append(a['href'].split('?')[0])
        for text in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text:
                found_links = re.findall(r'https?://chat\.whatsapp\.com/[^\s"\'<>()]+', text)
                for flink in found_links:
                    links.append(flink.split('?')[0])
        return list(set(links))
    except Exception as e:
        st.sidebar.warning(f"Failed to scrape (orig): {urlparse(url).netloc} ({type(e).__name__})", icon="🕸️")
        return []
# --- END of functions from USER'S WORKING EXAMPLE ---


# --- Enhanced scraping function (for Specific Page / Entire Website) ---
def scrape_whatsapp_links_enhanced(url, session):
    links = set()
    try:
        netloc_for_error = urlparse(url).netloc or url[:30]
        response = session.get(url, headers=get_random_headers_for_general_use(), timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0])
        for text_chunk in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text_chunk:
                found_in_chunk = re.findall(r'https?://chat\.whatsapp\.com/[^\s"\'<>()]+', text_chunk)
                for link_url in found_in_chunk: links.add(link_url.split('?')[0])
    except requests.exceptions.Timeout: st.sidebar.warning(f"Timeout (enh) {netloc_for_error}", icon="⏱️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape err (enh) {netloc_for_error}: {type(e).__name__}", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Parse err (enh) {netloc_for_error}: {type(e).__name__}", icon="💣")
    return list(links)

# --- Validation function (uses fake UA) ---
def validate_link(link):
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        response = requests.get(link, headers=get_random_headers_for_general_use(), timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200: result["Status"] = f"HTTP Error {response.status_code}"; return result
        if WHATSAPP_DOMAIN not in response.url: result["Status"] = "Invalid Link (Redirected)"; return result
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')
        result["Group Name"] = unescape(meta_title['content']).strip() if meta_title and meta_title.get('content') else "Unnamed Group"
        
        img_tags = soup.find_all('img', src=True)
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN_SHARED.match(src):
                result["Logo URL"] = src
                result["Status"] = "Active"
                break
        if result["Status"] != "Active":
            result["Status"] = "Expired"

    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.RequestException: result["Status"] = "Network Error"
    except Exception: result["Status"] = "Parsing Error"
    return result

def crawl_website(start_url, max_depth=3, max_pages=100):
    if not start_url.startswith(('http://', 'https://')): start_url = 'https://' + start_url
    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    urls_to_visit, visited_urls, scraped_content_urls = [(start_url, 0)], set(), set()
    session = requests.Session()
    with st.spinner(f"Crawling {base_domain} (Depth Limit:{max_depth}, Page Limit:{max_pages})..."):
        page_count = 0
        while urls_to_visit and page_count < max_pages:
            current_url, depth = urls_to_visit.pop(0)
            if current_url in visited_urls or depth > max_depth: continue
            visited_urls.add(current_url)
            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}): {current_url[:60]}...")
            try:
                response = session.get(current_url, headers=get_random_headers_for_general_use(), timeout=7)
                response.raise_for_status()
                scraped_content_urls.add(current_url); page_count += 1
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        abs_url = urljoin(current_url, link_tag['href'])
                        parsed_abs_url = urlparse(abs_url)
                        if parsed_abs_url.scheme in ['http', 'https'] and parsed_abs_url.netloc == base_domain and \
                           parsed_abs_url.path and abs_url not in visited_urls and (abs_url, depth + 1) not in urls_to_visit:
                            urls_to_visit.append((abs_url, depth + 1))
            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl skip on {urlparse(current_url).netloc}: {type(e).__name__}", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl critical err on {urlparse(current_url).netloc}: {type(e).__name__}", icon="💥")
    st.sidebar.success(f"Crawler explored {len(visited_urls)} URLs, found {len(scraped_content_urls)} scrape-able pages.")
    return list(scraped_content_urls), session


def load_links_from_text_file(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        try:
            return pd.read_csv(uploaded_file).iloc[:, 0].tolist()
        except Exception as e:
            st.error(f"Error reading CSV: {e}")
            return []
    else:
        try:
            return [line.decode().strip() for line in uploaded_file.readlines() if line.strip()]
        except Exception as e:
            st.error(f"Error reading TXT file: {e}")
            return []

def load_keywords_from_excel(uploaded_file):
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        st.error(f"Error reading Excel {uploaded_file.name}: {e}. Ensure 'openpyxl' installed.")
        return []

def generate_markdown_output(active_results_df):
    if active_results_df.empty: return "No active groups found to generate Markdown."
    # Headers match the visual order: Logo, Name, Link
    markdown_lines = ["| Logo | Group Name | Join Link |", "|:---:|:---:|:---:|"] # Added alignment
    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", "N/A")
        group_link = row.get("Group Link", "")
        
        if logo_url:
            resized_logo_url_server = append_query_param(logo_url, 'w', '96') # Request 96px for better quality when scaled down by CSS
            logo_md = f'<img src="{resized_logo_url_server}" alt="Logo" class="group-logo-markdown">'
        else:
            logo_md = "🖼️" # Placeholder if no logo
            
        # Ensure group name doesn't break Markdown table if it contains pipes
        safe_group_name = group_name.replace("|", "|")
        # Use "Join" for the button text as per visual example
        link_md = f'<a href="{group_link}" class="join-button-md" target="_blank" rel="noopener noreferrer">Join</a>'
        markdown_lines.append(f"| {logo_md} | {safe_group_name} | {link_md} |")
    return "\n".join(markdown_lines)

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Find, scrape, and validate WhatsApp group links efficiently.</p>', unsafe_allow_html=True)

    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google",
            "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", 
            "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", 
            "Upload Link File (TXT/CSV for Validation)"
        ], key="input_method_main_select")

        google_results_slider_top_n = 5 
        google_search_pause = 2.0

        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)"]:
            google_results_slider_top_n = st.slider(
                "Number of top Google results per query", 
                min_value=1, max_value=20, value=5, # Increased max results slightly
                key="google_top_n_slider"
            )
            google_search_pause = st.slider(
                "Google Search Pause (seconds):", min_value=1.0, max_value=15.0, value=3.0, step=0.5, # Increased default and max pause
                help="Longer pause helps avoid Google rate-limiting. Essential for bulk queries.", 
                key="google_pause_slider"
            )
        
        crawl_depth_val, max_crawl_pages_val = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive website crawling can be slow and resource-intensive. Use responsibly.", icon="🚨")
            crawl_depth_val = st.slider("Max Crawl Depth:", min_value=0, max_value=5, value=1, key="crawl_depth_slider") # Adjusted defaults for safety
            max_crawl_pages_val = st.slider("Max Pages to Crawl:", min_value=1, max_value=200, value=20, key="crawl_pages_slider") # Adjusted defaults
        
        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.success("All results and cache cleared!")
            st.rerun()


    all_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")
    
    general_purpose_session = requests.Session()
    try:
        if input_method == "Search and Scrape from Google":
            keyword_gs = st.text_input("Search Query:", placeholder="e.g., technology WhatsApp group Kenya", key="gs_keyword_input")
            if st.button("🔍 Search, Scrape, and Validate", use_container_width=True, key="gs_button"):
                if not keyword_gs: st.warning("Please enter a search query.")
                else:
                    with st.spinner(f"Searching Google for '{keyword_gs}'..."):
                        search_page_urls = google_search_user_original(keyword_gs, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                    if search_page_urls:
                        st.success(f"Google found {len(search_page_urls)} potential webpages. Now scraping them for WhatsApp links...")
                        prog_bar_gs = st.progress(0)
                        links_found_on_pages = 0
                        for i, page_url in enumerate(search_page_urls):
                            st.sidebar.text(f"Scraping (orig): {page_url[:50]}...")
                            links_from_page = scrape_whatsapp_links_user_original(page_url)
                            if links_from_page:
                                links_found_on_pages += len(links_from_page)
                                all_scraped_links.update(links_from_page)
                            prog_bar_gs.progress((i+1)/len(search_page_urls))
                        st.success(f"Scraping of Google results complete. Found {links_found_on_pages} WhatsApp link(s) before de-duplication.")
                    elif not search_page_urls and keyword_gs: # If keyword was given but no results
                        st.warning("No webpages found from Google for your query. Try broader terms or check Google Search Pause settings.")

        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file_bulk = st.file_uploader("Upload Excel (keywords in 1st column, one per row)", type=["xlsx"], key="gs_bulk_excel_upload")
            if excel_file_bulk and st.button("⚙️ Process Excel & Scrape from Google", use_container_width=True, key="gs_bulk_button"):
                keywords_bulk = load_keywords_from_excel(excel_file_bulk)
                if not keywords_bulk: st.warning("No keywords found in the Excel file, or file is empty.")
                else:
                    st.info(f"Processing {len(keywords_bulk)} keywords. This may take a while...")
                    prog_bulk, stat_txt_bulk = st.progress(0), st.empty()
                    total_links_from_bulk = 0
                    for i, kw_bulk in enumerate(keywords_bulk):
                        stat_txt_bulk.write(f"**Keyword {i+1}/{len(keywords_bulk)}:** `{kw_bulk}` - Searching Google...")
                        search_page_urls_bulk = google_search_user_original(kw_bulk, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                        if search_page_urls_bulk:
                            stat_txt_bulk.write(f"**Keyword {i+1}/{len(keywords_bulk)}:** `{kw_bulk}` - Scraping {len(search_page_urls_bulk)} pages...")
                            for page_idx, page_url_bulk in enumerate(search_page_urls_bulk):
                                st.sidebar.text(f"Scraping (orig bulk {i+1}-{page_idx+1}): {page_url_bulk[:40]}...")
                                links_from_page_bulk = scrape_whatsapp_links_user_original(page_url_bulk)
                                if links_from_page_bulk:
                                    total_links_from_bulk += len(links_from_page_bulk)
                                    all_scraped_links.update(links_from_page_bulk)
                        prog_bulk.progress((i + 1) / len(keywords_bulk))
                    stat_txt_bulk.success(f"Bulk Google processing complete. Found {total_links_from_bulk} WhatsApp link(s) before de-duplication.")

        elif input_method == "Scrape from Specific Webpage URL":
            page_url_specific = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input")
            if st.button("📄 Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if not page_url_specific or not (page_url_specific.startswith("http://") or page_url_specific.startswith("https://")):
                    st.warning("Please enter a valid URL (starting with http:// or https://).")
                else:
                    with st.spinner(f"Scraping {page_url_specific}..."):
                        links_from_page_spec = scrape_whatsapp_links_enhanced(page_url_specific, general_purpose_session)
                        all_scraped_links.update(links_from_page_spec)
                    st.success(f"Scraping of {page_url_specific} complete. Found {len(links_from_page_spec)} link(s).")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url_crawl = st.text_input("Enter Base Domain URL to Crawl:", placeholder="example.com (without http/https)", key="crawl_domain_input")
            if st.button("🕸️ Crawl Website & Scrape Links", use_container_width=True, key="crawl_button"):
                if not domain_url_crawl: st.warning("Please enter a base domain URL.")
                else:
                    full_domain_url = domain_url_crawl if domain_url_crawl.startswith(('http://', 'https://')) else 'https://' + domain_url_crawl
                    pages_to_scrape_crawl, crawl_session_obj = crawl_website(full_domain_url, max_depth=crawl_depth_val, max_pages=max_crawl_pages_val)
                    try:
                        if pages_to_scrape_crawl:
                            st.info(f"Crawler finished. Now scraping {len(pages_to_scrape_crawl)} discovered pages...")
                            prog_crawl, stat_txt_crawl = st.progress(0), st.empty()
                            total_links_from_crawl = 0
                            for i, p_url_crawl in enumerate(pages_to_scrape_crawl):
                                stat_txt_crawl.text(f"Scraping (enh crawl): {p_url_crawl[:60]}... ({i+1}/{len(pages_to_scrape_crawl)})")
                                links_from_page_crawl = scrape_whatsapp_links_enhanced(p_url_crawl, crawl_session_obj) # Use the crawl session
                                if links_from_page_crawl:
                                    total_links_from_crawl += len(links_from_page_crawl)
                                    all_scraped_links.update(links_from_page_crawl)
                                prog_crawl.progress((i + 1) / len(pages_to_scrape_crawl))
                            stat_txt_crawl.success(f"Website scraping complete. Found {total_links_from_crawl} WhatsApp link(s) before de-duplication.")
                        else: st.warning(f"Crawler did not find any pages to scrape from {full_domain_url} with current settings.")
                    finally:
                        if 'crawl_session_obj' in locals() and crawl_session_obj: crawl_session_obj.close()
        
        elif input_method == "Enter Links Manually (for Validation)":
            links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=150, placeholder=f"e.g., {WHATSAPP_DOMAIN}ABC123\n{WHATSAPP_DOMAIN}XYZ789", key="manual_links_text_area")
            if st.button("✍️ Validate Entered Links", use_container_width=True, key="manual_validate_button"):
                links_manual = [line.strip() for line in links_text_manual.split('\n') if line.strip().startswith(WHATSAPP_DOMAIN)]
                if not links_manual: st.warning(f"Please enter at least one valid WhatsApp link (starting with {WHATSAPP_DOMAIN}).")
                else: all_scraped_links.update(links_manual)


        elif input_method == "Upload Link File (TXT/CSV for Validation)":
            uploaded_file_val = st.file_uploader("Upload TXT or CSV (one link per line, or first column for CSV)", type=["txt", "csv"], key="upload_file_links")
            if uploaded_file_val and st.button("📂 Validate File Links", use_container_width=True, key="upload_validate_button"):
                links_from_file = load_links_from_text_file(uploaded_file_val)
                valid_links_from_file = [l for l in links_from_file if l.startswith(WHATSAPP_DOMAIN)]
                if not valid_links_from_file: st.warning(f"No valid WhatsApp links (starting with {WHATSAPP_DOMAIN}) found in the uploaded file.")
                else: all_scraped_links.update(valid_links_from_file)
    finally:
        if 'general_purpose_session' in locals() and general_purpose_session: general_purpose_session.close()


    # --- Unified Validation Step ---
    if all_scraped_links:
        links_to_validate_now = list(all_scraped_links - st.session_state.processed_links_in_session)
        if not links_to_validate_now:
            st.info("No new WhatsApp links found to validate, or all previously found links have been processed in this session.")
        else:
            st.success(f"Found {len(all_scraped_links)} total unique WhatsApp link(s). Validating {len(links_to_validate_now)} new/unprocessed link(s)...")
            prog_val, stat_val = st.progress(0), st.empty()
            new_results_validation = []
            with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
                for i, future in enumerate(as_completed(future_to_link)):
                    link_validated = future_to_link[future]
                    try:
                        result_validated = future.result()
                        new_results_validation.append(result_validated)
                    except Exception as exc: # Catch errors from validate_link future if any
                        st.error(f"Error validating link {link_validated}: {exc}")
                        new_results_validation.append({"Group Name": "Validation Error", "Group Link": link_validated, "Logo URL": "", "Status": "Error"})
                    
                    st.session_state.processed_links_in_session.add(link_validated)
                    prog_val.progress((i + 1) / len(links_to_validate_now))
                    stat_val.text(f"Validated {i + 1}/{len(links_to_validate_now)} links: {link_validated.split('/')[-1]}")
            
            # Add new results to existing session results
            existing_links_in_results = {res['Group Link'] for res in st.session_state.results}
            for res in new_results_validation:
                if res['Group Link'] not in existing_links_in_results:
                    st.session_state.results.append(res)
                else: # Update existing entry if it was re-validated (e.g. status changed)
                    for idx, old_res in enumerate(st.session_state.results):
                        if old_res['Group Link'] == res['Group Link']:
                            st.session_state.results[idx] = res
                            break
            
            stat_val.success(f"Validation complete for {len(links_to_validate_now)} link(s)!")


    # --- Display Results ---
    if st.session_state.results:
        df_results_display = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='last')
        st.session_state.results = df_results_display.to_dict('records') # Update session state with de-duplicated
        
        active_df_display = df_results_display[df_results_display['Status'] == 'Active'].copy()
        expired_df_display = df_results_display[df_results_display['Status'] == 'Expired'].copy()
        error_df_display = df_results_display[~df_results_display['Status'].isin(['Active', 'Expired'])].copy()
        
        st.subheader("📊 Results Summary")
        col1_disp, col2_disp, col3_disp, col4_disp = st.columns(4)
        with col1_disp:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Processed", len(df_results_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col2_disp:
            st.markdown('<div class="metric-card" style="border-left: 5px solid #28a745;">', unsafe_allow_html=True) # Green accent for active
            st.metric("✅ Active Links", len(active_df_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col3_disp:
            st.markdown('<div class="metric-card" style="border-left: 5px solid #ffc107;">', unsafe_allow_html=True) # Yellow accent for expired
            st.metric("⚠️ Expired Links", len(expired_df_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col4_disp:
            st.markdown('<div class="metric-card" style="border-left: 5px solid #dc3545;">', unsafe_allow_html=True) # Red accent for errors
            st.metric("❌ Error/Other", len(error_df_display))
            st.markdown('</div>', unsafe_allow_html=True)


        with st.expander("🔎 View and Filter All Processed Results", expanded=False):
            status_filter_options = sorted(list(df_results_display['Status'].unique()))
            default_filter = ["Active"] if "Active" in status_filter_options else status_filter_options
            status_filter_val = st.multiselect("Filter by Status:", options=status_filter_options, default=default_filter, key="status_filter_multiselect")
            
            name_filter_text = st.text_input("Filter by Group Name (contains, case-insensitive):", key="name_filter_text")

            filtered_df_for_display = df_results_display.copy()
            if status_filter_val:
                filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Status'].isin(status_filter_val)]
            if name_filter_text:
                filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Group Name'].str.contains(name_filter_text, case=False, na=False)]
            
            st.dataframe(
                filtered_df_for_display[['Group Name', 'Status', 'Group Link', 'Logo URL']], # Reordered for clarity
                column_config={
                    "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="🔗 Join Group", width="medium"),
                    "Logo URL": st.column_config.ImageColumn("Logo Preview", width="small") # Show image directly if URL is valid
                },
                height=400,
                use_container_width=True,
                key="results_dataframe"
            )
        
        st.subheader("📋 Markdown Export (Active Groups)")
        if not active_df_display.empty:
            md_data_export = generate_markdown_output(active_df_display)
            
            col_md_prev, col_md_text = st.columns(2)
            with col_md_prev:
                with st.expander("👁️ Live Markdown Preview", expanded=True):
                     st.markdown(f"<div class='markdown-table-container'>{md_data_export}</div>", unsafe_allow_html=True)
            with col_md_text:
                with st.expander("📝 Copy or Download Markdown Code", expanded=True):
                    st.text_area("Markdown Table Code:", value=md_data_export, height=280, key="md_export_area", help="Ctrl+A then Ctrl+C to copy")
                    st.download_button("📥 Download as .md File", md_data_export, "active_whatsapp_groups.md", "text/markdown", use_container_width=True, key="md_export_download")
        else: 
            st.info("No active groups found to generate Markdown output.")
        
        st.subheader("💾 Download Full Results")
        col_dl1_orig, col_dl2_orig = st.columns(2)
        with col_dl1_orig:
            if not active_df_display.empty:
                csv_active_orig = active_df_display.to_csv(index=False).encode('utf-8')
                st.download_button("✔️ Download Active Groups (CSV)", csv_active_orig, "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv_orig")
            else:
                st.button("✔️ Download Active Groups (CSV)", disabled=True, use_container_width=True)

        with col_dl2_orig:
            csv_all_orig = df_results_display.to_csv(index=False).encode('utf-8')
            st.download_button("🧾 Download All Processed (CSV)", csv_all_orig, "all_processed_groups.csv", "text/csv", use_container_width=True, key="dl_all_csv_orig")

    elif not all_scraped_links and not st.session_state.results: # Only show if truly nothing has happened yet
        st.info("✨ Start by searching, entering links, or uploading a file to find WhatsApp groups!", icon="👋")


if __name__ == "__main__":
    # Ensure necessary libraries are available
    lib_errors = []
    try: import openpyxl
    except ImportError: lib_errors.append("Module 'openpyxl' for Excel reading is missing. Please install: `pip install openpyxl`")
    
    try: 
        from fake_useragent import UserAgent
        UserAgent() # Test initialization
    except ImportError: lib_errors.append("Module 'fake-useragent' is missing. Web scraping effectiveness might be reduced. Install: `pip install fake-useragent`")
    except Exception as e_ua: lib_errors.append(f"Fake-useragent initialized with issues (using fallback). Error: {e_ua}. Web scraping might use a default User-Agent.")

    if lib_errors:
        for err in lib_errors: st.error(err)
        st.stop()
    
    main()
