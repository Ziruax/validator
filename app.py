import streamlit as st
import pandas as pd
import requests
from html import unescape
import html as html_converter
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_library # User's import
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io
from fake_useragent import UserAgent
import logging # For more detailed logging if needed outside Streamlit

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Initialize UserAgent ---
try:
    ua_general = UserAgent()
    logging.info("Fake UserAgent initialized successfully.")
except Exception as e:
    st.error(f"Could not initialize Fake UserAgent, using a default. Error: {type(e).__name__} - {e}")
    logging.error(f"Fake UserAgent initialization failed: {type(e).__name__} - {e}", exc_info=True)
    class FallbackUserAgent:
        def random(self):
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
MAX_VALIDATION_WORKERS = 10 # Consider making this configurable

# --- Custom CSS ---
st.markdown("""
<style>
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; }
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }
img.group-logo-html-table { width:35px; height:35px; border-radius:50%; object-fit:cover; vertical-align:middle; margin-right: 5px; }
table.my-table { width: 100%; border-collapse: collapse; font-family: sans-serif; margin-bottom: 1em; }
table.my-table th, table.my-table td { border: 1px solid #ccc; padding: 8px; text-align: left; vertical-align: middle; }
table.my-table th { background: #25D366; color: white; font-weight: bold; }
table.my-table tr:nth-child(even) { background: #f9f9f9; }
table.my-table img { vertical-align: middle; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def get_random_headers_for_general_use():
    return {"User-Agent": ua_general.random(), "Accept-Language": "en-US,en;q=0.9"}

def append_query_param(url, param_name, param_value):
    if not url: return ""
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        query_params[param_name] = [param_value]
        new_query_string = urlencode(query_params, doseq=True)
        return parsed_url._replace(query=new_query_string).geturl()
    except Exception as e:
        logging.warning(f"Could not append query param to URL '{url}': {e}")
        return url # Return original URL on error

# --- Google Search Function (User's Original Style) ---
def google_search_user_original(query, top_n=5, pause_duration=2.0):
    st.sidebar.info(f"Googling (user original method): '{query}' (top {top_n}, pause: {pause_duration}s)...")
    try:
        # Parameters based on user's initial diff: num=top_n, stop=top_n
        urls = list(google_search_library(
            query,
            lang="en",
            num=top_n,
            stop=top_n, # This ensures it stops after 'top_n' results
            pause=pause_duration
        ))
        if not urls:
            st.warning(f"No search results found for '{query}'.")
        logging.info(f"Google search for '{query}' returned {len(urls)} URLs.")
        return urls
    except TypeError as te:
        error_message = f"Google Search TypeError: {str(te)}. \n\nParameters used: num={top_n}, stop={top_n}, lang='en', pause={pause_duration}. \n\nThis usually means the parameter names are incorrect for your specific 'googlesearch' library version. Please verify the library's documentation for correct arguments (e.g., 'num_results' instead of 'num'/'stop', or vice-versa)."
        st.error(error_message)
        logging.error(f"Google Search TypeError with query '{query}': {te}", exc_info=True)
        return []
    except Exception as e:
        st.error(f"An unexpected Google Search error occurred: {type(e).__name__} - {str(e)}")
        logging.error(f"Google Search unexpected error with query '{query}': {e}", exc_info=True)
        return []

# --- Scraping Functions ---
def scrape_whatsapp_links_user_original(url):
    """Scrapes WhatsApp links using a fixed User-Agent (user's original method)."""
    links = set()
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = response.apparent_encoding # More reliable encoding detection
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0]) # Normalize
        # Search in text nodes as well
        text_nodes = soup.find_all(string=True)
        for text_node in text_nodes:
            if WHATSAPP_DOMAIN in text_node:
                found_in_text = re.findall(r'https?://chat\.whatsapp\.com/[A-Za-z0-9_-]+', text_node)
                for flink in found_in_text:
                    links.add(flink.split('?')[0]) # Normalize
        if links: logging.info(f"Scraped {len(links)} links (orig method) from {url}")
        return list(links)
    except requests.exceptions.RequestException as e:
        st.sidebar.warning(f"Network error (orig) scraping {urlparse(url).netloc}: {type(e).__name__}", icon="🌐")
        logging.warning(f"Network error (orig) scraping {url}: {e}")
    except Exception as e:
        st.sidebar.warning(f"Error (orig) scraping {urlparse(url).netloc}: {type(e).__name__}", icon="⚠️")
        logging.warning(f"Error (orig) scraping {url}: {e}", exc_info=True)
    return list(links)

def scrape_whatsapp_links_enhanced(url, session):
    """Scrapes WhatsApp links using a provided session and random User-Agent."""
    links = set()
    try:
        netloc = urlparse(url).netloc or url[:30]
        response = session.get(url, headers=get_random_headers_for_general_use(), timeout=15)
        response.encoding = response.apparent_encoding
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0])
        text_nodes = soup.find_all(string=True)
        for text_node in text_nodes:
            if WHATSAPP_DOMAIN in text_node:
                found_in_text = re.findall(r'https?://chat\.whatsapp\.com/[A-Za-z0-9_-]+', text_node)
                for link_url in found_in_text: links.add(link_url.split('?')[0])
        if links: logging.info(f"Scraped {len(links)} links (enh method) from {url}")
        return list(links)
    except requests.exceptions.Timeout: st.sidebar.warning(f"Timeout (enh) scraping {netloc}", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"HTTP {e.response.status_code} (enh) scraping {netloc}", icon="📉")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Network error (enh) scraping {netloc}: {type(e).__name__}", icon="🌐")
    except Exception as e: st.sidebar.warning(f"Error (enh) scraping {netloc}: {type(e).__name__}", icon="💣")
    return list(links)

# --- Validation Function ---
def validate_link(link):
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error (Validation)"}
    try:
        response = requests.get(link, headers=get_random_headers_for_general_use(), timeout=12, allow_redirects=True)
        response.encoding = response.apparent_encoding
        
        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result
        if WHATSAPP_DOMAIN not in response.url: # Check final URL after redirects
            result["Status"] = "Invalid Link (Redirected Off WhatsApp)"
            return result
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title_tag = soup.find('meta', property='og:title')
        
        group_name_content = "Unknown Group Name"
        current_status = "Error (Parsing Meta)" # Default if meta parsing fails

        if meta_title_tag and meta_title_tag.get('content'):
            group_name_content = unescape(meta_title_tag['content']).strip()
            gn_lower = group_name_content.lower()
            # Heuristics for expired/invalid links based on title
            if any(s in gn_lower for s in ["group no longer available", "couldn't join", "invite link was reset", "link has expired"]):
                current_status = "Expired/Invalid (Title)"
            elif "you can't join this group because it is full" in gn_lower:
                current_status = "Group Full (Title)"
            elif "whatsapp group invite" == gn_lower and len(group_name_content) == len("WhatsApp Group Invite"): # Very generic title
                current_status = "Potentially Invalid (Generic Title)"
            else: # Title seems okay or specific
                current_status = "Title OK" 
        else: # No meta title, often indicates an error or non-standard invite page
            current_status = "Expired/Invalid (No Title)"
        
        result["Group Name"] = group_name_content

        # Check for logo
        img_tags = soup.find_all('img', src=True)
        logo_found = False
        for img in img_tags:
            src = unescape(img.get('src', ''))
            if IMAGE_PATTERN_SHARED.match(src):
                result["Logo URL"] = src
                logo_found = True
                break
        
        # Final status determination
        if current_status == "Title OK":
            result["Status"] = "Active" if logo_found else "Potentially Active (No Logo)"
        elif current_status == "Potentially Invalid (Generic Title)":
            result["Status"] = "Active" if logo_found else "Expired/Invalid (Generic Title, No Logo)"
        else: # Covers "Expired/Invalid (Title)", "Group Full (Title)", "Expired/Invalid (No Title)", etc.
            result["Status"] = current_status # Keep status determined by title/no title

    except requests.exceptions.Timeout: result["Status"] = "Network Error: Timeout"
    except requests.exceptions.ConnectionError: result["Status"] = "Network Error: Connection"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error: {type(e).__name__}"
    except Exception as e:
        result["Status"] = f"Parsing Error: {type(e).__name__}"
        logging.error(f"Error validating link {link}: {e}", exc_info=True)
    return result

# --- Crawling Function ---
def crawl_website(start_url, max_depth=3, max_pages=None):
    if not start_url or not start_url.strip():
        st.error("Start URL for crawling cannot be empty.")
        return [], None
    if not (start_url.startswith('http://') or start_url.startswith('https://')):
        start_url = 'https://' + start_url
    
    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    if not base_domain:
        st.error(f"Invalid start URL: '{start_url}'. Could not determine base domain.")
        return [], None

    # Store (normalized_url, depth) tuples
    urls_to_visit = [(urljoin(start_url, parsed_start_url.path), 0)] # Start with normalized URL
    visited_urls = set() # Store normalized URLs
    scraped_page_urls = set()
    
    session = requests.Session()
    page_limit_msg = 'Unlimited' if max_pages is None else str(max_pages)
    
    with st.spinner(f"Crawling {base_domain} (Depth: {max_depth}, Max Pages: {page_limit_msg})..."):
        page_count = 0
        while urls_to_visit and (max_pages is None or page_count < max_pages):
            current_norm_url, depth = urls_to_visit.pop(0)
            
            if current_norm_url in visited_urls or depth > max_depth:
                continue
            visited_urls.add(current_norm_url)
            
            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}/{page_limit_msg if max_pages is not None else '∞'}): {current_norm_url[:70]}...")
            try:
                response = session.get(current_norm_url, headers=get_random_headers_for_general_use(), timeout=10, allow_redirects=True)
                response.encoding = response.apparent_encoding
                response.raise_for_status()
                
                final_url_after_redirects = urljoin(response.url, urlparse(response.url).path)
                if urlparse(final_url_after_redirects).netloc != base_domain:
                    # logging.info(f"Crawl: Redirected off-domain from {current_norm_url} to {final_url_after_redirects}")
                    continue # Skip if redirected off domain
                
                # If redirected within domain, update visited_urls and current_norm_url
                if final_url_after_redirects != current_norm_url:
                    if final_url_after_redirects in visited_urls: continue
                    visited_urls.add(final_url_after_redirects)
                    # current_norm_url = final_url_after_redirects # No need to update current_norm_url for this iteration

                scraped_page_urls.add(response.url) # Add the actual URL fetched (could be after redirect)
                page_count += 1
                
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag.get('href')
                        if not href: continue
                        
                        abs_url = urljoin(response.url, href) # Base URL for join should be the final response URL
                        parsed_abs_url = urlparse(abs_url)
                        
                        if parsed_abs_url.scheme not in ['http', 'https'] or \
                           parsed_abs_url.netloc != base_domain or \
                           not parsed_abs_url.path or \
                           parsed_abs_url.fragment: # Ignore fragment links, mailto, etc.
                            continue
                        
                        norm_abs_url_to_add = urljoin(abs_url, parsed_abs_url.path) # Normalize
                        if norm_abs_url_to_add not in visited_urls and norm_abs_url_to_add not in [u[0] for u in urls_to_visit]:
                            urls_to_visit.append((norm_abs_url_to_add, depth + 1))
            
            except requests.exceptions.Timeout: st.sidebar.warning(f"Crawl timeout: {current_norm_url[:50]}", icon="⏱️")
            except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Crawl HTTP {e.response.status_code}: {current_norm_url[:50]}", icon="📉")
            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl net-err on {current_norm_url[:50]}: {type(e).__name__}", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl general err on {current_norm_url[:50]}: {type(e).__name__}", icon="💥"); logging.error(f"Crawl error: {e}", exc_info=True)
            
    st.sidebar.success(f"Crawler found {len(scraped_page_urls)} unique pages on {base_domain}.")
    return list(scraped_page_urls), session

# --- File Loading Functions ---
def load_links_from_file(uploaded_file):
    links = []
    if not uploaded_file: return links
    try:
        content = uploaded_file.read().decode('utf-8', errors='ignore')
        links = [line.strip() for line in content.splitlines() if line.strip()]
        if not links: st.warning(f"File '{uploaded_file.name}' is empty or contains no text lines.")
    except Exception as e:
        st.error(f"Error reading file {uploaded_file.name}: {e}")
        logging.error(f"Error reading file {uploaded_file.name}: {e}", exc_info=True)
    return links

def load_keywords_from_excel(uploaded_file):
    keywords = []
    if not uploaded_file: return keywords
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty:
            st.warning(f"Excel file '{uploaded_file.name}' is empty.")
            return keywords
        keywords = df.iloc[:, 0].dropna().astype(str).tolist()
        if not keywords: st.warning(f"No keywords found in the first column of '{uploaded_file.name}'.")
    except Exception as e:
        st.error(f"Error reading Excel file {uploaded_file.name}: {e}. (Ensure 'openpyxl' is installed)")
        logging.error(f"Error reading Excel {uploaded_file.name}: {e}", exc_info=True)
    return keywords

# --- HTML Table Generation ---
def generate_html_table_output(active_results_df):
    if active_results_df.empty: return "<p>No active groups found to generate HTML table.</p>"
    html_lines = ['<table class="my-table">', 
                  '  <thead><tr><th>Logo</th><th>Name</th><th>Link</th></tr></thead>', 
                  '  <tbody>']
    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = html_converter.escape(row.get("Group Name", "N/A"))
        group_link = html_converter.escape(row.get("Group Link", ""))
        
        logo_html = f'<img src="{append_query_param(logo_url, "w", "80")}" alt="Logo" class="group-logo-html-table">' if logo_url else " "
        link_html = f'<a href="{group_link}" target="_blank" rel="noopener noreferrer">Join Group</a>' if group_link else "N/A"
        html_lines.append(f'    <tr><td>{logo_html}</td><td>{group_name}</td><td>{link_html}</td></tr>')
    html_lines.extend(['  </tbody>', '</table>'])
    return "\n".join(html_lines)

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

    # Initialize session state variables
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV for Validation)"
        ], key="input_method_main_select", index=0)

        # --- Google Search Settings ---
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)"]:
            st.subheader("Google Search Settings")
            google_results_slider_top_n = st.slider("Number of Google results to process", 1, 20, 5, key="google_top_n_slider")
            google_search_pause = st.slider("Google Search Pause (s)", 1.0, 10.0, 2.0, 0.5, key="google_pause_slider")
        else: # Set defaults if not shown
            google_results_slider_top_n = 5
            google_search_pause = 2.0

        # --- Website Crawl Settings ---
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.subheader("Website Crawl Settings")
            st.warning("⚠️ Extensive crawling can be slow. Start with small values.", icon="🚨")
            crawl_depth_val = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider")
            unlimited_crawl_ui = st.checkbox("Unlimited pages (caution!)", False, key="unlimited_crawl_cb")
            if unlimited_crawl_ui:
                max_crawl_pages_val_ui = None
                st.info("Page limit disabled. Crawling may take a very long time.")
            else:
                max_crawl_pages_val_ui = st.slider("Max Pages to Crawl", 1, 500, 50, key="crawl_pages_slider")
        else: # Set defaults if not shown
            crawl_depth_val = 2
            max_crawl_pages_val_ui = 50
            unlimited_crawl_ui = False


        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.success("All results and cache cleared!")
            st.rerun()

    # --- Action Zone ---
    all_scraped_links_set = set() # Use a set for automatic deduplication
    st.subheader(f"🚀 Action Zone: {input_method}")
    
    # Initialize sessions to None, create only if needed
    general_purpose_session = None
    crawl_session = None 

    try:
        if input_method == "Search and Scrape from Google":
            keyword_gs = st.text_input("Enter Search Query:", placeholder="e.g., Tech WhatsApp groups", key="gs_keyword_input")
            if st.button("Search, Scrape, and Validate", use_container_width=True, key="gs_button"):
                if not keyword_gs.strip(): st.warning("Please enter a search query.")
                else:
                    search_page_urls = google_search_user_original(keyword_gs, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                    if search_page_urls:
                        st.info(f"Found {len(search_page_urls)} webpages. Scraping links...")
                        prog_bar = st.progress(0)
                        for i, page_url in enumerate(search_page_urls):
                            st.sidebar.text(f"Scraping (orig): {urlparse(page_url).netloc}{urlparse(page_url).path[:30]}...")
                            links_from_page = scrape_whatsapp_links_user_original(page_url)
                            all_scraped_links_set.update(links_from_page)
                            prog_bar.progress((i + 1) / len(search_page_urls))
                        st.success(f"Scraping complete. Found {len(all_scraped_links_set)} unique potential links.")

        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file_bulk = st.file_uploader("Upload Excel (keywords in 1st column)", type=["xlsx"], key="gs_bulk_excel_upload")
            if excel_file_bulk:
                if st.button("Process Excel & Scrape from Google", use_container_width=True, key="gs_bulk_button"):
                    keywords_bulk = load_keywords_from_excel(excel_file_bulk)
                    if keywords_bulk:
                        st.info(f"Processing {len(keywords_bulk)} keywords...")
                        prog_bulk, stat_txt_bulk = st.progress(0), st.empty()
                        for i, kw in enumerate(keywords_bulk):
                            stat_txt_bulk.write(f"Keyword: **{kw}** ({i+1}/{len(keywords_bulk)})")
                            search_urls = google_search_user_original(kw, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                            if search_urls:
                                for s_url in search_urls:
                                    st.sidebar.text(f"Scraping (orig) for '{kw}': {urlparse(s_url).netloc}{urlparse(s_url).path[:20]}...")
                                    all_scraped_links_set.update(scrape_whatsapp_links_user_original(s_url))
                            prog_bulk.progress((i + 1) / len(keywords_bulk))
                        stat_txt_bulk.success(f"Bulk processing complete. Found {len(all_scraped_links_set)} unique potential links.")
                    else: st.warning("No keywords processed from Excel.")

        elif input_method == "Scrape from Specific Webpage URL":
            page_url_specific = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if not page_url_specific.strip() or not (page_url_specific.startswith("http://") or page_url_specific.startswith("https://")):
                    st.warning("Please enter a valid URL.")
                else:
                    general_purpose_session = requests.Session()
                    with st.spinner(f"Scraping {page_url_specific}..."):
                        links_from_page = scrape_whatsapp_links_enhanced(page_url_specific, general_purpose_session)
                        all_scraped_links_set.update(links_from_page)
                    st.success(f"Scraping complete. Found {len(links_from_page)} potential links.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url_crawl = st.text_input("Enter Base Domain URL:", placeholder="example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape Website", use_container_width=True, key="crawl_button"):
                if not domain_url_crawl.strip(): st.warning("Please enter a base domain URL.")
                else:
                    actual_max_pages = None if unlimited_crawl_ui else max_crawl_pages_val_ui
                    crawled_pages, crawl_session = crawl_website(domain_url_crawl, max_depth=crawl_depth_val, max_pages=actual_max_pages)
                    if crawled_pages and crawl_session:
                        st.info(f"Crawled {len(crawled_pages)} pages. Now scraping them...")
                        prog_crawl_scrape, stat_txt_crawl_scrape = st.progress(0), st.empty()
                        for i, c_page_url in enumerate(crawled_pages):
                            stat_txt_crawl_scrape.text(f"Scraping (enh): {urlparse(c_page_url).path[:50]}... ({i+1}/{len(crawled_pages)})")
                            all_scraped_links_set.update(scrape_whatsapp_links_enhanced(c_page_url, crawl_session))
                            prog_crawl_scrape.progress((i + 1) / len(crawled_pages))
                        st.success(f"Website scraping complete. Found {len(all_scraped_links_set)} unique potential links.")
                    elif not crawled_pages: st.warning("Crawler did not find any pages to scrape.")

        elif input_method == "Enter Links Manually (for Validation)":
            links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=150, placeholder=f"e.g., {WHATSAPP_DOMAIN}ABC123XYZ", key="manual_links_text_area")
            if st.button("Add & Validate Manual Links", use_container_width=True, key="manual_validate_button"):
                manual_links_raw = [line.strip() for line in links_text_manual.split('\n') if line.strip()]
                valid_manual_links = [l for l in manual_links_raw if l.startswith(WHATSAPP_DOMAIN)]
                ignored_count = len(manual_links_raw) - len(valid_manual_links)
                if ignored_count > 0: st.warning(f"{ignored_count} entered lines were not valid WhatsApp links and were ignored.")
                if not valid_manual_links: st.warning("No valid WhatsApp links entered.")
                else: all_scraped_links_set.update(valid_manual_links)

        elif input_method == "Upload Link File (TXT/CSV for Validation)":
            uploaded_file_val = st.file_uploader("Upload TXT or CSV file (one link per line/first column)", type=["txt", "csv"], key="upload_file_links")
            if uploaded_file_val:
                if st.button("Process File & Validate Links", use_container_width=True, key="upload_validate_button"):
                    links_from_file_raw = load_links_from_file(uploaded_file_val)
                    valid_file_links = [l for l in links_from_file_raw if l.startswith(WHATSAPP_DOMAIN)]
                    ignored_f_count = len(links_from_file_raw) - len(valid_file_links)
                    if ignored_f_count > 0: st.warning(f"{ignored_f_count} links from file were not valid WhatsApp links and were ignored.")
                    if not valid_file_links: st.warning("No valid WhatsApp links found in the file.")
                    else: all_scraped_links_set.update(valid_file_links)
    finally: # Ensure sessions are closed
        if general_purpose_session: general_purpose_session.close(); logging.info("Closed general_purpose_session.")
        if crawl_session: crawl_session.close(); logging.info("Closed crawl_session.")

    # --- Unified Validation Step ---
    if all_scraped_links_set:
        new_links_to_validate = list(all_scraped_links_set - st.session_state.processed_links_in_session)
        if not new_links_to_validate:
            st.info("No new WhatsApp links to validate, or all found links were already processed.")
        else:
            st.success(f"Found {len(all_scraped_links_set)} total unique links. Validating {len(new_links_to_validate)} new links...")
            prog_val, stat_val = st.progress(0), st.empty()
            validation_results_batch = []
            with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                future_to_link = {executor.submit(validate_link, link): link for link in new_links_to_validate}
                for i, future in enumerate(as_completed(future_to_link)):
                    link = future_to_link[future]
                    try:
                        result = future.result()
                        validation_results_batch.append(result)
                        st.session_state.processed_links_in_session.add(link) # Mark as processed
                        stat_val.text(f"Validated {i+1}/{len(new_links_to_validate)}: ...{link[-25:]} ({result.get('Status', 'Unknown')})")
                    except Exception as exc: # Should be rare as validate_link itself catches errors
                        logging.error(f"Critical error during future.result() for {link}: {exc}", exc_info=True)
                        validation_results_batch.append({"Group Name": "Validation System Error", "Group Link": link, "Logo URL": "", "Status": "Failed Validation"})
                    prog_val.progress((i + 1) / len(new_links_to_validate))
            
            # Append new results to existing session state results
            # Deduplicate based on 'Group Link' after adding, keeping the latest validation
            current_results_df = pd.DataFrame(st.session_state.results)
            new_results_df = pd.DataFrame(validation_results_batch)
            combined_df = pd.concat([current_results_df, new_results_df], ignore_index=True)
            if not combined_df.empty:
                combined_df.drop_duplicates(subset=['Group Link'], keep='last', inplace=True)
                st.session_state.results = combined_df.to_dict('records')
            
            stat_val.success(f"Validation complete for {len(new_links_to_validate)} new links!")
            st.rerun() # Rerun to update display with new results immediately

    # --- Display Results ---
    if st.session_state.results:
        df_results = pd.DataFrame(st.session_state.results)
        if df_results.empty:
            st.info("No results to display yet.")
            return

        st.subheader("📊 Results Summary")
        # Ensure 'Status' column exists and is string type for consistent operations
        df_results['Status'] = df_results['Status'].astype(str)
        
        active_df = df_results[df_results['Status'].str.contains("Active", case=False, na=False)].copy()
        expired_df = df_results[df_results['Status'].str.contains("Expired|Invalid|Full", case=False, na=False)].copy()
        # All others are considered 'Error' or 'Other'
        other_statuses = ['Active', 'Potentially Active (No Logo)'] # Consider these as non-error/non-expired for this count
        error_df = df_results[
            ~df_results['Status'].isin(other_statuses) & \
            ~df_results['Status'].str.contains("Expired|Invalid|Full", case=False, na=False)
        ].copy()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Links Processed", len(df_results))
        col2.metric("Active Links", len(active_df))
        col3.metric("Expired/Invalid/Full", len(expired_df))
        col4.metric("Other/Error", len(error_df))

        with st.expander("🔎 View and Filter Results", expanded=True):
            status_options = sorted(list(df_results['Status'].unique()))
            default_statuses = [s for s in ["Active", "Potentially Active (No Logo)"] if s in status_options]
            
            status_filter = st.multiselect("Filter by Status", options=status_options, default=default_statuses)
            name_filter = st.text_input("Filter by Group Name (contains, case-insensitive):")

            filtered_df = df_results.copy() # Start with all results
            if status_filter:
                filtered_df = filtered_df[filtered_df['Status'].isin(status_filter)]
            if name_filter:
                # Ensure 'Group Name' is string for filtering
                filtered_df['Group Name'] = filtered_df['Group Name'].astype(str)
                filtered_df = filtered_df[filtered_df['Group Name'].str.contains(name_filter, case=False, na=False)]
            
            st.dataframe(
                filtered_df[['Logo URL', 'Group Name', 'Group Link', 'Status']],
                column_config={
                    "Logo URL": st.column_config.ImageColumn("Logo", width="small"),
                    "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join", width="medium"),
                    "Status": st.column_config.TextColumn("Status", width="medium")
                }, height=400, use_container_width=True
            )
        
        st.subheader("📋 HTML Table Export (Active Groups)")
        if not active_df.empty:
            html_table_export = generate_html_table_output(active_df)
            with st.expander("Copy or Download HTML Table", expanded=False):
                st.text_area("HTML Table (Copy this):", value=html_table_export, height=200, key="html_export_area")
                st.download_button("📥 Download HTML (.html)", html_table_export, "active_whatsapp_groups.html", "text/html", use_container_width=True)
            with st.expander("📋 HTML Table Preview", expanded=True): 
                 st.markdown(html_table_export, unsafe_allow_html=True)
        else: st.info("No 'Active' groups found to generate HTML table.")
        
        st.subheader("💾 Download Raw Data (CSV)")
        col_dl1, col_dl2 = st.columns(2)
        if not active_df.empty:
            csv_active = active_df.to_csv(index=False).encode('utf-8')
            col_dl1.download_button("📥 Active Groups (CSV)", csv_active, "active_groups.csv", "text/csv", use_container_width=True)
        else: col_dl1.button("📥 Active Groups (CSV)", disabled=True, use_container_width=True)
        
        csv_all = df_results.to_csv(index=False).encode('utf-8')
        col_dl2.download_button("📥 All Results (CSV)", csv_all, "all_results.csv", "text/csv", use_container_width=True)
    else:
        st.info("No results yet. Start by choosing an input method and processing some links.", icon="ℹ️")

# --- Application Entry Point ---
if __name__ == "__main__":
    # Pre-flight checks for essential libraries
    libs_ok = True
    try: import openpyxl
    except ImportError: st.error("Required library 'openpyxl' is missing. Please install: pip install openpyxl"); libs_ok = False
    
    # Test fake-useragent basic import and instantiation
    try: UserAgent()
    except ImportError: st.warning("Library 'fake-useragent' is missing. Scraping may be less effective. Install: pip install fake-useragent", icon="⚠️")
    except Exception as ua_exc: st.warning(f"Fake-useragent had an issue during test ({type(ua_exc).__name__}: {ua_exc}). Using default User-Agent.", icon="⚠️")
        
    if libs_ok:
        main()
    else:
        st.stop()
