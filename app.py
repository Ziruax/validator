import streamlit as st
import pandas as pd
import requests
from html import unescape
import html as html_converter # For escaping HTML content
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
    st.error(f"Could not initialize Fake UserAgent for general scraping, using a default. Error: {type(e).__name__} - {e}")
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
MAX_VALIDATION_WORKERS = 10

# --- Custom CSS ---
st.markdown("""
<style>
/* General Styles */
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; }
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }
img.group-logo-markdown { width:35px; height:35px; border-radius:50%; object-fit:cover; vertical-align:middle; margin-right: 5px; }

/* Table Styles (Fix #3) */
table.my-table { 
    width: 100%; 
    border-collapse: collapse; 
    font-family: sans-serif;
    margin-bottom: 1em; /* Added some margin for better spacing */
}
table.my-table th, table.my-table td {
    border: 1px solid #ccc;
    padding: 8px;
    text-align: left;
    vertical-align: middle; /* Align content vertically */
}
table.my-table th {
    background: #25D366; /* WhatsApp Green */
    color: white;
    font-weight: bold;
}
table.my-table tr:nth-child(even) {
    background: #f9f9f9;
}
table.my-table img { /* Ensure images within table cells are also vertically aligned nicely if needed */
    vertical-align: middle;
}
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def get_random_headers_for_general_use():
    """Returns headers with a random User-Agent for general scraping/validation."""
    return {
        "User-Agent": ua_general.random(),
        "Accept-Language": "en-US,en;q=0.9"
    }

def append_query_param(url, param_name, param_value):
    """Appends a query parameter to a URL."""
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
        st.sidebar.info(f"Googling (user original) '{query}' (top {top_n}, pause: {pause_duration}s)...")
        urls = list(google_search_library(
            query,
            lang="en",
            num=top_n,
            stop=top_n,
            pause=pause_duration
        ))
        if not urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms.")
        return urls
    except Exception as e:
        st.error(f"Google Search error (user original): {type(e).__name__} - {str(e)}")
        return []

def scrape_whatsapp_links_user_original(url):
    """Scrape WhatsApp group links from a webpage. (User's original function)"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8' # Ensure correct encoding
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set() # Use a set to avoid duplicates initially
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links.add(a['href'].split('?')[0])
        for text in soup.stripped_strings: # More efficient than soup.get_text() for this
            if WHATSAPP_DOMAIN in text:
                # Regex to find WhatsApp links more robustly in text
                found_links = re.findall(r'https?://chat\.whatsapp\.com/[A-Za-z0-9_-]+', text)
                for flink in found_links:
                    links.add(flink.split('?')[0])
        return list(links)
    except requests.exceptions.RequestException as e:
        st.sidebar.warning(f"Network error (orig) on {urlparse(url).netloc}: {type(e).__name__}", icon="🌐")
        return []
    except Exception as e:
        st.sidebar.warning(f"Scrape error (orig) on {urlparse(url).netloc}: {type(e).__name__} - {str(e)}", icon="⚠️")
        return []
# --- END of functions from USER'S WORKING EXAMPLE ---


# --- Enhanced scraping function (for Specific Page / Entire Website) ---
def scrape_whatsapp_links_enhanced(url, session):
    links = set()
    try:
        netloc_for_error = urlparse(url).netloc or url[:30]
        response = session.get(url, headers=get_random_headers_for_general_use(), timeout=15)
        response.encoding = 'utf-8' # Ensure correct encoding
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0])
        for text_chunk in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text_chunk:
                found_in_chunk = re.findall(r'https?://chat\.whatsapp\.com/[A-Za-z0-9_-]+', text_chunk)
                for link_url in found_in_chunk: links.add(link_url.split('?')[0])
    except requests.exceptions.Timeout: st.sidebar.warning(f"Timeout (enh) scraping {netloc_for_error}", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"HTTP error (enh) {e.response.status_code} scraping {netloc_for_error}", icon="📉")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Network error (enh) scraping {netloc_for_error}: {type(e).__name__}", icon="🌐")
    except Exception as e: st.sidebar.warning(f"Parse error (enh) scraping {netloc_for_error}: {type(e).__name__} - {str(e)[:50]}", icon="💣")
    return list(links)

# --- Validation function (uses fake UA) ---
def validate_link(link):
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        response = requests.get(link, headers=get_random_headers_for_general_use(), timeout=10, allow_redirects=True)
        response.encoding = 'utf-8' # Ensure correct encoding
        
        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result
        # Check if the final URL after redirects is still a WhatsApp chat link
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid Link (Redirected)"
            return result
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title_tag = soup.find('meta', property='og:title')
        
        if meta_title_tag and meta_title_tag.get('content'):
            group_name_content = unescape(meta_title_tag['content']).strip()
            result["Group Name"] = group_name_content
            
            # Heuristics for expired/invalid links based on title
            gn_lower = group_name_content.lower()
            if "whatsapp group invite" == gn_lower or \
               "group no longer available" in gn_lower or \
               "couldn't join" in gn_lower or \
               "invite link was reset" in gn_lower or \
               "you can't join this group because it is full" in gn_lower:
                result["Status"] = "Expired/Invalid" # More specific status
        else:
            result["Group Name"] = "Unnamed Group / Error Page"
            result["Status"] = "Expired/Invalid" # Often indicates an error or invalid link page

        img_tags = soup.find_all('img', src=True)
        logo_found = False
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN_SHARED.match(src):
                result["Logo URL"] = src
                # If a logo is found and status wasn't set by title heuristics to Expired/Invalid, assume Active
                if result["Status"] not in ["Expired/Invalid", f"HTTP Error {response.status_code}", "Invalid Link (Redirected)"]:
                    result["Status"] = "Active"
                logo_found = True
                break
        
        # If no logo was found and status is still default "Error" or "Unknown", and not Expired/Invalid by title
        if not logo_found and result["Status"] in ["Error", "Unknown", "Unnamed Group / Error Page"]:
            result["Status"] = "Potentially Expired (No Logo)"
        elif not logo_found and result["Status"] == "Active": # Should not happen if Active is set only on logo find
            result["Status"] = "Potentially Expired (No Logo)"


    except requests.exceptions.Timeout: result["Status"] = "Network Error: Timeout"
    except requests.exceptions.ConnectionError: result["Status"] = "Network Error: Connection"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error: {type(e).__name__}"
    except Exception as e: result["Status"] = f"Parsing Error: {type(e).__name__}"
    return result


def crawl_website(start_url, max_depth=3, max_pages=None):
    if not start_url.startswith(('http://', 'https://')): start_url = 'https://' + start_url
    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    urls_to_visit, visited_urls, scraped_content_urls = [(start_url, 0)], set(), set()
    session = requests.Session() # Create session for the crawl
    crawl_message_page_limit = 'Unlimited' if max_pages is None else str(max_pages)

    with st.spinner(f"Crawling {base_domain} (Depth limit: {max_depth}, Page limit: {crawl_message_page_limit})..."):
        page_count = 0
        while urls_to_visit and (max_pages is None or page_count < max_pages):
            current_url, depth = urls_to_visit.pop(0)
            if current_url in visited_urls or depth > max_depth: continue
            
            # Normalize URL to avoid re-visiting slight variations
            norm_url = urljoin(current_url, urlparse(current_url).path) # Basic normalization
            if norm_url in visited_urls: continue
            visited_urls.add(norm_url)
            visited_urls.add(current_url) # Add original too

            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}/{crawl_message_page_limit if max_pages is not None else '∞'}): {current_url[:60]}...")
            try:
                response = session.get(current_url, headers=get_random_headers_for_general_use(), timeout=10, allow_redirects=True)
                response.encoding = 'utf-8'
                response.raise_for_status()
                
                # Ensure we are still on the same domain after potential redirects
                if urlparse(response.url).netloc != base_domain:
                    st.sidebar.text(f"Redirected off-domain: {current_url} -> {response.url}")
                    continue

                scraped_content_urls.add(current_url); page_count += 1
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        abs_url = urljoin(current_url, link_tag['href'])
                        parsed_abs_url = urlparse(abs_url)
                        
                        # Filter out non-HTTP links, off-domain links, and fragments
                        if parsed_abs_url.scheme not in ['http', 'https'] or \
                           parsed_abs_url.netloc != base_domain or \
                           not parsed_abs_url.path or \
                           parsed_abs_url.fragment: # Ignore fragment links
                            continue
                        
                        norm_abs_url = urljoin(abs_url, parsed_abs_url.path) # Normalize before adding to queue

                        if norm_abs_url not in visited_urls and all(uv[0] != norm_abs_url for uv in urls_to_visit):
                             # Check if a slight variation is already in urls_to_visit
                            is_present = False
                            for u_visit, _ in urls_to_visit:
                                if urljoin(u_visit, urlparse(u_visit).path) == norm_abs_url:
                                    is_present = True
                                    break
                            if not is_present:
                                urls_to_visit.append((norm_abs_url, depth + 1))

            except requests.exceptions.Timeout: st.sidebar.warning(f"Crawl timeout: {urlparse(current_url).path}", icon="⏱️")
            except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Crawl HTTP {e.response.status_code}: {urlparse(current_url).path}", icon="📉")
            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl net-err: {type(e).__name__} on {urlparse(current_url).path}", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl general err: {type(e).__name__} on {urlparse(current_url).path}", icon="💥")
    st.sidebar.success(f"Crawler found {len(scraped_content_urls)} pages matching criteria.")
    return list(scraped_content_urls), session


def load_links_from_text_file(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        try:
            df = pd.read_csv(uploaded_file)
            if df.empty:
                st.warning(f"CSV file '{uploaded_file.name}' is empty.")
                return []
            return df.iloc[:, 0].dropna().astype(str).tolist()
        except pd.errors.EmptyDataError:
            st.warning(f"CSV file '{uploaded_file.name}' is empty or has no data.")
            return []
        except Exception as e:
            st.error(f"Error reading CSV {uploaded_file.name}: {e}")
            return []
    else:
        try:
            content = uploaded_file.read().decode('utf-8', errors='ignore') # Added utf-8 and ignore errors
            return [line.strip() for line in content.splitlines() if line.strip()]
        except Exception as e:
            st.error(f"Error reading TXT {uploaded_file.name}: {e}")
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

def generate_html_table_output(active_results_df):
    if active_results_df.empty: return "<p>No active groups found to generate HTML table.</p>"
    
    html_lines = ['<table class="my-table">', 
                  '  <thead>', 
                  '    <tr><th>Logo</th><th>Name</th><th>Link</th></tr>', 
                  '  </thead>', 
                  '  <tbody>']
    
    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", "N/A")
        safe_group_name = html_converter.escape(group_name) 
        group_link = row.get("Group Link", "")
        
        if logo_url:
            resized_logo_url_server = append_query_param(logo_url, 'w', '80')
            logo_html = f'<img src="{resized_logo_url_server}" alt="Logo" class="group-logo-markdown">'
        else:
            logo_html = " "
            
        link_html = f'<a href="{group_link}" target="_blank" rel="noopener noreferrer">Join Group</a>'
        
        html_lines.append(f'    <tr><td>{logo_html}</td><td>{safe_group_name}</td><td>{link_html}</td></tr>')
    
    html_lines.append('  </tbody>')
    html_lines.append('</table>')
    return "\n".join(html_lines)

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Enhanced tool to find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

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
                "Number of top Google results to scrape from", 
                min_value=1, max_value=20, value=5, key="google_top_n_slider"
            )
            google_search_pause = st.slider(
                "Google Search Pause (seconds):", min_value=1.0, max_value=10.0, value=2.0, step=0.5,
                help="Pause between Google search API calls to avoid rate-limiting.", key="google_pause_slider"
            )
        
        crawl_depth_val, max_crawl_pages_val_ui = 2, 50
        unlimited_crawl_ui = False
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive website crawling can be very slow and resource-intensive. Use with caution.", icon="🚨")
            crawl_depth_val = st.slider("Max Crawl Depth:", min_value=0, max_value=5, value=2, key="crawl_depth_slider")
            
            unlimited_crawl_ui = st.checkbox("Attempt unlimited page crawl (VERY SLOW, use cautiously)?", False, key="unlimited_crawl_cb")
            if unlimited_crawl_ui:
                st.info("Page limit disabled. Crawling can take a very long time or hit server limits.")
                max_crawl_pages_val_ui = None
            else:
                max_crawl_pages_val_ui = st.slider("Max Pages to Crawl:", min_value=1, max_value=500, value=50, key="crawl_pages_slider")
        
        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.rerun()

    all_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")
    
    general_purpose_session = None
    crawl_session_obj = None

    try:
        if input_method in ["Scrape from Specific Webpage URL"]:
             general_purpose_session = requests.Session()

        if input_method == "Search and Scrape from Google":
            keyword_gs = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_keyword_input")
            if st.button("Search, Scrape, and Validate", use_container_width=True, key="gs_button"):
                if not keyword_gs: st.warning("Please enter a search query.")
                else:
                    with st.spinner("Searching Google (user original method)..."):
                        search_page_urls = google_search_user_original(keyword_gs, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                    if search_page_urls:
                        st.success(f"Found {len(search_page_urls)} webpages. Scraping WhatsApp links (user original method)...")
                        prog_bar_gs = st.progress(0)
                        for i, page_url in enumerate(search_page_urls):
                            st.sidebar.text(f"Scraping (orig): {urlparse(page_url).netloc}{urlparse(page_url).path[:30]}...")
                            links_from_page = scrape_whatsapp_links_user_original(page_url)
                            all_scraped_links.update(links_from_page)
                            prog_bar_gs.progress((i+1)/len(search_page_urls))
                        st.success(f"Google page scraping complete. Found {len(all_scraped_links)} potential links.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file_bulk = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if excel_file_bulk and st.button("Process Excel & Scrape from Google", use_container_width=True, key="gs_bulk_button"):
                keywords_bulk = load_keywords_from_excel(excel_file_bulk)
                if not keywords_bulk: st.warning("No keywords found or file is empty.")
                else:
                    st.info(f"Processing {len(keywords_bulk)} keywords. Starting Google searches & scraping (user original methods)...")
                    prog_bulk, stat_txt_bulk = st.progress(0), st.empty()
                    total_links_from_bulk = 0
                    for i, kw_bulk in enumerate(keywords_bulk):
                        stat_txt_bulk.write(f"Keyword: **{kw_bulk}** ({i+1}/{len(keywords_bulk)})")
                        search_page_urls_bulk = google_search_user_original(kw_bulk, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                        if search_page_urls_bulk:
                            for page_idx, page_url_bulk in enumerate(search_page_urls_bulk):
                                st.sidebar.text(f"Scraping (orig) {page_idx+1}/{len(search_page_urls_bulk)} for '{kw_bulk}': {urlparse(page_url_bulk).netloc}{urlparse(page_url_bulk).path[:20]}...")
                                links_from_page_bulk = scrape_whatsapp_links_user_original(page_url_bulk)
                                new_links_count = len(set(links_from_page_bulk) - all_scraped_links)
                                all_scraped_links.update(links_from_page_bulk)
                                total_links_from_bulk += new_links_count
                        prog_bulk.progress((i + 1) / len(keywords_bulk))
                    stat_txt_bulk.success(f"Bulk Google processing complete. Found {total_links_from_bulk} new potential links.")

        elif input_method == "Scrape from Specific Webpage URL":
            page_url_specific = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
            if st.button("Scrape Page (Enhanced Method) & Validate", use_container_width=True, key="specific_url_button"):
                if not page_url_specific or not (page_url_specific.startswith("http://") or page_url_specific.startswith("https://")):
                    st.warning("Please enter a valid URL starting with http:// or https://.")
                else:
                    with st.spinner(f"Scraping {page_url_specific} (enhanced method)..."):
                        links_from_page_spec = scrape_whatsapp_links_enhanced(page_url_specific, general_purpose_session)
                        all_scraped_links.update(links_from_page_spec)
                    st.success(f"Scraping of {page_url_specific} complete. Found {len(links_from_page_spec)} potential links.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url_crawl = st.text_input("Enter Base Domain URL:", placeholder="example.com or https://example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape (Enhanced Method)", use_container_width=True, key="crawl_button"):
                if not domain_url_crawl: st.warning("Please enter a domain URL.")
                else:
                    current_max_pages = None if unlimited_crawl_ui else max_crawl_pages_val_ui
                    pages_to_scrape_crawl, crawl_session_obj = crawl_website(domain_url_crawl, max_depth=crawl_depth_val, max_pages=current_max_pages)
                    if pages_to_scrape_crawl:
                        st.info(f"Crawled. Now scraping {len(pages_to_scrape_crawl)} pages (enhanced method)...")
                        prog_crawl, stat_txt_crawl = st.progress(0), st.empty()
                        links_from_crawl_total = 0
                        for i, p_url_crawl in enumerate(pages_to_scrape_crawl):
                            stat_txt_crawl.text(f"Scraping (enh): {urlparse(p_url_crawl).path[:50]}... ({i+1}/{len(pages_to_scrape_crawl)})")
                            links_from_page_crawl = scrape_whatsapp_links_enhanced(p_url_crawl, crawl_session_obj)
                            new_links_count_crawl = len(set(links_from_page_crawl) - all_scraped_links)
                            all_scraped_links.update(links_from_page_crawl)
                            links_from_crawl_total += new_links_count_crawl
                            prog_crawl.progress((i + 1) / len(pages_to_scrape_crawl))
                        stat_txt_crawl.success(f"Website scraping complete. Found {links_from_crawl_total} new potential links.")
                    else: st.warning("No pages found/scraped from domain.")
        
        elif input_method == "Enter Links Manually (for Validation)":
            links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123", key="manual_links_text_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                raw_links_manual = [line.strip() for line in links_text_manual.split('\n') if line.strip()]
                links_manual = [l for l in raw_links_manual if l.startswith(WHATSAPP_DOMAIN)]
                invalid_entered_count = len(raw_links_manual) - len(links_manual)
                if invalid_entered_count > 0:
                    st.warning(f"{invalid_entered_count} entered line(s) were not valid WhatsApp link formats and were ignored.")
                if not links_manual: st.warning("Please enter at least one valid WhatsApp link.")
                else: all_scraped_links.update(links_manual)


        elif input_method == "Upload Link File (TXT/CSV for Validation)":
            uploaded_file_val = st.file_uploader("Upload TXT or CSV (one link per line, or first column for CSV)", type=["txt", "csv"], key="upload_file_links")
            if uploaded_file_val and st.button("Validate File Links", use_container_width=True, key="upload_validate_button"):
                links_from_file_raw = load_links_from_text_file(uploaded_file_val)
                valid_links_from_file = [l for l in links_from_file_raw if l.startswith(WHATSAPP_DOMAIN)]
                invalid_file_count = len(links_from_file_raw) - len(valid_links_from_file)
                if invalid_file_count > 0:
                    st.warning(f"{invalid_file_count} link(s) from file were not valid WhatsApp link formats and were ignored.")
                if not valid_links_from_file: st.warning("No valid WhatsApp links found in the uploaded file.")
                else: all_scraped_links.update(valid_links_from_file)
    finally:
        if general_purpose_session: general_purpose_session.close()
        if crawl_session_obj: crawl_session_obj.close()


    # --- Unified Validation Step ---
    if all_scraped_links:
        links_to_validate_now = list(all_scraped_links - st.session_state.processed_links_in_session)
        if not links_to_validate_now:
            st.info("No new WhatsApp links found or all previously found links already processed.")
        else:
            st.success(f"Found {len(all_scraped_links)} total unique potential links. Validating {len(links_to_validate_now)} new links...")
            prog_val, stat_val = st.progress(0), st.empty()
            new_results_validation = []
            with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
                for i, future in enumerate(as_completed(future_to_link)):
                    link_validated = future_to_link[future]
                    current_result_status = "Unknown"
                    try:
                        result_validated = future.result()
                        new_results_validation.append(result_validated)
                        current_result_status = result_validated.get('Status', 'Unknown')
                    except Exception as exc: # Should be rare as validate_link handles its own errors
                        st.error(f"Critical error validating link {link_validated}: {exc}")
                        new_results_validation.append({"Group Name": "Validation Error", "Group Link": link_validated, "Logo URL": "", "Status": "Validation Failed"})
                        current_result_status = "Validation Failed"
                    
                    st.session_state.processed_links_in_session.add(link_validated)
                    prog_val.progress((i + 1) / len(links_to_validate_now))
                    stat_val.text(f"Validated {i + 1}/{len(links_to_validate_now)}: {urlparse(link_validated).path[1:25]}... ({current_result_status})")
            st.session_state.results.extend(new_results_validation)
            stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")


    # --- Display Results ---
    if 'results' in st.session_state and st.session_state.results:
        df_results_display = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first').reset_index(drop=True)
        st.session_state.results = df_results_display.to_dict('records')
        
        active_df_display = df_results_display[df_results_display['Status'] == 'Active'].copy()
        expired_invalid_df_display = df_results_display[df_results_display['Status'].str.contains("Expired|Invalid|Full", case=False, na=False)].copy()
        error_df_display = df_results_display[
            ~df_results_display['Status'].isin(['Active']) & \
            ~df_results_display['Status'].str.contains("Expired|Invalid|Full", case=False, na=False)
        ].copy()
        
        st.subheader("📊 Results Summary")
        col1_disp, col2_disp, col3_disp, col4_disp = st.columns(4)
        with col1_disp: st.metric("Total Links Processed", len(df_results_display))
        with col2_disp: st.metric("Active Links", len(active_df_display))
        with col3_disp: st.metric("Expired/Invalid/Full", len(expired_invalid_df_display))
        with col4_disp: st.metric("Other/Error", len(error_df_display))

        with st.expander("🔎 View and Filter Results", expanded=True):
            status_filter_options = sorted(list(df_results_display['Status'].unique()))
            default_selection = ["Active"] if "Active" in status_filter_options else status_filter_options[:1]
            status_filter_val = st.multiselect("Filter by Status", options=status_filter_options, default=default_selection)
            
            name_filter_text = st.text_input("Filter by Group Name (contains, case-insensitive):")

            filtered_df_for_display = df_results_display
            if status_filter_val:
                filtered_df_for_display = filtered_df_for_display[filtered_df_for_display['Status'].isin(status_filter_val)]
            if name_filter_text:
                filtered_df_for_display = filtered_df_for_display[
                    filtered_df_for_display['Group Name'].str.contains(name_filter_text, case=False, na=False)
                ]
            
            st.dataframe(
                filtered_df_for_display[['Logo URL', 'Group Name', 'Group Link', 'Status']],
                column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Group", width="medium"),
                    "Logo URL": st.column_config.ImageColumn("Logo", width="small"),
                    "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                    "Status": st.column_config.TextColumn("Status", width="medium")
                },
                height=400,
                use_container_width=True
            )
        
        st.subheader("📋 HTML Table Export (Active Groups)")
        if not active_df_display.empty:
            html_table_data_export = generate_html_table_output(active_df_display)
            with st.expander("Copy or Download HTML Table", expanded=True):
                st.text_area("HTML Table (Copy this for embedding):", value=html_table_data_export, height=250, key="html_export_area", help="Ctrl+A then Ctrl+C to copy the HTML code.")
                st.download_button("📥 Download HTML Table (.html)", html_table_data_export, "active_groups_table.html", "text/html", use_container_width=True, key="html_export_download")
            with st.expander("📋 HTML Table Preview", expanded=False): 
                 st.markdown(html_table_data_export, unsafe_allow_html=True)
        else: 
            st.info("No active groups found to generate HTML table output.")
        
        st.subheader("💾 Download Raw Data")
        col_dl1_orig, col_dl2_orig = st.columns(2)
        with col_dl1_orig:
            if not active_df_display.empty:
                csv_active_orig = active_df_display.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Active Groups (CSV)", csv_active_orig, "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv_orig")
            else:
                st.button("📥 Download Active Groups (CSV)", disabled=True, use_container_width=True, key="dl_active_csv_orig_disabled")
        with col_dl2_orig:
            if not df_results_display.empty:
                csv_all_orig = df_results_display.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download All Results (CSV)", csv_all_orig, "all_groups.csv", "text/csv", use_container_width=True, key="dl_all_csv_orig")
            else:
                st.button("📥 Download All Results (CSV)", disabled=True, use_container_width=True, key="dl_all_csv_orig_disabled")

    else:
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")

if __name__ == "__main__":
    libraries_ok = True
    try:
        import openpyxl
    except ImportError:
        st.error("Library 'openpyxl' for Excel is missing. Please install: `pip install openpyxl`")
        libraries_ok = False

    try:
        # Simplified test: just try to construct it.
        # The main UserAgent initialization at the top of the script handles more complex fallback.
        UserAgent()
    except ImportError:
        st.warning("Library 'fake-useragent' is missing. General scraping might be less effective. Install: `pip install fake-useragent`", icon="⚠️")
    except Exception as ua_exc: # Catch other exceptions like TypeError
        # Display a more informative warning, including the type and message of the error
        st.warning(f"Fake-useragent had an issue during test ({type(ua_exc).__name__}: {str(ua_exc)}). General scraping will use a default User-Agent.", icon="⚠️")

    if libraries_ok:
        main()
    else:
        st.stop()
