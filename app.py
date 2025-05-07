import streamlit as st
import pandas as pd
import requests
import html # For html.escape()
from bs4 import BeautifulSoup
import re
import time
import io
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import Google Search Library ---
try:
    from googlesearch import search as google_search_library
except ImportError:
    st.error("The `googlesearch-python` library is not installed. Please install it: `pip install googlesearch-python`")
    def google_search_library(query, num_results, lang, pause):
        st.error("`googlesearch-python` library not found. Cannot perform Google searches.")
        return []

# --- Import Fake User Agent Library ---
try:
    from fake_useragent import UserAgent
    ua_general = UserAgent()
    def get_random_headers_general():
        try:
            return {
                "User-Agent": ua_general.random,
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception:
             return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
except ImportError:
    st.warning("`fake-useragent` library not found. Install with `pip install fake-useragent`. Using default User-Agent.", icon="⚠️")
    def get_random_headers_general():
         return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except Exception as e:
     st.warning(f"Error initializing fake-useragent: {e}. Using default User-Agent.", icon="⚠️")
     def get_random_headers_general():
         return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

# --- Streamlit Configuration & Constants ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
UNNAMED_GROUP_PLACEHOLDER = "Unnamed Group" # Define a constant for unnamed groups
IMAGE_PATTERN_PPS = re.compile(r'https:\/\/pps\.whatsapp\.net\/v\/t\d+\/[-\w]+\/\d+\.jpg\?')
OG_IMAGE_PATTERN = re.compile(r'https?:\/\/[^\/\s]+\/[^\/\s]+\.(jpg|jpeg|png)(\?[^\s]*)?')
MAX_VALIDATION_WORKERS = 8

# --- Custom CSS ---
st.markdown("""
<style>
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; margin: 5px 0; }
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }
.stProgress > div > div > div > div { background-color: #25D366; }
.metric-card { background-color: #F5F6F5; padding: 12px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); color: #333333; text-align: center; margin-bottom: 10px; }
/* Style for the div replacing h1 in metric card */
.metric-card .metric-value {
    font-size: 1.8em; /* Mimic h1 size */
    font-weight: bold; /* Mimic h1 weight */
    margin-top: 5px;
    margin-bottom: 0;
    line-height: 1.2; /* Adjust line height as needed */
}
.stTextInput > div > div > input, .stTextArea > div > textarea { border: 1px solid #25D366 !important; border-radius: 5px !important; padding: 8px !important; }
.st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F5F6F5; }
.stExpander { border: 1px solid #E0E0E0; border-radius: 5px; padding: 10px; margin-top: 10px; margin-bottom: 10px; }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #25D366; }
.stDataFrame table th { background-color: #25D366; color: white; }
.whatsapp-groups-table { border-collapse: collapse; width: 100%; margin-top: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; border: 1px solid #eee; }
.whatsapp-groups-table tr { border-bottom: 1px solid #eee; }
.whatsapp-groups-table tr:last-child { border-bottom: none; }
.whatsapp-groups-table td { padding: 12px; vertical-align: middle; text-align: left; }
.whatsapp-groups-table td:nth-child(1) { width: 60px; padding-right: 8px; text-align: center; }
.whatsapp-groups-table td:nth-child(2) { flex-grow: 1; padding-left: 8px; padding-right: 12px; word-break: break-word; font-weight: 500; color: #333; text-align: center; }
.whatsapp-groups-table td:nth-child(3) { width: 140px; text-align: right; padding-left: 12px; }
.group-logo-img { width: 40px; height: 40px; border-radius: 50%; object-fit: cover; display: block; margin: 0 auto; border: 1px solid #eee; }
.join-button { display: inline-block; background-color: #25D366; color: #FFFFFF !important; padding: 8px 16px; border-radius: 8px; text-decoration: none; font-weight: bold; text-align: center; white-space: nowrap; font-size: 0.9em; transition: background-color 0.2s ease; }
.join-button:hover { background-color: #1EBE5A; color: #FFFFFF !important; text-decoration: none; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def append_query_param(url, param_name, param_value):
    if not url: return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    url_without_fragment = parsed_url._replace(query=new_query_string, fragment='').geturl()
    return f"{url_without_fragment}#{parsed_url.fragment}" if parsed_url.fragment else url_without_fragment

def load_keywords_from_excel(uploaded_file):
    if uploaded_file is None: return []
    try:
        df = pd.read_excel(io.BytesIO(uploaded_file.getvalue()), engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        keywords = [kw.strip() for kw in df.iloc[:, 0].dropna().astype(str).tolist() if len(kw.strip()) > 1]
        if not keywords: st.warning("No valid keywords found in the first column of the Excel file.")
        return keywords
    except Exception as e:
        st.error(f"Error reading Excel: {e}. Ensure 'openpyxl' is installed.", icon="❌")
        return []

def load_links_from_file(uploaded_file):
    if uploaded_file is None: return []
    try:
        content = uploaded_file.getvalue()
        text_content = None
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                text_content = content.decode(encoding)
                st.sidebar.info(f"Decoded file with {encoding}.")
                break
            except UnicodeDecodeError: continue
        if text_content is None:
             st.error(f"Could not decode file {uploaded_file.name}.", icon="❌"); return []

        if uploaded_file.name.endswith('.csv'):
            try:
                 df = pd.read_csv(io.StringIO(text_content))
                 if df.empty: st.warning("CSV file is empty."); return []
                 return [link.strip() for link in df.iloc[:, 0].dropna().astype(str).tolist() if link.strip().startswith(('http://', 'https://'))]
            except Exception as e:
                 st.error(f"Error reading CSV: {e}.", icon="❌"); return []
        else: # Assume TXT
             return [line.strip() for line in text_content.splitlines() if line.strip()]
    except Exception as e:
        st.error(f"Error processing file {uploaded_file.name}: {e}", icon="❌"); return []

# --- Core Logic Functions ---
def validate_link(link):
    result = {"Group Name": UNNAMED_GROUP_PLACEHOLDER, "Group Link": link, "Logo URL": "", "Status": "Error"} # Use constant
    try:
        response = requests.get(link, headers=get_random_headers_general(), timeout=20, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            result["Status"] = "Expired (404 Not Found)" if response.status_code == 404 else f"HTTP Error {response.status_code}"
            return result
        if WHATSAPP_DOMAIN not in response.url:
            final_netloc = urlparse(response.url).netloc or 'Unknown Site'
            result["Status"] = f"Redirected Away ({final_netloc})"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        page_text_lower = soup.get_text().lower()
        expired_phrases = ["invite link is invalid", "invite link was reset", "group doesn't exist", "this group is no longer available"]
        if any(phrase in page_text_lower for phrase in expired_phrases):
            result["Status"] = "Expired"

        group_name_found = False
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = html.unescape(meta_title['content']).strip()
            if group_name: result["Group Name"] = group_name; group_name_found = True
        if not group_name_found:
             potential_name_tags = soup.find_all(['h2', 'strong', 'span'], class_=re.compile('group-name', re.IGNORECASE)) + soup.find_all('div', class_=re.compile('name', re.IGNORECASE))
             for tag in potential_name_tags:
                 text = tag.get_text().strip()
                 if text and len(text) > 2 and text.lower() not in ["whatsapp group invite", "whatsapp", "join group", "invite link"]:
                     result["Group Name"] = text; group_name_found = True; break
        # No explicit "else: result["Group Name"] = UNNAMED_GROUP_PLACEHOLDER" here, it's already the default

        logo_found = False
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
             src = html.unescape(meta_image['content'])
             if OG_IMAGE_PATTERN.match(src) or src.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                 result["Logo URL"] = src; logo_found = True
        if not logo_found:
            for img in soup.find_all('img', src=True):
                src = html.unescape(img['src'])
                if src.startswith('https://pps.whatsapp.net/'):
                    result["Logo URL"] = src; logo_found = True; break
        
        if result["Status"] == "Error":
            result["Status"] = "Active"

    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError: result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: result["Status"] = f"Parsing Error ({type(e).__name__})"
    return result

def scrape_whatsapp_links_from_page(url, session=None):
    links = set()
    try:
        headers = get_random_headers_general()
        response = session.get(url, headers=headers, timeout=15) if session else requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8'
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                parsed_url = urlparse(href)
                links.add(f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}")
        text_content = soup.get_text()
        if WHATSAPP_DOMAIN in text_content:
            for link_url in re.findall(r'(https?://chat\.whatsapp\.com/[^\s"\'<>()\[\]{}]+)', text_content):
                clean_link = re.sub(r'[.,;!?"\'<>)]+$', '', link_url)
                parsed_url = urlparse(clean_link)
                links.add(f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}")
    except requests.exceptions.Timeout: st.sidebar.warning(f"Scrape Timeout: {url[:50]}...", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Scrape HTTP Err {e.response.status_code}: {url[:50]}...", icon="⚠️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape Net Err ({type(e).__name__}): {url[:50]}...", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Scrape Parse Err ({type(e).__name__}): {url[:50]}...", icon="💣")
    return list(links)

def google_search_and_scrape(query, top_n=5, pause_duration=2.0):
    st.info(f"Googling '{query}' (top {top_n}, pause {pause_duration}s)...")
    all_links = set()
    try:
        search_page_urls = list(google_search_library(query, num_results=top_n, lang="en", pause=pause_duration))
        if not search_page_urls:
            st.warning(f"No Google results for '{query}'."); return []
        st.success(f"Found {len(search_page_urls)} pages. Scraping links...")
        prog_bar, stat_txt = st.progress(0), st.empty()
        with requests.Session() as scrape_session:
            for i, url in enumerate(search_page_urls):
                stat_txt.text(f"Scraping page {i+1}/{len(search_page_urls)}: {url[:60]}...")
                all_links.update(link for link in scrape_whatsapp_links_from_page(url, session=scrape_session) if link.startswith(WHATSAPP_DOMAIN))
                prog_bar.progress((i+1)/len(search_page_urls))
        stat_txt.success(f"Scraping complete. Found {len(all_links)} WhatsApp links.")
        return list(all_links)
    except Exception as e:
        st.error(f"Google search/scrape error: {e}", icon="❌"); return list(all_links)

def crawl_website(start_url, max_depth=2, max_pages=50):
    scraped_whatsapp_links = set()
    if not start_url.strip(): return scraped_whatsapp_links
    if not start_url.startswith(('http://', 'https://')):
         start_url = 'https://' + start_url; st.sidebar.warning(f"Prepending 'https://': {start_url}", icon="🔗")
    parsed_start_url = urlparse(start_url)
    if not parsed_start_url.netloc:
        st.sidebar.error(f"Invalid start URL: {start_url}", icon="🚫"); return scraped_whatsapp_links
    base_domain = parsed_start_url.netloc.replace('www.', '')
    urls_in_queue_tuples, visited_urls, queue_list = set(), set(), []
    queue_list.append((start_url, 0)); urls_in_queue_tuples.add((start_url, 0))
    page_count, max_q_size = 0, max_pages * 10
    with requests.Session() as session, st.spinner(f"Crawling {base_domain}..."):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size:
                 st.sidebar.warning(f"Queue > {max_q_size}. Stopping discovery.", icon="❗️"); queue_list = queue_list[:max_q_size]
            current_url, depth = queue_list.pop(0)
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')
            if normalized_current_url in visited_urls or depth > max_depth: continue
            visited_urls.add(normalized_current_url)
            if page_count >= max_pages: break
            st.sidebar.text(f"Crawl (D:{depth},P:{page_count+1},Q:{len(queue_list)}): {current_url[:50]}...")
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10)
                response.raise_for_status()
                if 'text/html' not in response.headers.get('Content-Type', '').lower(): continue
                page_count += 1
                scraped_whatsapp_links.update(link for link in scrape_whatsapp_links_from_page(current_url, session=session) if link.startswith(WHATSAPP_DOMAIN))
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag.get('href')
                        if href:
                            abs_url = urljoin(current_url, href)
                            parsed_abs_url = urlparse(abs_url)
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain and \
                               not parsed_abs_url.fragment:
                                normalized_abs_url = urljoin(abs_url, parsed_abs_url.path or '/')
                                if normalized_abs_url not in visited_urls and (abs_url, depth + 1) not in urls_in_queue_tuples:
                                     queue_list.append((abs_url, depth + 1)); urls_in_queue_tuples.add((abs_url, depth + 1))
            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl Parse Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
    st.sidebar.success(f"Crawl done. Scraped {page_count} pages, found {len(scraped_whatsapp_links)} links.")
    if page_count >= max_pages: st.sidebar.warning(f"Stopped at {max_pages} pages.", icon="❗️")
    if len(queue_list) > max_q_size: st.sidebar.warning(f"Queue capped at {max_q_size}.", icon="❗️")
    return scraped_whatsapp_links

def generate_styled_html_table(active_results_df):
    # Filter out "Unnamed Group" before generating the table
    df_to_display = active_results_df[active_results_df['Group Name'] != UNNAMED_GROUP_PLACEHOLDER].copy()

    if df_to_display.empty: # Check if DataFrame is empty AFTER filtering
        return "<p>No named active groups found for styled table.</p>"

    html_string = '<table class="whatsapp-groups-table"><tbody>'
    for _, row in df_to_display.iterrows(): # Iterate over the filtered DataFrame
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", UNNAMED_GROUP_PLACEHOLDER) # Default just in case
        group_link = row.get("Group Link", "")
        
        html_string += '<tr>'
        html_string += '<td class="group-logo-cell">'
        alt_text = f"{html.escape(group_name)} Logo" if group_name != UNNAMED_GROUP_PLACEHOLDER else "Group Logo"
        if logo_url:
            display_logo_url = append_query_param(logo_url, 'w', '96') if logo_url.startswith('https://pps.whatsapp.net/') else logo_url
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img">'
        else:
             html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        html_string += '</td>'
        
        safe_group_name = html.escape(group_name)
        html_string += f'<td class="group-name-cell">{safe_group_name}</td>'
        
        html_string += '<td class="join-button-cell">'
        if group_link and group_link.startswith(WHATSAPP_DOMAIN):
             html_string += f'<a href="{html.escape(group_link)}" class="join-button" target="_blank" rel="noopener noreferrer">Join Group</a>' # Added rel for security
        else:
             html_string += '<span style="color:#888; font-size:0.9em;">N/A</span>'
        html_string += '</td></tr>'
    html_string += '</tbody></table>'
    return html_string

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True) # This is the main H1 for the page
    st.markdown('<p class="subtitle">Find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()
    
    if not isinstance(st.session_state.processed_links_in_session, set):
        st.session_state.processed_links_in_session = set()
    if isinstance(st.session_state.results, list):
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and res_item['Group Link']:
                try:
                    parsed_link = urlparse(res_item['Group Link'])
                    normalized_link = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
                    st.session_state.processed_links_in_session.add(normalized_link)
                except Exception:
                    st.session_state.processed_links_in_session.add(res_item['Group Link'])

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV/Excel)"
        ], key="input_method_main_select")

        gs_top_n, gs_pause = 5, 2.0
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            gs_top_n = st.slider("Google Results to Scrape (per keyword)", 1, 20, 5, key="gs_top_n_slider")
            gs_pause = st.slider("Google Search Pause (s)", 0.5, 10.0, 2.0, 0.5, key="gs_pause_slider")
        
        crawl_depth, crawl_pages = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be slow. Use with caution.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 300, 50, key="crawl_pages_slider")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.cache_data.clear(); st.success("Results & cache cleared!"); st.rerun()

    current_action_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input")
            if st.button("Search & Validate", use_container_width=True, key="gs_button"):
                if query: current_action_scraped_links.update(google_search_and_scrape(query, gs_top_n, gs_pause))
                else: st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            file = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if file and st.button("Process Excel & Scrape", use_container_width=True, key="gs_bulk_button"):
                keywords = load_keywords_from_excel(file)
                if keywords:
                    st.info(f"Processing {len(keywords)} keywords...")
                    prog, stat_txt = st.progress(0), st.empty()
                    for i, kw in enumerate(keywords):
                        stat_txt.text(f"Keyword: {kw} ({i+1}/{len(keywords)}). Links found: {len(current_action_scraped_links)}")
                        current_action_scraped_links.update(google_search_and_scrape(kw, gs_top_n, gs_pause))
                        prog.progress((i+1)/len(keywords))
                    stat_txt.success(f"Bulk processing done. Found {len(current_action_scraped_links)} links.")
                else: st.warning("No keywords in Excel.")

        elif input_method == "Scrape from Specific Webpage URL":
            url = st.text_input("Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if url and (url.startswith("http://") or url.startswith("https://")):
                    with st.spinner(f"Scraping {url}..."):
                        current_action_scraped_links.update(scrape_whatsapp_links_from_page(url))
                    st.success(f"Scraping done. Found {len(current_action_scraped_links)} links.")
                else: st.warning("Please enter a valid URL.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain = st.text_input("Base Domain URL:", placeholder="example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape", use_container_width=True, key="crawl_button"):
                if domain:
                    st.info("Starting crawl. Progress in sidebar.")
                    current_action_scraped_links.update(crawl_website(domain, crawl_depth, crawl_pages))
                    st.success(f"Crawl done. Found {len(current_action_scraped_links)} links.")
                else: st.warning("Please enter a domain.")

        elif input_method == "Enter Links Manually (for Validation)":
            text = st.text_area("WhatsApp Links (one per line):", height=200, key="manual_links_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                links = [line.strip() for line in text.split('\n') if line.strip()]
                if links:
                    valid_links = {l for l in links if l.startswith(WHATSAPP_DOMAIN)}
                    if len(valid_links) < len(links): st.warning(f"Skipped {len(links)-len(valid_links)} non-WhatsApp links.")
                    current_action_scraped_links.update(valid_links)
                else: st.warning("Please enter links.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file = st.file_uploader("Upload TXT, CSV, or Excel (.xlsx for keywords)", type=["txt", "csv", "xlsx"], key="upload_file_input")
            if file and st.button("Process File", use_container_width=True, key="upload_process_button"):
                if file.name.endswith('.xlsx'):
                    st.info("Loading keywords from Excel for Google search...")
                    keywords = load_keywords_from_excel(file)
                    if keywords:
                        prog, stat_txt = st.progress(0), st.empty()
                        for i, kw in enumerate(keywords):
                            stat_txt.text(f"Keyword: {kw} ({i+1}/{len(keywords)}). Links found: {len(current_action_scraped_links)}")
                            current_action_scraped_links.update(google_search_and_scrape(kw, gs_top_n, gs_pause))
                            prog.progress((i+1)/len(keywords))
                        stat_txt.success(f"Excel processing done. Found {len(current_action_scraped_links)} links.")
                    else: st.warning("No keywords in Excel.")
                elif file.name.endswith(('.txt', '.csv')):
                    st.info("Loading links from TXT/CSV for validation...")
                    links = load_links_from_file(file)
                    if links:
                        valid_links = {l for l in links if l.startswith(WHATSAPP_DOMAIN)}
                        if len(valid_links) < len(links): st.warning(f"Skipped {len(links)-len(valid_links)} non-WhatsApp links.")
                        current_action_scraped_links.update(valid_links)
                    else: st.warning("No links in file.")
                else: st.warning("Unsupported file. Use .txt, .csv, or .xlsx.")
    except Exception as e: st.error(f"Input/Scraping Error: {e}", icon="💥")

    links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)
    if links_to_validate_now:
        st.success(f"Found {len(current_action_scraped_links)} links. Validating {len(links_to_validate_now)} new links...")
        prog_val, stat_val = st.progress(0), st.empty()
        new_results = []
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
            for i, future in enumerate(as_completed(future_to_link)):
                link_validated = future_to_link[future]
                result_validated = future.result()
                new_results.append(result_validated)
                try:
                    parsed_url = urlparse(link_validated)
                    normalized_link = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                    st.session_state.processed_links_in_session.add(normalized_link)
                except Exception:
                    st.session_state.processed_links_in_session.add(link_validated)
                prog_val.progress((i+1)/len(links_to_validate_now))
                stat_val.text(f"Validated {i+1}/{len(links_to_validate_now)} links")
        st.session_state.results.extend(new_results)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
    elif current_action_scraped_links and not links_to_validate_now:
         st.info("No *new* WhatsApp links found. All were previously processed.")

    if 'results' in st.session_state and st.session_state.results:
        df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first').reset_index(drop=True)
        st.session_state.results = df.to_dict('records')

        active_df = df[df['Status'].str.contains('Active', na=False)].copy()
        expired_df = df[df['Status'] == 'Expired'].copy()
        error_df = df[~df['Status'].str.contains('Active', na=False) & (df['Status'] != 'Expired')].copy()


        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        # Using div with class "metric-value" instead of h1 for metric numbers
        col1.markdown(f'<div class="metric-card">Total Links<br><div class="metric-value">{len(df)}</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df)}</div></div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="metric-card">Expired Links<br><div class="metric-value">{len(expired_df)}</div></div>', unsafe_allow_html=True)
        col4.markdown(f'<div class="metric-card">Error Links<br><div class="metric-value">{len(error_df)}</div></div>', unsafe_allow_html=True)


        with st.expander("🔎 View and Filter Results Table", expanded=True):
            all_statuses = sorted(list(df['Status'].unique()))
            default_statuses = [s for s in all_statuses if 'Active' in s]
            if not default_statuses and 'Expired' in all_statuses : default_statuses.append('Expired')
            if not default_statuses and all_statuses : default_statuses = [all_statuses[0]]

            status_filter = st.multiselect("Filter by Status", all_statuses, default=default_statuses, key="status_filter_multiselect")
            filtered_df = df[df['Status'].isin(status_filter)] if status_filter else df.copy()
            st.dataframe(filtered_df, column_config={
                "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join", width="medium"),
                "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View Logo", width="small", help="Direct URL of logo."),
                "Status": st.column_config.TextColumn("Status", width="small")
            }, hide_index=True, height=400, use_container_width=True)

        st.subheader("✨ Styled Output (Active Groups)")
        # Pass active_df which already contains only active groups
        # The generate_styled_html_table function will further filter out "Unnamed Group"
        if not active_df.empty:
            html_out = generate_styled_html_table(active_df)
            if "<td" in html_out: # Check if the table actually has content after filtering Unnamed
                with st.expander("View and Copy Styled HTML / Download", expanded=True):
                    st.markdown(html_out, unsafe_allow_html=True)
                    st.text_area("Raw HTML Code:", value=html_out, height=300, key="styled_html_export_area", help="Ctrl+A, Ctrl+C")
                    st.download_button("📥 Download HTML", html_out.encode('utf-8'), "styled_groups.html", "text/html", use_container_width=True, key="styled_html_export_download")
            else:
                st.info("No named active groups found to display in the styled table.") # Message if all active groups were "Unnamed"
        else: st.info("No active groups for styled output.")

        st.subheader("Downloads")
        dl_col1, dl_col2 = st.columns(2)
        dl_col1.download_button("📥 Active Groups (CSV)", active_df.to_csv(index=False).encode('utf-8'), "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv")
        dl_col2.download_button("📥 All Results (CSV)", df.to_csv(index=False).encode('utf-8'), "all_results.csv", "text/csv", use_container_width=True, key="dl_all_csv")
    else:
        st.info("Start by searching, entering, or uploading links!", icon="ℹ️")

if __name__ == "__main__":
    required_libraries_map = {
        "requests": "requests",
        "bs4": "BeautifulSoup (from bs4)",
        "pandas": "pandas",
        "openpyxl": "openpyxl (for Excel)",
        "fake_useragent": "fake-useragent",
        "googlesearch": "googlesearch-python"
    }
    missing_libs_display = []
    install_command_keys = [] # Store keys for pip install command
    for import_name, display_name in required_libraries_map.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_libs_display.append(display_name)
            install_command_keys.append(import_name)


    if missing_libs_display:
        st.error(f"The following required libraries are not installed: {', '.join(missing_libs_display)}. Please install them using pip (e.g., `pip install {' '.join(install_command_keys)}`).", icon="🚨")
        st.stop()
    main()
