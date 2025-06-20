import streamlit as st
import pandas as pd
import requests
import html
from bs4 import BeautifulSoup
import re
import time
import io
import traceback
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

# --- Import Google Search Library ---
try:
    from googlesearch import search as google_search_function_actual
except ImportError:
    st.error("The `googlesearch-python` library is not installed. Please install it: `pip install googlesearch-python`")
    def google_search_function_actual(query, num_results, lang, **kwargs):
        st.error("`googlesearch-python` library not found. Cannot perform Google searches.")
        return []

# --- Import Fake User Agent Library ---
try:
    from fake_useragent import UserAgent
    from fake_useragent.errors import FakeUserAgentError
    ua_general = UserAgent()
    def get_random_headers_general():
        try:
            return {
                "User-Agent": ua_general.random,
                "Accept-Language": "en-US,en;q=0.9"
            }
        except FakeUserAgentError:
            st.warning("fake-useragent failed to get a User-Agent. Using fallback.", icon="⚠️")
            return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception as e_random:
            st.warning(f"Error getting random User-Agent: {e_random}. Using fallback.", icon="⚠️")
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
except FakeUserAgentError as e_init:
    st.warning(f"Error initializing fake-useragent: {e_init}. Using default User-Agent.", icon="⚠️")
    def get_random_headers_general():
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except Exception as e_general_init:
    st.warning(f"Error initializing fake-useragent: {e_general_init}. Using default User-Agent.", icon="⚠️")
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
UNNAMED_GROUP_PLACEHOLDER = ""
IMAGE_PATTERN_PPS = re.compile(r'https:\/\/pps\.whatsapp\.net\/v\/t\d+\/[-\w]+\/\d+\.jpg\?')
OG_IMAGE_PATTERN = re.compile(r'https?:\/\/[^\/\s]+\/[^\/\s]+\.(jpg|jpeg|png)(\?[^\s]*)?')
MAX_VALIDATION_WORKERS = 8

# --- Custom CSS ---
st.markdown("""
<style>
/* ... (existing CSS remains the same) ... */
.expander-collapsed .streamlit-expanderHeader:after {
    content: "+";
    float: right;
    font-weight: bold;
}
.expander-expanded .streamlit-expanderHeader:after {
    content: "-";
    float: right;
    font-weight: bold;
}
.progress-container {
    position: relative;
    margin-bottom: 15px;
}
.progress-text {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    font-weight: bold;
    color: #333;
}
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
    result = {"Group Name": UNNAMED_GROUP_PLACEHOLDER, "Group Link": link, "Logo URL": "", "Status": "Error"}
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
        elif result["Status"] == "Expired" and (group_name_found or logo_found):
            if soup.find('a', attrs={'id': 'action-button', 'href': link}):
                result["Status"] = "Active"
        
        # New validation rules
        if result["Status"] == "Active" and not result["Group Name"].strip():
            result["Status"] = "Expire"
        elif not result["Group Name"].strip():
            result["Status"] = "Expire"

    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError: result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: 
        result["Status"] = f"Parsing Error ({type(e).__name__})"
        if not result["Group Name"].strip():
            result["Status"] = "Expire"
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
                clean_link = re.sub(r'(\.[a-zA-Z]{2,4})$', '', clean_link) if not clean_link.endswith(('.html', '.htm', '.php')) else clean_link
                clean_link = clean_link.split('&')[0] 
                parsed_url = urlparse(clean_link)
                if len(parsed_url.path.replace('/', '')) > 15:
                    links.add(f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}")
    except requests.exceptions.Timeout: st.sidebar.warning(f"Scrape Timeout: {url[:50]}...", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Scrape HTTP Err {e.response.status_code}: {url[:50]}...", icon="⚠️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape Net Err ({type(e).__name__}): {url[:50]}...", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Scrape Parse Err ({type(e).__name__}): {url[:50]}...", icon="💣")
    return list(links)

def google_search_and_scrape(query, top_n=5):
    st.info(f"Googling '{query}' (top {top_n} results)...")
    all_scraped_wa_links = set()
    try:
        search_page_urls = list(google_search_function_actual(query, num_results=top_n, lang="en"))
        if not search_page_urls:
            st.warning(f"No Google results for '{query}'. Possible reasons: "
                       f"1. Query yields no results. "
                       f"2. Google blocking (try VPN/wait). "
                       f"3. `googlesearch-python` library issue.", icon="🤔")
            return []

        st.success(f"Found {len(search_page_urls)} pages from Google. Scraping them for WhatsApp links...")
        prog_bar, stat_txt = st.progress(0), st.empty()
        with requests.Session() as scrape_session:
            for i, url_from_google in enumerate(search_page_urls):
                stat_txt.text(f"Scraping page {i+1}/{len(search_page_urls)}: {url_from_google[:60]}...")
                wa_links_from_page = scrape_whatsapp_links_from_page(url_from_google, session=scrape_session)
                newly_found_count = 0
                for link in wa_links_from_page:
                    if link.startswith(WHATSAPP_DOMAIN) and link not in all_scraped_wa_links:
                        all_scraped_wa_links.add(link)
                        newly_found_count += 1
                if newly_found_count > 0:
                    st.sidebar.info(f"Found {newly_found_count} new WA links on {url_from_google[:30]}...")
                prog_bar.progress((i+1)/len(search_page_urls))
        stat_txt.success(f"Scraping of Google results complete. Found {len(all_scraped_wa_links)} unique WhatsApp links from '{query}'.")
        return list(all_scraped_wa_links)
    except TypeError as e:
        st.error(f"Google search TypeError: {e}. Check `googlesearch-python` version/parameters.", icon="❌")
        return []
    except Exception as e:
        st.error(f"Unexpected Google search/scrape error for '{query}': {e}. Check connection/library.", icon="❌")
        return []

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
    
    # Create progress bar for crawling
    crawl_progress = st.progress(0)
    status_text = st.empty()
    
    with requests.Session() as session, st.spinner(f"Crawling {base_domain}..."):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size:
                st.sidebar.warning(f"Queue > {max_q_size}. Stopping.", icon="❗️"); queue_list = queue_list[:max_q_size]
            current_url, depth = queue_list.pop(0)
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')
            if normalized_current_url in visited_urls or depth > max_depth: continue
            visited_urls.add(normalized_current_url)
            if page_count >= max_pages: break
            
            # Update progress
            progress = min(page_count / max_pages, 1.0)
            crawl_progress.progress(progress)
            status_text.text(f"Crawled: {page_count}/{max_pages} pages | Queue: {len(queue_list)} | Depth: {depth}")
            
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10)
                response.raise_for_status()
                if 'text/html' not in response.headers.get('Content-Type', '').lower(): continue
                page_count += 1
                wa_links_from_page = scrape_whatsapp_links_from_page(current_url, session=session)
                newly_found_count = 0
                for link in wa_links_from_page:
                    if link.startswith(WHATSAPP_DOMAIN) and link not in scraped_whatsapp_links:
                        scraped_whatsapp_links.add(link)
                        newly_found_count += 1
                if newly_found_count > 0:
                    st.sidebar.info(f"Crawl: Found {newly_found_count} new WA links on {current_url[:30]}...")

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
            except requests.exceptions.RequestException as e: 
                st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: 
                st.sidebar.error(f"Crawl Parse Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
    
    crawl_progress.progress(1.0)
    status_text.text(f"Crawl complete: Scraped {page_count} pages, found {len(scraped_whatsapp_links)} links.")
    st.sidebar.success(f"Crawl done. Scraped {page_count} pages, found {len(scraped_whatsapp_links)} links.")
    if page_count >= max_pages: st.sidebar.warning(f"Stopped at {max_pages} pages.", icon="❗️")
    if len(queue_list) > max_q_size: st.sidebar.warning(f"Queue capped at {max_q_size}.", icon="❗️")
    return scraped_whatsapp_links

def generate_styled_html_table(data_df_for_table):
    df_to_display = data_df_for_table[data_df_for_table['Group Name'] != UNNAMED_GROUP_PLACEHOLDER].copy()
    
    if df_to_display.empty:
        return "<p style='text-align:center; color:#777; margin-top:20px;'><i>No groups match the current display filters. Try adjusting them.</i></p>"

    html_string = '<table class="whatsapp-groups-table" aria-label="List of Active WhatsApp Groups">'
    html_string += '<caption>Filtered Active WhatsApp Groups</caption>'
    html_string += '<thead><tr>'
    html_string += '<th scope="col">Logo</th>'
    html_string += '<th scope="col">Group Name</th>'
    html_string += '<th scope="col">Group Link</th>'
    html_string += '</tr></thead>'
    html_string += '<tbody>'
    for _, row in df_to_display.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", UNNAMED_GROUP_PLACEHOLDER)
        group_link = row.get("Group Link", "")
        html_string += '<tr>'
        html_string += '<td class="group-logo-cell">'
        alt_text = f"{html.escape(group_name)} Group Logo"
        if logo_url:
            display_logo_url = append_query_param(logo_url, 'w', '96') if logo_url.startswith('https://pps.whatsapp.net/') else logo_url
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img" loading="lazy">'
        else:
            html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        html_string += '</td>'
        safe_group_name = html.escape(group_name)
        html_string += f'<td class="group-name-cell">{safe_group_name}</td>'
        html_string += '<td class="join-button-cell">'
        if group_link and group_link.startswith(WHATSAPP_DOMAIN):
            html_string += f'<a href="{html.escape(group_link)}" class="join-button" target="_blank" rel="noopener noreferrer">Join Group</a>'
        else:
            html_string += '<span style="color:#888; font-size:0.9em;">N/A</span>'
        html_string += '</td></tr>'
    html_string += '</tbody></table>'
    return html_string

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, Scrape, Validate, and Manage WhatsApp Group Links with Enhanced Filtering.</p>', unsafe_allow_html=True)

    # Initialize session state with more robust structure
    if 'results' not in st.session_state: 
        st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: 
        st.session_state.processed_links_in_session = set()
    if 'styled_table_name_keywords' not in st.session_state: 
        st.session_state.styled_table_name_keywords = ""
    if 'styled_table_current_limit_value' not in st.session_state: 
        st.session_state.styled_table_current_limit_value = 50
    if 'adv_filter_status' not in st.session_state: 
        st.session_state.adv_filter_status = ["Active"]  # Active as default filter
    if 'adv_filter_name_keywords' not in st.session_state: 
        st.session_state.adv_filter_name_keywords = ""
    if 'current_action_scraped_links' not in st.session_state: 
        st.session_state.current_action_scraped_links = set()
    if 'validation_in_progress' not in st.session_state: 
        st.session_state.validation_in_progress = False
    if 'validation_progress' not in st.session_state: 
        st.session_state.validation_progress = 0
    if 'validation_total' not in st.session_state: 
        st.session_state.validation_total = 0
    if 'validation_completed' not in st.session_state: 
        st.session_state.validation_completed = 0
    if 'crawl_in_progress' not in st.session_state: 
        st.session_state.crawl_in_progress = False

    # Ensure processed_links_in_session is a set and populate it
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

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV/Excel)"
        ], key="input_method_main_select")

        # Increased Google search limit to 50
        gs_top_n = 5
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            gs_top_n = st.slider("Google Results to Scrape (per keyword)", 1, 50, 5, key="gs_top_n_slider", 
                                 help="Number of Google search result pages to analyze per keyword (max 50).")
        
        # Increased scraping limits
        crawl_depth, crawl_pages = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be slow. Use with caution.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 10, 2, key="crawl_depth_slider",
                                   help="Maximum depth to crawl (0 = only start page, 10 = very deep)")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 1000, 50, key="crawl_pages_slider",
                                   help="Maximum number of pages to crawl (1000 max)")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.session_state.styled_table_name_keywords = ""
            st.session_state.styled_table_current_limit_value = 50
            st.session_state.adv_filter_status = ["Active"]  # Reset to Active filter
            st.session_state.adv_filter_name_keywords = ""
            st.session_state.current_action_scraped_links = set()
            st.session_state.validation_in_progress = False
            st.session_state.validation_progress = 0
            st.session_state.validation_total = 0
            st.session_state.validation_completed = 0
            st.session_state.crawl_in_progress = False
            st.success("Results & filters cleared!")
            st.rerun()

    # Action Zone
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input")
            if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button"):
                if query: 
                    st.session_state.current_action_scraped_links = set(google_search_and_scrape(query, gs_top_n))
                else: 
                    st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            file = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if file and st.button("Process Excel, Scrape & Validate", use_container_width=True, key="gs_bulk_button"):
                keywords = load_keywords_from_excel(file)
                if keywords:
                    st.info(f"Processing {len(keywords)} keywords...")
                    prog_b, stat_b = st.progress(0), st.empty()
                    total_l = 0
                    for i, kw in enumerate(keywords):
                        stat_b.text(f"Keyword: '{kw}' ({i+1}/{len(keywords)}). Total links: {total_l}")
                        links_from_kw = google_search_and_scrape(kw, gs_top_n)
                        st.session_state.current_action_scraped_links.update(links_from_kw)
                        total_l = len(st.session_state.current_action_scraped_links)
                        prog_b.progress((i+1)/len(keywords))
                    stat_b.success(f"Bulk done. Found {total_l} links.")
                else: 
                    st.warning("No valid keywords in Excel.")

        elif input_method == "Scrape from Specific Webpage URL":
            url = st.text_input("Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if url and (url.startswith("http://") or url.startswith("https://")):
                    with st.spinner(f"Scraping {url}..."):
                        st.session_state.current_action_scraped_links = set(scrape_whatsapp_links_from_page(url))
                    st.success(f"Scraping done. Found {len(st.session_state.current_action_scraped_links)} links.")
                else: 
                    st.warning("Please enter a valid URL.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain = st.text_input("Base Domain URL:", placeholder="example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape", use_container_width=True, key="crawl_button"):
                if domain:
                    st.info("Starting crawl. Progress in sidebar.")
                    st.session_state.crawl_in_progress = True
                    try:
                        st.session_state.current_action_scraped_links = set(crawl_website(domain, crawl_depth, crawl_pages))
                        st.session_state.crawl_in_progress = False
                    except Exception as e:
                        st.error(f"Crawl failed: {str(e)}")
                        st.session_state.crawl_in_progress = False
                        traceback.print_exc()
                else: 
                    st.warning("Please enter a domain.")

        elif input_method == "Enter Links Manually (for Validation)":
            text = st.text_area("WhatsApp Links (one per line):", height=200, key="manual_links_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                links = [line.strip() for line in text.split('\n') if line.strip()]
                if links:
                    valid_links = {l for l in links if l.startswith(WHATSAPP_DOMAIN)}
                    if len(valid_links) < len(links): 
                        st.warning(f"Skipped {len(links)-len(valid_links)} non-WhatsApp links.")
                    st.session_state.current_action_scraped_links = valid_links
                else: 
                    st.warning("Please enter links.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file = st.file_uploader("Upload TXT, CSV (links) or Excel (keywords)", type=["txt", "csv", "xlsx"], key="upload_file_input")
            if file and st.button("Process File", use_container_width=True, key="upload_process_button"):
                if file.name.endswith('.xlsx'):
                    st.info("Loading keywords from Excel for Google search...")
                    keywords = load_keywords_from_excel(file)
                    if keywords:
                        prog_e, stat_e = st.progress(0), st.empty()
                        total_le = 0
                        for i, kw in enumerate(keywords):
                            stat_e.text(f"Keyword: {kw} ({i+1}/{len(keywords)}). Links: {total_le}")
                            links_from_kw = google_search_and_scrape(kw, gs_top_n)
                            st.session_state.current_action_scraped_links.update(links_from_kw)
                            total_le = len(st.session_state.current_action_scraped_links)
                            prog_e.progress((i+1)/len(keywords))
                        stat_e.success(f"Excel processing done. Found {total_le} links.")
                    else: 
                        st.warning("No keywords in Excel.")
                elif file.name.endswith(('.txt', '.csv')):
                    st.info("Loading links from TXT/CSV for validation...")
                    links = load_links_from_file(file)
                    if links:
                        valid_links = {l for l in links if l.startswith(WHATSAPP_DOMAIN)}
                        if len(valid_links) < len(links): 
                            st.warning(f"Skipped {len(links)-len(valid_links)} non-WhatsApp links.")
                        st.session_state.current_action_scraped_links = valid_links
                    else: 
                        st.warning("No links in file.")
                else: 
                    st.warning("Unsupported file. Use .txt, .csv, or .xlsx.")
    except Exception as e: 
        st.error(f"Input/Scraping Error: {e}", icon="💥")
        traceback.print_exc()

    # Validation
    if st.session_state.current_action_scraped_links:
        links_to_validate_now = list(st.session_state.current_action_scraped_links - st.session_state.processed_links_in_session)
        
        if links_to_validate_now:
            st.session_state.validation_in_progress = True
            st.session_state.validation_total = len(links_to_validate_now)
            st.session_state.validation_completed = 0
            
            st.success(f"Found {len(st.session_state.current_action_scraped_links)} links. Validating {st.session_state.validation_total} new links...")
            
            # Create progress containers
            progress_container = st.container()
            progress_bar = progress_container.progress(0)
            status_text = st.empty()
            
            # Run validation in a way that preserves state
            try:
                with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
                    
                    for i, future in enumerate(as_completed(future_to_link)):
                        link_validated = future_to_link[future]
                        try:
                            result_validated = future.result()
                            st.session_state.results.append(result_validated)
                            
                            try:
                                parsed_url_val = urlparse(link_validated)
                                normalized_link_val = f"{parsed_url_val.scheme}://{parsed_url_val.netloc}{parsed_url_val.path}"
                            except:
                                normalized_link_val = link_validated
                                
                            st.session_state.processed_links_in_session.add(normalized_link_val)
                            
                        except Exception as val_exc:
                            st.warning(f"Error validating {link_validated[:40]}...: {val_exc}", icon="⚠️")
                            try:
                                parsed_url_val_err = urlparse(link_validated)
                                normalized_link_val_err = f"{parsed_url_val_err.scheme}://{parsed_url_val_err.netloc}{parsed_url_val_err.path}"
                            except:
                                normalized_link_val_err = link_validated
                            st.session_state.processed_links_in_session.add(normalized_link_val_err)
                            st.session_state.results.append({
                                "Group Name": "Validation Error", 
                                "Group Link": link_validated, 
                                "Logo URL": "", 
                                "Status": f"Validation Failed: {type(val_exc).__name__}"
                            })
                        
                        # Update progress
                        st.session_state.validation_completed = i + 1
                        progress_percent = st.session_state.validation_completed / st.session_state.validation_total
                        progress_bar.progress(progress_percent)
                        status_text.text(f"Validated {st.session_state.validation_completed}/{st.session_state.validation_total} links")
                
                status_text.success(f"Validation complete for {st.session_state.validation_total} new links!")
                st.session_state.validation_in_progress = False
                st.session_state.current_action_scraped_links = set()
                
            except Exception as e:
                st.error(f"Validation process failed: {str(e)}", icon="❌")
                st.session_state.validation_in_progress = False
                traceback.print_exc()
                
        elif st.session_state.current_action_scraped_links and not links_to_validate_now:
            st.info("No *new* WhatsApp links found from this action. All were previously processed.")
            st.session_state.current_action_scraped_links = set()

    # Show progress if validation was interrupted
    if st.session_state.validation_in_progress and st.session_state.validation_total > 0:
        st.warning("Validation was interrupted. Resuming progress...")
        progress_percent = st.session_state.validation_completed / st.session_state.validation_total
        progress_bar = st.progress(progress_percent)
        st.text(f"Validated {st.session_state.validation_completed}/{st.session_state.validation_total} links")
        
        if st.button("Continue Validation", key="continue_validation_button"):
            # This will trigger the validation to resume on next run
            st.rerun()
        if st.button("Cancel Validation", key="cancel_validation_button"):
            st.session_state.validation_in_progress = False
            st.session_state.current_action_scraped_links = set()
            st.info("Validation canceled.")
            st.rerun()

    # Results Display
    if 'results' in st.session_state and st.session_state.results:
        try:
            # Create DataFrame from results
            unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
            st.session_state.results = unique_results_df.to_dict('records')
            df_display_master = unique_results_df.reset_index(drop=True)

            condition_expired = (df_display_master['Status'].str.startswith('Expired')) | (df_display_master['Status'] == 'Expire')
            active_df_all_master = df_display_master[df_display_master['Status'] == 'Active'].copy()
            expired_df_master = df_display_master[condition_expired].copy()
            error_df_master = df_display_master[~condition_expired & (df_display_master['Status'] != 'Active')].copy()

            st.subheader("📊 Results Summary")
            col1, col2, col3, col4 = st.columns(4)
            col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master)}</div></div>', unsafe_allow_html=True)
            col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df_all_master)}</div></div>', unsafe_allow_html=True)
            col3.markdown(f'<div class="metric-card">Expired/Expire Links<br><div class="metric-value">{len(expired_df_master)}</div></div>', unsafe_allow_html=True)
            col4.markdown(f'<div class="metric-card">Other Status<br><div class="metric-value">{len(error_df_master)}</div></div>', unsafe_allow_html=True)

            # Styled Table with Filters (collapsed by default)
            st.subheader("✨ Active Groups Display (Styled Table)")
            with st.expander("View and Filter Active Groups", expanded=False):  # Collapsed by default
                if not active_df_all_master.empty:
                    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                    st.markdown("#### Filter Displayed Active Groups:")

                    with st.form("styled_table_filters_form"):
                        name_keywords_input = st.text_input(
                            "Filter by Group Name Keywords (comma-separated):",
                            value=st.session_state.styled_table_name_keywords,
                            placeholder="e.g., study, fun, tech",
                            help="Enter keywords (comma-separated). Shows groups matching ANY keyword."
                        ).strip()
                        limit_input = st.number_input(
                            "Max Groups to Display in Table:",
                            min_value=1,
                            max_value=1000,
                            value=st.session_state.styled_table_current_limit_value,
                            step=10,
                            help="Set the maximum number of groups to display in the table."
                        )
                        apply_filters = st.form_submit_button("Apply Filters")

                    if apply_filters:
                        st.session_state.styled_table_name_keywords = name_keywords_input
                        st.session_state.styled_table_current_limit_value = limit_input

                    if st.button("Reset Filters", key="reset_styled_table_filters_button"):
                        st.session_state.styled_table_name_keywords = ""
                        st.session_state.styled_table_current_limit_value = 50
                        st.rerun()

                    # Filter the dataframe
                    active_df_for_styled_table = active_df_all_master.copy()
                    if st.session_state.styled_table_name_keywords:
                        keywords_list = [kw.strip().lower() for kw in st.session_state.styled_table_name_keywords.split(',') if kw.strip()]
                        if keywords_list:
                            regex_pattern = '|'.join(map(re.escape, keywords_list))
                            active_df_for_styled_table = active_df_for_styled_table[
                                active_df_for_styled_table['Group Name'].str.lower().str.contains(regex_pattern, na=False, regex=True)
                            ]

                    num_matching = len(active_df_for_styled_table)
                    num_displayed = min(num_matching, st.session_state.styled_table_current_limit_value)
                    active_df_for_styled_table_final = active_df_for_styled_table.head(num_displayed)

                    if num_matching > 0:
                        st.write(f"Showing {num_displayed} out of {num_matching} matching active groups.")
                    else:
                        st.write("No groups match the current filters.")

                    html_out = generate_styled_html_table(active_df_for_styled_table_final)
                    st.markdown(html_out, unsafe_allow_html=True)
                    st.markdown("---")
                    st.text_area("Copy Raw HTML Code (above table):", value=html_out, height=150, key="styled_html_export_area_key", help="Ctrl+A, Ctrl+C")
                    st.markdown('</div>', unsafe_allow_html=True) # Close filter-container
                else:
                    st.info("No active groups found yet to display here.")

            # Advanced Filtering for Downloads (collapsed by default)
            with st.expander("🔬 Advanced Filtering for Downloads & Analysis (Optional)", expanded=False):
                st.markdown('<div class="filter-container" style="border-style:solid;">', unsafe_allow_html=True)
                st.markdown("#### Filter Full Dataset (for Download/Analysis):")
                
                all_statuses_master = sorted(list(df_display_master['Status'].unique()))
                st.session_state.adv_filter_status = st.multiselect(
                    "Filter by Status:", options=all_statuses_master,
                    default=st.session_state.adv_filter_status, key="adv_status_filter_multiselect_key"
                )

                st.session_state.adv_filter_name_keywords = st.text_input(
                    "Filter by Group Name Keywords (comma-separated):", value=st.session_state.adv_filter_name_keywords,
                    key="adv_name_keyword_filter_input_key", placeholder="e.g., news, jobs, global",
                    help="Applies to the entire dataset for download/analysis. Comma-separated."
                ).strip()
                st.markdown('</div>', unsafe_allow_html=True)

                df_for_adv_download_or_view = df_display_master.copy()
                adv_filters_applied = False
                if st.session_state.adv_filter_status:
                    df_for_adv_download_or_view = df_for_adv_download_or_view[df_for_adv_download_or_view['Status'].isin(st.session_state.adv_filter_status)]
                    adv_filters_applied = True
                if st.session_state.adv_filter_name_keywords:
                    adv_keywords_list = [kw.strip().lower() for kw in st.session_state.adv_filter_name_keywords.split(',') if kw.strip()]
                    if adv_keywords_list:
                        adv_regex_pattern = '|'.join(map(re.escape, adv_keywords_list))
                        df_for_adv_download_or_view = df_for_adv_download_or_view[
                            df_for_adv_download_or_view['Group Name'].str.lower().str.contains(adv_regex_pattern, na=False, regex=True)
                        ]
                        adv_filters_applied = True
                
                st.markdown(f"**Preview of Data for Download/Analysis ({'Filtered' if adv_filters_applied else 'All'} - {len(df_for_adv_download_or_view)} rows):**")
                st.dataframe(df_for_adv_download_or_view, column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join", width="medium"),
                    "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                    "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View", width="small"),
                    "Status": st.column_config.TextColumn("Status", width="small")
                }, hide_index=True, height=300, use_container_width=True)

            # Downloads
            st.subheader("📥 Download Results (CSV)")
            dl_col1, dl_col2 = st.columns(2)
            if not active_df_all_master.empty:
                dl_col1.download_button("Active Groups (CSV)", active_df_all_master.to_csv(index=False).encode('utf-8'), "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv_main_key")
            else:
                dl_col1.button("Active Groups (CSV)", disabled=True, use_container_width=True, help="No active groups to download.")

            if not df_for_adv_download_or_view.empty:
                download_label = "All Processed Results (CSV)"
                if adv_filters_applied: download_label = f"Filtered Processed Results (CSV - {len(df_for_adv_download_or_view)} rows)"
                dl_col2.download_button(download_label, df_for_adv_download_or_view.to_csv(index=False).encode('utf-8'), "processed_results.csv", "text/csv", use_container_width=True, key="dl_all_or_filtered_csv_key")
            elif not df_display_master.empty and df_for_adv_download_or_view.empty and adv_filters_applied:
                dl_col2.button("No Results Match Advanced Filters", disabled=True, use_container_width=True)
            else:
                dl_col2.button("All Processed Results (CSV)", disabled=True, use_container_width=True, help="No results to download.")
                
        except Exception as e:
            st.error(f"Error displaying results: {str(e)}", icon="❌")
            traceback.print_exc()
    else:
        st.info("Start by searching, entering, or uploading links to see results!", icon="ℹ️")

if __name__ == "__main__":
    main()
