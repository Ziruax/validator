import streamlit as st
import pandas as pd
import requests
import html
from bs4 import BeautifulSoup
import re
import time
import io
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import Google Search Library ---
try:
    from googlesearch import search as google_search_function_actual
except ImportError:
    st.error("The googlesearch-python library is not installed. Please install it: pip install googlesearch-python")
    def google_search_function_actual(query, num_results, lang, **kwargs):
        st.error("googlesearch-python library not found. Cannot perform Google searches.")
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
            st.warning("fake-useragent failed to get a UA. Using fallback.", icon="⚠️")
            return {
                "User-Agent": "Mozilla/5.0 ... Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }

except (ImportError, FakeUserAgentError, Exception) as e:
    st.warning(f"fake-useragent unavailable ({e}). Using default UA.", icon="⚠️")

    def get_random_headers_general():
        return {
            "User-Agent": "Mozilla/5.0 ... Safari/537.36",
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
UNNAMED_GROUP_PLACEHOLDER = "Unnamed Group"
IMAGE_PATTERN_PPS = re.compile(r'https://pps.whatsapp.net/v/t\d+/[-\w]+/\d+.jpg?')
OG_IMAGE_PATTERN = re.compile(
    r'https?://[^\s/]+/[^\s]+\.(?:jpg|jpeg|png)(?:\?[^\s]*)?',
    re.IGNORECASE
)
MAX_VALIDATION_WORKERS = 8

# --- Custom CSS ---
st.markdown("""
<style>
body { font-family: 'Arial', sans-serif; }
.main-title { font-size: 2.8em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: 600; letter-spacing: -1px; }
.subtitle { font-size: 1.3em; color: #555; text-align: center; margin-top: 5px; margin-bottom: 30px; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 18px; margin: 8px 0; transition: background-color 0.3s ease, transform 0.1s ease; }
.stButton>button:hover { background-color: #1EBE5A; transform: scale(1.03); }
.stButton>button:active { transform: scale(0.98); }
.stProgress > div > div > div > div { background-color: #25D366; border-radius: 4px; }
.metric-card { background-color: #F8F9FA; padding: 15px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); color: #333; text-align: center; margin-bottom: 15px; border: 1px solid #E9ECEF; }
.metric-card .metric-value { font-size: 2em; font-weight: 700; margin-top: 5px; margin-bottom: 0; line-height: 1.2; color: #25D366; }
.stTextInput > div > div > input, .stTextArea > div > textarea, .stNumberInput > div > div > input { border: 1px solid #CED4DA !important; border-radius: 6px !important; padding: 10px !important; box-shadow: inset 0 1px 2px rgba(0,0,0,0.075); }
.stTextInput > div > div > input:focus, .stTextArea > div > textarea:focus, .stNumberInput > div > div > input:focus { border-color: #25D366 !important; box-shadow: 0 0 0 0.2rem rgba(37, 211, 102, 0.25) !important; }
.st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F8F9FA; border-radius: 6px; }
.stExpander { border: 1px solid #E9ECEF; border-radius: 8px; padding: 12px; margin-top: 15px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; font-size: 1.2em; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #1EBE5A; font-size: 1.1em; }
.filter-container { background-color: #FDFDFD; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px dashed #DDE2E5; }
.filter-container .stTextInput input, .filter-container .stNumberInput input { background-color: #fff; }
h4 { color: #259952; margin-top:10px; margin-bottom:10px; border-left: 3px solid #25D366; padding-left: 8px;}
.whatsapp-groups-table { border-collapse: collapse; width: 100%; margin-top: 15px; box-shadow: 0 3px 6px rgba(0,0,0,0.08); border-radius: 8px; overflow: hidden; border: 1px solid #DEE2E6; }
.whatsapp-groups-table caption { caption-side: top; text-align: left; font-weight: 600; padding: 12px 15px; font-size: 1.15em; color: #343A40; background-color: #F8F9FA; border-bottom: 1px solid #DEE2E6;}
.whatsapp-groups-table th { background-color: #343A40; color: white; padding: 14px 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; font-size: 0.9em; }
.whatsapp-groups-table th:nth-child(1) { text-align: center; width: 80px; }
.whatsapp-groups-table th:nth-child(2) { text-align: left; }
.whatsapp-groups-table th:nth-child(3) { text-align: right; width: 150px; }
.whatsapp-groups-table tr { border-bottom: 1px solid #EAEEF2; }
.whatsapp-groups-table tr:last-child { border-bottom: none; }
.whatsapp-groups-table tr:nth-child(even) { background-color: #F9FAFB; }
.whatsapp-groups-table tr:hover { background-color: #EFF8FF; }
.whatsapp-groups-table td { padding: 12px; vertical-align: middle; text-align: left; font-size: 0.95em; }
.whatsapp-groups-table td:nth-child(1) { width: 60px; padding-right: 8px; text-align: center; }
.whatsapp-groups-table td:nth-child(2) { padding-left: 8px; padding-right: 12px; word-break: break-word; font-weight: 500; color: #212529; }
.whatsapp-groups-table td:nth-child(3) { width: 140px; text-align: right; padding-left: 12px; }
.group-logo-img { width: 45px; height: 45px; border-radius: 50%; object-fit: cover; display: block; margin: 0 auto; border: 2px solid #F0F0F0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.join-button { display: inline-block; background-color: #25D366; color: #FFFFFF !important; padding: 7px 14px; border-radius: 6px; text-decoration: none; font-weight: 500; text-align: center; white-space: nowrap; font-size: 0.85em; transition: background-color 0.2s ease, transform 0.1s ease; }
.join-button:hover { background-color: #1DB954; color: #FFFFFF !important; text-decoration: none; transform: translateY(-1px); }
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
    result = {"Group Name": "", "Group Link": link, "Logo URL": "", "Status": "Error"}
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
            if group_name: 
                result["Group Name"] = group_name
                group_name_found = True
        
        if not group_name_found:
            potential_name_tags = soup.find_all(['h2', 'strong', 'span'], class_=re.compile('group-name', re.IGNORECASE)) + soup.find_all('div', class_=re.compile('name', re.IGNORECASE))
            for tag in potential_name_tags:
                text = tag.get_text().strip()
                if text and len(text) > 2 and text.lower() not in ["whatsapp group invite", "whatsapp", "join group", "invite link"]:
                    result["Group Name"] = text
                    group_name_found = True
                    break
        
        # Mark as Inactive if no valid group name found
        if not group_name_found:
            result["Status"] = "Inactive"
            return result

        logo_found = False
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            src = html.unescape(meta_image['content'])
            if OG_IMAGE_PATTERN.match(src) or src.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                result["Logo URL"] = src
                logo_found = True
                
        if not logo_found:
            for img in soup.find_all('img', src=True):
                src = html.unescape(img['src'])
                if src.startswith('https://pps.whatsapp.net/'):
                    result["Logo URL"] = src
                    logo_found = True
                    break

        if result["Status"] == "Error":
            result["Status"] = "Active"
        elif result["Status"] == "Expired" and (group_name_found or logo_found):
            if soup.find('a', attrs={'id': 'action-button', 'href': link}):
                result["Status"] = "Active"

    except requests.exceptions.Timeout: 
        result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError: 
        result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e: 
        result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: 
        result["Status"] = f"Parsing Error ({type(e).__name__})"
    
    return result

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, Scrape, Validate, and Manage WhatsApp Group Links with Enhanced Filtering.</p>', unsafe_allow_html=True)

    # Initialize session state with default filters
    if 'results' not in st.session_state: 
        st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: 
        st.session_state.processed_links_in_session = set()
    if 'styled_table_name_keywords' not in st.session_state: 
        st.session_state.styled_table_name_keywords = ""
    if 'styled_table_current_limit_value' not in st.session_state: 
        st.session_state.styled_table_current_limit_value = 50
    if 'adv_filter_status' not in st.session_state: 
        st.session_state.adv_filter_status = ["Active"]  # Default filter set to Active
    if 'adv_filter_name_keywords' not in st.session_state: 
        st.session_state.adv_filter_name_keywords = ""

    # Ensure processed_links_in_session is a set
    if not isinstance(st.session_state.processed_links_in_session, set):
        st.session_state.processed_links_in_session = set()
        
    # Populate processed links from existing results
    if isinstance(st.session_state.results, list):
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and res_item['Group Link']:
                try:
                    parsed_link = urlparse(res_item['Group Link'])
                    normalized_link = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
                    st.session_state.processed_links_in_session.add(normalized_link)
                except Exception:
                    st.session_state.processed_links_in_session.add(res_item['Group Link'])

    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV/Excel)"
        ], key="input_method_main_select")

        # Increased Google search limit to 100
        gs_top_n = 5
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            gs_top_n = st.slider("Google Results to Scrape (per keyword)", 1, 100, 5, key="gs_top_n_slider", 
                                help="Number of Google search result pages to analyze per keyword.")
        
        crawl_depth, crawl_pages = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be slow. Use with caution.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 300, 50, key="crawl_pages_slider")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button"):
st.session_state.results, st.session_state.processed_links_in_session = [], set()
st.session_state.styled_table_name_keywords = ""
st.session_state.styled_table_current_limit_value = 50
st.session_state.adv_filter_status = ["Active"] # Reset to default
st.session_state.adv_filter_name_keywords = ""
st.cache_data.clear(); st.success("Results & filters cleared!"); st.rerun() 
# Action Zone
current_action_scraped_links = set()
st.subheader(f"🚀 Action Zone: {input_method}")

try:
    if input_method == "Search and Scrape from Google":
        query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input")
        if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button"):
            if query: 
                current_action_scraped_links.update(google_search_and_scrape(query, gs_top_n))
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
                    current_action_scraped_links.update(links_from_kw)
                    total_l = len(current_action_scraped_links)
                    prog_b.progress((i+1)/len(keywords))
                stat_b.success(f"Bulk done. Found {total_l} links.")
            else: 
                st.warning("No valid keywords in Excel.")

    elif input_method == "Scrape from Specific Webpage URL":
        url = st.text_input("Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
        if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
            if url and (url.startswith("http://") or url.startswith("https://")):
                with st.spinner(f"Scraping {url}..."):
                    current_action_scraped_links.update(scrape_whatsapp_links_from_page(url))
                st.success(f"Scraping done. Found {len(current_action_scraped_links)} links.")
            else: 
                st.warning("Please enter a valid URL.")

    elif input_method == "Scrape from Entire Website (Extensive Crawl)":
        domain = st.text_input("Base Domain URL:", placeholder="example.com", key="crawl_domain_input")
        if st.button("Crawl & Scrape", use_container_width=True, key="crawl_button"):
            if domain:
                st.info("Starting crawl. Progress in sidebar.")
                current_action_scraped_links.update(crawl_website(domain, crawl_depth, crawl_pages))
                st.success(f"Crawl done. Found {len(current_action_scraped_links)} links.")
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
                current_action_scraped_links.update(valid_links)
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
                        current_action_scraped_links.update(links_from_kw)
                        total_le = len(current_action_scraped_links)
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
                    current_action_scraped_links.update(valid_links)
                else: 
                    st.warning("No links in file.")
            else: 
                st.warning("Unsupported file. Use .txt, .csv, or .xlsx.")
except Exception as e: 
    st.error(f"Input/Scraping Error: {e}", icon="💥")

# Validation process
links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)
if links_to_validate_now:
    st.success(f"Found {len(current_action_scraped_links)} links. Validating {len(links_to_validate_now)} new links...")
    prog_val, stat_val = st.progress(0), st.empty()
    new_results_this_run = []
    with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
        future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
        for i, future in enumerate(as_completed(future_to_link)):
            link_validated = future_to_link[future]
            try:
                result_validated = future.result()
                new_results_this_run.append(result_validated)
                parsed_url_val = urlparse(link_validated)
                normalized_link_val = f"{parsed_url_val.scheme}://{parsed_url_val.netloc}{parsed_url_val.path}"
                st.session_state.processed_links_in_session.add(normalized_link_val)
            except Exception as val_exc:
                st.warning(f"Error validating {link_validated[:40]}...: {val_exc}", icon="⚠️")
                parsed_url_val_err = urlparse(link_validated)
                normalized_link_val_err = f"{parsed_url_val_err.scheme}://{parsed_url_val_err.netloc}{parsed_url_val_err.path}"
                st.session_state.processed_links_in_session.add(normalized_link_val_err)
                new_results_this_run.append({"Group Name": "Validation Error", "Group Link": link_validated, "Logo URL": "", "Status": f"Validation Failed: {type(val_exc).__name__}"})
            prog_val.progress((i+1)/len(links_to_validate_now))
            stat_val.text(f"Validated {i+1}/{len(links_to_validate_now)} links")
    
    if new_results_this_run:
        st.session_state.results.extend(new_results_this_run)
    stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
elif current_action_scraped_links and not links_to_validate_now:
    st.info("No *new* WhatsApp links found from this action. All were previously processed.")

# Results Display
if 'results' in st.session_state and st.session_state.results:
    unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
    st.session_state.results = unique_results_df.to_dict('records')
    df_display_master = unique_results_df.reset_index(drop=True)

    # Filter out Inactive groups by default
    active_df_all_master = df_display_master[df_display_master['Status'] == 'Active'].copy()
    expired_df_master = df_display_master[df_display_master['Status'] == 'Expired'].copy()
    inactive_df_master = df_display_master[df_display_master['Status'] == 'Inactive'].copy()
    error_df_master = df_display_master[~df_display_master['Status'].isin(['Active', 'Expired', 'Inactive'])].copy()

    st.subheader("📊 Results Summary")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master)}</div></div>', unsafe_allow_html=True)
    col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df_all_master)}</div></div>', unsafe_allow_html=True)
    col3.markdown(f'<div class="metric-card">Expired Links<br><div class="metric-value">{len(expired_df_master)}</div></div>', unsafe_allow_html=True)
    col4.markdown(f'<div class="metric-card">Inactive Links<br><div class="metric-value">{len(inactive_df_master)}</div></div>', unsafe_allow_html=True)
    col5.markdown(f'<div class="metric-card">Other Status<br><div class="metric-value">{len(error_df_master)}</div></div>', unsafe_allow_html=True)

    # Styled Table with Filters
    st.subheader("✨ Active Groups Display (Styled Table)")
    with st.expander("View and Filter Active Groups", expanded=True):
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

    # Advanced Filtering for Downloads
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

    # Downloads - Exclude Inactive groups from default "All Results"
    st.subheader("📥 Download Results (CSV)")
    dl_col1, dl_col2 = st.columns(2)
    if not active_df_all_master.empty:
        dl_col1.download_button("Active Groups (CSV)", active_df_all_master.to_csv(index=False).encode('utf-8'), "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv_main_key")
    else:
        dl_col1.button("Active Groups (CSV)", disabled=True, use_container_width=True, help="No active groups to download.")

    # Exclude Inactive groups by default
    df_default_download = df_display_master[~df_display_master['Status'].isin(['Inactive'])]
    if not df_default_download.empty:
        download_label = "All Processed Results (Excluding Inactive)"
        dl_col2.download_button(download_label, df_default_download.to_csv(index=False).encode('utf-8'), "processed_results.csv", "text/csv", use_container_width=True, key="dl_all_or_filtered_csv_key")
    elif not df_display_master.empty and df_default_download.empty:
        dl_col2.button("No Results After Filtering", disabled=True, use_container_width=True)
    else:
        dl_col2.button("All Processed Results (CSV)", disabled=True, use_container_width=True, help="No results to download.")
        
else:
    st.info("Start by searching, entering, or uploading links to see results!", icon="ℹ️")
if name == "main":
main()
