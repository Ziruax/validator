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
    def google_search_function_actual(query, num_results, lang, **kwargs): # type: ignore
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
            st.warning("fake-useragent failed to get a User-Agent. Using fallback.", icon="⚠️")
            return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception as e_random: # Catch any other potential error during .random call
            st.warning(f"Error getting random User-Agent: {e_random}. Using fallback.", icon="⚠️")
            return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
except ImportError:
    st.warning("fake-useragent library not found. Install with pip install fake-useragent. Using default User-Agent.", icon="⚠️")
    def get_random_headers_general():
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except FakeUserAgentError as e_init: # Catch error during UserAgent() initialization
    st.warning(f"Error initializing fake-useragent: {e_init}. Using default User-Agent.", icon="⚠️")
    def get_random_headers_general():
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except Exception as e_general_init: # Catch any other error during UserAgent() initialization
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
# MODIFIED: Validation Logic - Changed default "Group Name" from "Unknown" to an empty string
UNNAMED_GROUP_PLACEHOLDER = "" # Was "Unnamed Group"
IMAGE_PATTERN_PPS = re.compile(r'https://pps.whatsapp.net/v/t\d+/[-\w]+/\d+\.jpg\?.*') # More specific PPS pattern
OG_IMAGE_PATTERN = re.compile(r'https?://[^\/\s]+/[^\/\s]+\.(jpg|jpeg|png)(\?[^\s]*)?')
MAX_VALIDATION_WORKERS = 8
# GOOGLE_SEARCH_LIMIT = 100 # This will be handled by the slider's max value

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
.st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F8F9FA; border-radius: 6px; } /* May need updates if Streamlit changes class names */
.stExpander { border: 1px solid #E9ECEF; border-radius: 8px; padding: 12px; margin-top: 15px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; font-size: 1.2em; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #1EBE5A; font-size: 1.1em; }
.filter-container { background-color: #FDFDFD; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px dashed #DDE2E5; }
/* .filter-container .stTextInput input, .filter-container .stNumberInput input { background-color: #fff;zczy} /* Incomplete CSS rule? Commenting out */
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
                # Use StringIO to treat the string as a file
                df = pd.read_csv(io.StringIO(text_content))
                if df.empty: st.warning("CSV file is empty."); return []
                # Assuming links are in the first column
                return [link.strip() for link in df.iloc[:, 0].dropna().astype(str).tolist() if link.strip().startswith(('http://', 'https://'))]
            except Exception as e:
                st.error(f"Error reading CSV: {e}.", icon="❌"); return []
        else: # Assume TXT
            return [line.strip() for line in text_content.splitlines() if line.strip()]
    except Exception as e:
        st.error(f"Error processing file {uploaded_file.name}: {e}", icon="❌"); return []

# --- Core Logic Functions ---
def validate_link(link):
    # MODIFIED: Validation Logic - Default "Group Name" is "" (via UNNAMED_GROUP_PLACEHOLDER), default "Status" is "Inactive"
    result = {"Group Name": UNNAMED_GROUP_PLACEHOLDER, "Group Link": link, "Logo URL": "", "Status": "Inactive"}
    try:
        response = requests.get(link, headers=get_random_headers_general(), timeout=20, allow_redirects=True)
        response.encoding = 'utf-8' # Ensure UTF-8 encoding

        if response.status_code != 200:
            result["Status"] = "Expired (404 Not Found)" if response.status_code == 404 else f"HTTP Error {response.status_code}"
            return result
        
        if WHATSAPP_DOMAIN not in response.url: # Check final URL after redirects
            final_netloc = urlparse(response.url).netloc or 'Unknown Site'
            result["Status"] = f"Redirected Away ({final_netloc})"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        page_text_lower = soup.get_text().lower()
        expired_phrases = ["invite link is invalid", "invite link was reset", "group doesn't exist", "this group is no longer available"]
        
        group_name_found = False
        # Try to get group name from OG:TITLE first
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = html.unescape(meta_title['content']).strip()
            if group_name: # Ensure name is not empty
                result["Group Name"] = group_name
                group_name_found = True
        
        # Fallback: If OG:TITLE not found or empty, try other common tags
        if not group_name_found:
            potential_name_tags = soup.find_all(['h2', 'strong', 'span'], class_=re.compile(r'\b(group-name|name)\b', re.IGNORECASE)) + \
                                  soup.find_all('div', class_=re.compile(r'\b(name)\b', re.IGNORECASE)) # More specific class search
            for tag in potential_name_tags:
                text = tag.get_text(separator=' ', strip=True) # Get text and strip whitespace
                # Filter out common placeholder/generic texts
                if text and len(text) > 2 and text.lower() not in ["whatsapp group invite", "whatsapp", "join group", "invite link", "open chat", "open this link"]:
                    result["Group Name"] = text
                    group_name_found = True
                    break
        
        logo_url_found = False
        # Try OG:IMAGE for logo
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            src = html.unescape(meta_image['content'])
            if OG_IMAGE_PATTERN.match(src) or src.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')): # Basic validation
                result["Logo URL"] = src
                logo_url_found = True

        # Fallback: Try specific WhatsApp PPS image pattern or any img tag with pps.whatsapp.net
        if not logo_url_found:
            for img in soup.find_all('img', src=True):
                src = html.unescape(img['src'])
                if src.startswith('https://pps.whatsapp.net/'): # Main WhatsApp profile picture server
                    if IMAGE_PATTERN_PPS.match(src): # Check against more specific pattern if needed
                         result["Logo URL"] = src
                         logo_url_found = True
                         break
        
        # MODIFIED: Validation Logic - Status determination
        is_explicitly_expired_by_text = any(phrase in page_text_lower for phrase in expired_phrases)
        action_button_present = soup.find('a', attrs={'id': 'action-button', 'href': link}) is not None

        if is_explicitly_expired_by_text:
            if group_name_found and action_button_present:
                result["Status"] = "Active"  # Override "Expired" if name and join button exist
            else:
                result["Status"] = "Expired"
        elif group_name_found: # Not explicitly expired by text, and has a name
            result["Status"] = "Active"
        else: # No name found, or was explicitly expired without conditions to be Active
            # Status remains "Inactive" (the default) if no group name is found
            # and it wasn't set to "Expired" by text phrases.
            if not is_explicitly_expired_by_text: # Ensure it doesn't overwrite an "Expired" status if no name
                 result["Status"] = "Inactive"


    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError: result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: result["Status"] = f"Parsing Error ({type(e).__name__})" # Catch other errors
    
    return result

def scrape_whatsapp_links_from_page(url, session=None):
    links = set()
    try:
        headers = get_random_headers_general()
        response = session.get(url, headers=headers, timeout=15) if session else requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8' # Ensure correct encoding
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find in <a> tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                parsed_url = urlparse(href)
                # Keep only scheme, netloc, path; remove query params and fragments for uniqueness
                links.add(f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}")

        # Find in raw text content (more robust regex)
        text_content = soup.get_text() # Consider response.text for less processing if HTML structure is an issue
        if WHATSAPP_DOMAIN in text_content: # Quick check
            # Regex to find WhatsApp chat URLs, ensuring they have a path component
            # It tries to avoid matching URLs ending with common file extensions unless part of query
            raw_found_links = re.findall(r'(https?://chat\.whatsapp\.com/[A-Za-z0-9_-]{16,})', text_content) # Common length for invite codes
            for link_url in raw_found_links:
                clean_link = link_url.strip().split('&')[0] # Remove params like 'text' often appended
                parsed_url = urlparse(clean_link)
                # Further check on path length/structure if needed
                if len(parsed_url.path.replace('/', '')) >= 16: # Typical invite codes are longer
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
        # Use the actual imported function
        search_page_urls = list(google_search_function_actual(query, num_results=top_n, lang="en")) # stop=top_n, pause=2.0 are defaults
        
        if not search_page_urls:
            st.warning(f"No Google results for '{query}'. Possible reasons: "
                       f"1. Query genuinely yields no results. "
                       f"2. Google blocking (try VPN/wait, or reduce search frequency/volume). "
                       f"3. `googlesearch-python` library issue or API change.", icon="🤔")
            return []

        st.success(f"Found {len(search_page_urls)} pages from Google. Scraping them for WhatsApp links...")
        prog_bar, stat_txt = st.progress(0), st.empty()
        
        with requests.Session() as scrape_session: # Use a session for potential keep-alive benefits
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

    except TypeError as e: # Often related to googlesearch library issues or parameters
        st.error(f"Google search TypeError: {e}. Check `googlesearch-python` version/parameters, or for Google blocking.", icon="❌")
        return []
    except Exception as e: # Catch-all for other unexpected errors during search/scrape
        st.error(f"Unexpected Google search/scrape error for '{query}': {e}. Check connection/library, or for Google blocking.", icon="❌")
        return []

def crawl_website(start_url, max_depth=2, max_pages=50):
    scraped_whatsapp_links = set()
    if not start_url.strip(): return scraped_whatsapp_links

    if not start_url.startswith(('http://', 'https://')):
        start_url = 'https://' + start_url
        st.sidebar.warning(f"Prepending 'https://': {start_url}", icon="🔗")

    parsed_start_url = urlparse(start_url)
    if not parsed_start_url.netloc: # Basic validation
        st.sidebar.error(f"Invalid start URL: {start_url}", icon="🚫")
        return scraped_whatsapp_links
    
    base_domain = parsed_start_url.netloc.replace('www.', '') # Normalize domain

    urls_in_queue_tuples, visited_urls, queue_list = set(), set(), [] # Visited for normalized paths
    queue_list.append((start_url, 0)) # Store (url, depth)
    urls_in_queue_tuples.add((start_url, 0)) # For quick check if (url, depth) combo is already added

    page_count = 0
    max_q_size = max_pages * 10 # Heuristic to prevent excessively large queues

    with requests.Session() as session, st.spinner(f"Crawling {base_domain}... (Max Depth: {max_depth}, Max Pages: {max_pages})"):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size: # Safety break for queue size
                st.sidebar.warning(f"Crawl queue exceeded {max_q_size}. Stopping early.", icon="❗️")
                queue_list = queue_list[:max_q_size] # Trim queue if too large
            
            current_url, depth = queue_list.pop(0)
            
            # Normalize URL for visited check (path part only, ignore query/fragment for visited pages)
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')

            if normalized_current_url in visited_urls or depth > max_depth:
                continue
            
            visited_urls.add(normalized_current_url)

            if page_count >= max_pages: break # Ensure we don't exceed max_pages

            st.sidebar.text(f"Crawl (D:{depth},P:{page_count+1}/{max_pages},Q:{len(queue_list)}): {current_url[:50]}...")
            
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10)
                response.raise_for_status() # Check for HTTP errors

                # Only process HTML content
                if 'text/html' not in response.headers.get('Content-Type', '').lower():
                    continue
                
                page_count += 1 # Count successfully fetched HTML pages

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
                            abs_url = urljoin(current_url, href) # Resolve relative URLs
                            parsed_abs_url = urlparse(abs_url)

                            # Stay within the same domain and scheme, ignore fragments for queue uniqueness
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain and \
                               not parsed_abs_url.fragment: # Avoid crawling fragment URLs as new pages
                                
                                normalized_abs_url_for_visited = urljoin(abs_url, parsed_abs_url.path or '/') # For visited check
                                
                                if normalized_abs_url_for_visited not in visited_urls and \
                                   (abs_url, depth + 1) not in urls_in_queue_tuples: # Check if this exact URL at next depth is new
                                    queue_list.append((abs_url, depth + 1))
                                    urls_in_queue_tuples.add((abs_url, depth + 1))
            
            except requests.exceptions.RequestException as e:
                st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: # Catch other parsing errors, etc.
                st.sidebar.error(f"Crawl Gen Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
    
    st.sidebar.success(f"Crawl complete. Scraped {page_count} pages, found {len(scraped_whatsapp_links)} unique WhatsApp links.")
    if page_count >= max_pages: st.sidebar.warning(f"Reached max pages limit ({max_pages}).", icon="❗️")
    if len(queue_list) >= max_q_size: st.sidebar.warning(f"Crawl queue was capped at {max_q_size}.", icon="❗️")
    return scraped_whatsapp_links

def generate_styled_html_table(data_df_for_table):
    # MODIFIED: UNNAMED_GROUP_PLACEHOLDER is now "", so this filters out groups with no name.
    # The input `data_df_for_table` is already filtered for 'Active' status.
    df_to_display = data_df_for_table[data_df_for_table['Group Name'] != UNNAMED_GROUP_PLACEHOLDER].copy()

    if df_to_display.empty:
        return "<p style='text-align:center; color:#777; margin-top:20px;'><i>No active groups match the current display filters. Try adjusting them or broadening your search.</i></p>"

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
        group_name = row.get("Group Name", "") # Default to empty if somehow missing after filter
        group_link = row.get("Group Link", "")
        
        html_string += '<tr>'
        html_string += '<td class="group-logo-cell">'
        alt_text = f"{html.escape(group_name)} Group Logo" if group_name else "Group Logo"
        if logo_url:
            # Append query param for WhatsApp profile pics for better sizing, if applicable
            display_logo_url = append_query_param(logo_url, 'w', '96') if logo_url.startswith('https://pps.whatsapp.net/') else logo_url
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img" loading="lazy" onerror="this.style.display=\'none\'; this.nextSibling.style.display=\'flex\';">' # Fallback for broken image
            html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:none; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>' # Placeholder shown on error
        else:
            html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        html_string += '</td>'
        
        safe_group_name = html.escape(group_name) if group_name else "<i>No Name Provided</i>"
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
    
    # Initialize session state variables
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()
    if 'styled_table_name_keywords' not in st.session_state: st.session_state.styled_table_name_keywords = ""
    if 'styled_table_current_limit_value' not in st.session_state: st.session_state.styled_table_current_limit_value = 50
    if 'adv_filter_status' not in st.session_state: st.session_state.adv_filter_status = []
    if 'adv_filter_name_keywords' not in st.session_state: st.session_state.adv_filter_name_keywords = ""
    # MODIFIED: Default Filter - Flag to manage default application of "Active" filter
    if 'adv_filter_status_default_applied' not in st.session_state: st.session_state.adv_filter_status_default_applied = False

    # Ensure processed_links_in_session is a set and populate it (idempotent)
    if not isinstance(st.session_state.processed_links_in_session, set):
        st.session_state.processed_links_in_session = set()
    if isinstance(st.session_state.results, list): # Could be run multiple times
        current_processed_links = set()
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and res_item['Group Link']:
                try:
                    parsed_link = urlparse(res_item['Group Link'])
                    normalized_link = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
                    current_processed_links.add(normalized_link)
                except Exception: # Fallback if parsing fails
                    current_processed_links.add(res_item['Group Link'])
        st.session_state.processed_links_in_session.update(current_processed_links)


    # Sidebar
    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV/Excel)"
        ], key="input_method_main_select")

        gs_top_n = 5 # Default
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            # MODIFIED: Google Search Limit - Increased slider max to 100
            gs_top_n = st.slider(
                "Google Results to Scrape (per keyword)", 
                min_value=1, max_value=100, value=5, key="gs_top_n_slider", # Max value increased
                help="Number of Google search result pages to analyze per keyword. Higher values (>20) may be slow or lead to Google blocks."
            )
            # MODIFIED: Google Search Limit - Added warning
            if gs_top_n > 20:
                st.warning(f"Searching for {gs_top_n} results per keyword may be very slow and significantly increase the risk of temporary Google blocks. Recommended: <= 20.", icon="⚠️")
        
        crawl_depth, crawl_pages = 2, 50 # Defaults
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be very slow and resource-intensive. Use with caution on large websites.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider", help="0 for current page only.")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 300, 50, key="crawl_pages_slider")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.session_state.styled_table_name_keywords = ""
            st.session_state.styled_table_current_limit_value = 50
            st.session_state.adv_filter_status = []
            st.session_state.adv_filter_name_keywords = ""
            # MODIFIED: Default Filter - Reset flag on clear
            st.session_state.adv_filter_status_default_applied = False
            st.cache_data.clear() # Clear Streamlit's internal data caches if used
            st.success("Results, processed link history, and filters cleared successfully!")
            st.rerun()

    # Action Zone
    current_action_scraped_links = set() # Links found in the current user action
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input")
            if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button"):
                if query: current_action_scraped_links.update(google_search_and_scrape(query, gs_top_n))
                else: st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            file = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if file and st.button("Process Excel, Scrape & Validate", use_container_width=True, key="gs_bulk_button"):
                keywords = load_keywords_from_excel(file)
                if keywords:
                    st.info(f"Processing {len(keywords)} keywords from Excel...")
                    prog_b, stat_b = st.progress(0), st.empty()
                    total_l_bulk = 0
                    for i, kw in enumerate(keywords):
                        stat_b.text(f"Keyword: '{kw}' ({i+1}/{len(keywords)}). Total links found so far: {total_l_bulk}")
                        links_from_kw = google_search_and_scrape(kw, gs_top_n)
                        current_action_scraped_links.update(links_from_kw)
                        total_l_bulk = len(current_action_scraped_links) # Update count after adding
                        prog_b.progress((i+1)/len(keywords))
                    stat_b.success(f"Bulk Excel processing complete. Found {total_l_bulk} unique WhatsApp links across all keywords.")
                else: st.warning("No valid keywords found in the uploaded Excel file.")

        elif input_method == "Scrape from Specific Webpage URL":
            url = st.text_input("Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if url and (url.startswith("http://") or url.startswith("https://")):
                    with st.spinner(f"Scraping {url}..."):
                        current_action_scraped_links.update(scrape_whatsapp_links_from_page(url))
                    st.success(f"Scraping of {url} complete. Found {len(current_action_scraped_links)} unique WhatsApp links on this page.")
                else: st.warning("Please enter a valid URL (starting with http:// or https://).")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain = st.text_input("Base Domain URL:", placeholder="example.com (e.g., mysite.com or newsportal.org)", key="crawl_domain_input")
            if st.button("Crawl & Scrape Website", use_container_width=True, key="crawl_button"):
                if domain:
                    st.info(f"Starting website crawl for '{domain}'. Progress will be shown in the sidebar.")
                    current_action_scraped_links.update(crawl_website(domain, crawl_depth, crawl_pages))
                    st.success(f"Website crawl for '{domain}' complete. Found {len(current_action_scraped_links)} unique WhatsApp links.")
                else: st.warning("Please enter a base domain URL to crawl.")

        elif input_method == "Enter Links Manually (for Validation)":
            text = st.text_area("WhatsApp Links (one per line):", height=200, key="manual_links_area", placeholder=f"e.g., {WHATSAPP_DOMAIN}xxxxxxxxxxxxxx\n{WHATSAPP_DOMAIN}yyyyyyyyyyyyyy")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                links = [line.strip() for line in text.split('\n') if line.strip()]
                if links:
                    valid_links = {l for l in links if l.startswith(WHATSAPP_DOMAIN)}
                    invalid_format_count = len(links) - len(valid_links)
                    if invalid_format_count > 0:
                        st.warning(f"Skipped {invalid_format_count} entries that do not look like valid WhatsApp group links.")
                    if valid_links:
                        current_action_scraped_links.update(valid_links)
                        st.info(f"{len(valid_links)} WhatsApp links queued for validation.")
                    else:
                        st.warning("No valid WhatsApp group links found in the input.")
                else: st.warning("Please enter some WhatsApp links to validate.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file = st.file_uploader("Upload TXT/CSV (links in 1st col) or Excel (keywords in 1st col)", type=["txt", "csv", "xlsx"], key="upload_file_input")
            if file and st.button("Process File & Validate/Scrape", use_container_width=True, key="upload_process_button"):
                if file.name.endswith('.xlsx'):
                    st.info("Processing Excel file for keywords to search on Google...")
                    keywords = load_keywords_from_excel(file)
                    if keywords:
                        prog_e, stat_e = st.progress(0), st.empty()
                        total_le = 0
                        for i, kw in enumerate(keywords):
                            stat_e.text(f"Keyword: {kw} ({i+1}/{len(keywords)}). Links found so far: {total_le}")
                            links_from_kw = google_search_and_scrape(kw, gs_top_n)
                            current_action_scraped_links.update(links_from_kw)
                            total_le = len(current_action_scraped_links)
                            prog_e.progress((i+1)/len(keywords))
                        stat_e.success(f"Excel (keywords) processing complete. Found {total_le} unique WhatsApp links.")
                    else: st.warning("No valid keywords found in the Excel file.")
                elif file.name.endswith(('.txt', '.csv')):
                    st.info("Processing TXT/CSV file for direct WhatsApp links...")
                    links = load_links_from_file(file)
                    if links:
                        valid_links = {l for l in links if l.startswith(WHATSAPP_DOMAIN)}
                        invalid_format_count = len(links) - len(valid_links)
                        if invalid_format_count > 0:
                            st.warning(f"Skipped {invalid_format_count} entries from the file that do not look like valid WhatsApp group links.")
                        if valid_links:
                            current_action_scraped_links.update(valid_links)
                            st.info(f"{len(valid_links)} WhatsApp links from file queued for validation.")
                        else:
                            st.warning("No valid WhatsApp group links found in the uploaded file.")
                    else: st.warning("No links found or able to be processed from the uploaded file.")
                else: st.warning("Unsupported file type. Please upload a .txt, .csv, or .xlsx file.")
    except Exception as e: st.error(f"An error occurred during the input/scraping phase: {e}", icon="💥")

    # Validation Phase
    links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)

    if links_to_validate_now:
        st.success(f"Total {len(current_action_scraped_links)} unique links gathered. Validating {len(links_to_validate_now)} new/unprocessed links...")
        prog_val, stat_val = st.progress(0), st.empty()
        new_results_this_run = []
        
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
            for i, future in enumerate(as_completed(future_to_link)):
                link_validated = future_to_link[future]
                try:
                    result_validated = future.result()
                    new_results_this_run.append(result_validated)
                    # Normalize link before adding to processed_links_in_session
                    parsed_url_val = urlparse(link_validated)
                    normalized_link_val = f"{parsed_url_val.scheme}://{parsed_url_val.netloc}{parsed_url_val.path}"
                    st.session_state.processed_links_in_session.add(normalized_link_val)
                except Exception as val_exc: # Should be caught by validate_link, but as a fallback
                    st.warning(f"Critical error validating {link_validated[:40]}...: {val_exc}", icon="⚠️")
                    # Add to processed set even if validation fails critically to avoid re-processing
                    parsed_url_val_err = urlparse(link_validated) # type: ignore
                    normalized_link_val_err = f"{parsed_url_val_err.scheme}://{parsed_url_val_err.netloc}{parsed_url_val_err.path}" # type: ignore
                    st.session_state.processed_links_in_session.add(normalized_link_val_err) # type: ignore
                    new_results_this_run.append({"Group Name": "Validation Error", "Group Link": link_validated, "Logo URL": "", "Status": f"CritValidationFail: {type(val_exc).__name__}"}) # type: ignore
                
                prog_val.progress((i+1)/len(links_to_validate_now))
                stat_val.text(f"Validated {i+1}/{len(links_to_validate_now)} links. Last: {link_validated[:40]}... Status: {new_results_this_run[-1]['Status']}")
        
        if new_results_this_run:
            st.session_state.results.extend(new_results_this_run)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
    elif current_action_scraped_links and not links_to_validate_now:
        st.info("All WhatsApp links found in this action were already processed in this session. No new links to validate.")

    # Results Display
    if 'results' in st.session_state and st.session_state.results:
        # Deduplicate results based on 'Group Link', keeping the first occurrence
        unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = unique_results_df.to_dict('records') # Update session state with deduplicated list
        
        df_display_master = unique_results_df.reset_index(drop=True)

        # Create subsets for metrics
        active_df_all_master = df_display_master[df_display_master['Status'] == 'Active'].copy() # Strictly 'Active'
        expired_df_master = df_display_master[df_display_master['Status'].str.contains('Expired', na=False)].copy()
        inactive_df_master = df_display_master[df_display_master['Status'] == 'Inactive'].copy()
        error_df_master = df_display_master[
            ~df_display_master['Status'].isin(['Active', 'Inactive']) & \
            ~df_display_master['Status'].str.contains('Expired', na=False)
        ].copy()


        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master)}</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card">Active Groups<br><div class="metric-value">{len(active_df_all_master)}</div></div>', unsafe_allow_html=True)
        # Combine Inactive and Expired for a "Non-Active" category or show separately
        col3.markdown(f'<div class="metric-card">Inactive/Nameless<br><div class="metric-value">{len(inactive_df_master)}</div></div>', unsafe_allow_html=True)
        col4.markdown(f'<div class="metric-card">Expired/Error<br><div class="metric-value">{len(expired_df_master) + len(error_df_master)}</div></div>', unsafe_allow_html=True)


        # Styled Table with Filters for Active Groups
        st.subheader("✨ Active Groups Display (Styled Table)")
        with st.expander("View and Filter Active Groups", expanded=True):
            if not active_df_all_master.empty:
                st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                st.markdown("#### Filter Displayed Active Groups:")

                with st.form("styled_table_filters_form"):
                    name_keywords_input_styled = st.text_input(
                        "Filter by Group Name Keywords (comma-separated):",
                        value=st.session_state.styled_table_name_keywords,
                        placeholder="e.g., study, fun, tech",
                        help="Enter keywords (comma-separated). Shows active groups matching ANY keyword in their name."
                    ).strip()
                    limit_input_styled = st.number_input(
                        "Max Active Groups to Display in Table:",
                        min_value=1, max_value=len(active_df_all_master) if len(active_df_all_master) > 0 else 1000, # Dynamic max
                        value=st.session_state.styled_table_current_limit_value,
                        step=10,
                        help="Set the maximum number of active groups to display in the styled table."
                    )
                    apply_styled_filters = st.form_submit_button("Apply Filters to Styled Table")

                if apply_styled_filters:
                    st.session_state.styled_table_name_keywords = name_keywords_input_styled
                    st.session_state.styled_table_current_limit_value = limit_input_styled
                    st.rerun() # Rerun to apply filters immediately

                if st.button("Reset Styled Table Filters", key="reset_styled_table_filters_button"):
                    st.session_state.styled_table_name_keywords = ""
                    st.session_state.styled_table_current_limit_value = 50
                    st.rerun()

                # Filter the active dataframe for the styled table
                active_df_for_styled_table = active_df_all_master.copy()
                if st.session_state.styled_table_name_keywords:
                    keywords_list_styled = [kw.strip().lower() for kw in st.session_state.styled_table_name_keywords.split(',') if kw.strip()]
                    if keywords_list_styled:
                        # Ensure 'Group Name' is string type and handle NaN
                        active_df_for_styled_table['Group Name'] = active_df_for_styled_table['Group Name'].astype(str)
                        regex_pattern_styled = '|'.join(map(re.escape, keywords_list_styled))
                        active_df_for_styled_table = active_df_for_styled_table[
                            active_df_for_styled_table['Group Name'].str.lower().str.contains(regex_pattern_styled, na=False, regex=True)
                        ]
                
                num_matching_styled = len(active_df_for_styled_table)
                num_displayed_styled = min(num_matching_styled, st.session_state.styled_table_current_limit_value)
                active_df_for_styled_table_final = active_df_for_styled_table.head(num_displayed_styled)

                if num_matching_styled > 0:
                    st.write(f"Showing {num_displayed_styled} of {num_matching_styled} matching active groups in the styled table.")
                else:
                    st.write("No active groups match the current filters for the styled table.")

                html_out = generate_styled_html_table(active_df_for_styled_table_final)
                st.markdown(html_out, unsafe_allow_html=True)
                st.markdown("---")
                st.text_area("Copy Raw HTML Code (for above table):", value=html_out, height=150, key="styled_html_export_area_key", help="Ctrl+A, Ctrl+C to copy the HTML for embedding elsewhere.")
                st.markdown('</div>', unsafe_allow_html=True) # Close filter-container
            else:
                st.info("No 'Active' groups found yet to display in the styled table. Try scraping/validating more links.")

        # Advanced Filtering for Downloads
        with st.expander("🔬 Advanced Filtering for Downloads & Analysis", expanded=False):
            st.markdown('<div class="filter-container" style="border-style:solid;">', unsafe_allow_html=True)
            st.markdown("#### Filter Full Dataset (for Download/Analysis):")
            
            # MODIFIED: CSV Outputs - Base for this download excludes "Inactive"
            df_base_for_adv_download = df_display_master[df_display_master['Status'] != 'Inactive'].copy()
            
            # Use statuses from this pre-filtered DataFrame for the multiselect options
            all_statuses_for_adv_filter = sorted(list(df_base_for_adv_download['Status'].unique()))

            # MODIFIED: Default Filter - Apply "Active" as default if applicable
            if not st.session_state.adv_filter_status_default_applied and \
               not st.session_state.adv_filter_status and \
               "Active" in all_statuses_for_adv_filter:
                st.session_state.adv_filter_status = ["Active"]
                st.session_state.adv_filter_status_default_applied = True # Mark that default has been applied

            current_adv_filter_status = st.multiselect(
                "Filter by Status (for 'All Processed Results' CSV):", options=all_statuses_for_adv_filter,
                default=st.session_state.adv_filter_status, # Uses potentially pre-filled ["Active"]
                key="adv_status_filter_multiselect_key",
                help="Select statuses to include in the 'All Processed Results' CSV. 'Inactive' groups are already excluded from this output."
            )
            # Update session state immediately if multiselect changes
            if current_adv_filter_status != st.session_state.adv_filter_status:
                st.session_state.adv_filter_status = current_adv_filter_status
                st.session_state.adv_filter_status_default_applied = True # User has interacted, so default logic won't re-apply unless cleared
                # st.rerun() # Optional: rerun on filter change

            current_adv_filter_name_keywords = st.text_input(
                "Filter by Group Name Keywords (for 'All Processed Results' CSV, comma-separated):", 
                value=st.session_state.adv_filter_name_keywords,
                key="adv_name_keyword_filter_input_key", placeholder="e.g., news, jobs, global",
                help="Applies to the 'All Processed Results' CSV. Comma-separated."
            ).strip()
            if current_adv_filter_name_keywords != st.session_state.adv_filter_name_keywords:
                st.session_state.adv_filter_name_keywords = current_adv_filter_name_keywords
                # st.rerun() # Optional: rerun on filter change
            
            st.markdown('</div>', unsafe_allow_html=True)

            # Apply advanced filters to the base DataFrame (which already excludes 'Inactive')
            df_for_adv_download_or_view = df_base_for_adv_download.copy()
            adv_filters_applied_flag = False # True if user selected any advanced filters

            if st.session_state.adv_filter_status: # User has selected specific statuses
                df_for_adv_download_or_view = df_for_adv_download_or_view[
                    df_for_adv_download_or_view['Status'].isin(st.session_state.adv_filter_status)
                ]
                adv_filters_applied_flag = True
            
            if st.session_state.adv_filter_name_keywords:
                adv_keywords_list = [kw.strip().lower() for kw in st.session_state.adv_filter_name_keywords.split(',') if kw.strip()]
                if adv_keywords_list:
                    # Ensure 'Group Name' is string for robust filtering
                    df_for_adv_download_or_view['Group Name'] = df_for_adv_download_or_view['Group Name'].astype(str)
                    adv_regex_pattern = '|'.join(map(re.escape, adv_keywords_list))
                    df_for_adv_download_or_view = df_for_adv_download_or_view[
                        df_for_adv_download_or_view['Group Name'].str.lower().str.contains(adv_regex_pattern, na=False, regex=True)
                    ]
                    adv_filters_applied_flag = True
            
            preview_label = "All (excluding Inactive)"
            if adv_filters_applied_flag:
                preview_label = "Filtered (excluding Inactive)"
            
            st.markdown(f"**Preview of Data for 'All Processed Results' CSV ({preview_label} - {len(df_for_adv_download_or_view)} rows):**")
            st.dataframe(df_for_adv_download_or_view, column_config={
                "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Link", width="medium"),
                "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View Logo", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small")
            }, hide_index=True, height=300, use_container_width=True)

        # Downloads
        st.subheader("📥 Download Results (CSV)")
        dl_col1, dl_col2 = st.columns(2)
        
        # Download for "Active Groups" (only status 'Active')
        if not active_df_all_master.empty:
            dl_col1.download_button(
                label=f"Active Groups ({len(active_df_all_master)} rows) (CSV)", 
                data=active_df_all_master.to_csv(index=False).encode('utf-8'), 
                file_name="active_whatsapp_groups.csv", 
                mime="text/csv", 
                use_container_width=True, 
                key="dl_active_csv_main_key"
            )
        else:
            dl_col1.button("Active Groups (CSV)", disabled=True, use_container_width=True, help="No 'Active' groups found to download.")

        # MODIFIED: CSV Outputs - Download for "All Processed Results" (which excludes 'Inactive' by default)
        # The DataFrame `df_for_adv_download_or_view` is used here.
        if not df_for_adv_download_or_view.empty:
            download_label_all_processed = "All Processed Results (Excl. Inactive)"
            if adv_filters_applied_flag: # If user applied filters on top of the "exclude Inactive" base
                download_label_all_processed = f"Filtered Processed Results (Excl. Inactive) ({len(df_for_adv_download_or_view)} rows)"
            else: # Default view for this CSV (base excluding Inactive, no further user filters)
                 download_label_all_processed = f"All Processed Results (Excl. Inactive) ({len(df_for_adv_download_or_view)} rows)"

            dl_col2.download_button(
                label=f"{download_label_all_processed} (CSV)",
                data=df_for_adv_download_or_view.to_csv(index=False).encode('utf-8'),
                file_name="all_processed_whatsapp_groups.csv", # Renamed filename for clarity
                mime="text/csv",
                use_container_width=True,
                key="dl_all_or_filtered_csv_key"
            )
        elif not df_base_for_adv_download.empty() and df_for_adv_download_or_view.empty() and adv_filters_applied_flag:
            # Base had data, but user filters resulted in empty
            dl_col2.button("No Results Match Advanced Filters (for Processed CSV)", disabled=True, use_container_width=True)
        else: # No data even in the base (df_base_for_adv_download was empty)
            dl_col2.button("All Processed Results (CSV)", disabled=True, use_container_width=True, help="No results (excluding Inactive) to download.")
            
    else: # No results in st.session_state.results
        st.info("Start by searching, entering, or uploading links to see results and download options!", icon="ℹ️")

if __name__ == "__main__":
    main()
