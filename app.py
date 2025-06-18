import streamlit as st
import pandas as pd
import requests
import html # Use html.unescape
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
            # st.warning("fake-useragent failed to get a User-Agent. Using fallback.", icon="⚠️") # Can be too noisy
            return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception: # Catch any other potential error during .random call
            # st.warning(f"Error getting random User-Agent: {e_random}. Using fallback.", icon="⚠️")
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
except Exception: # Catch error during UserAgent() initialization or other fake_useragent issues
    # st.warning(f"Error initializing fake-useragent: {e_init}. Using default User-Agent.", icon="⚠️")
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
UNNAMED_GROUP_PLACEHOLDER = "" # Groups without a name will have an empty string here
# Regex for WhatsApp Profile Pictures (PPS) - specific to v/t structure
IMAGE_PATTERN_PPS = re.compile(r'https://pps\.whatsapp\.net/v/t\d+/[-\w\.]+/\d+\.jpg\?.*') # slightly more permissive for characters in path
# Regex for general OpenGraph image URLs (more permissive)
OG_IMAGE_PATTERN = re.compile(r'https?://[^\/\s]+/.+\.(jpg|jpeg|png|gif|webp)(\?[^\s]*)?', re.IGNORECASE)
MAX_VALIDATION_WORKERS = 8
# GOOGLE_SEARCH_LIMIT is now controlled by the slider max_value

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
/* .st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F8F9FA; border-radius: 6px; } /* May need updates if Streamlit changes class names */
.stExpander { border: 1px solid #E9ECEF; border-radius: 8px; padding: 12px; margin-top: 15px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; font-size: 1.2em; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #1EBE5A; font-size: 1.1em; }
.filter-container { background-color: #FDFDFD; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px dashed #DDE2E5; }
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
        # Use io.BytesIO to handle the uploaded file content directly
        df = pd.read_excel(io.BytesIO(uploaded_file.getvalue()), engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        # Robustly get keywords from the first column, convert to string, strip whitespace
        keywords = [kw.strip() for kw in df.iloc[:, 0].dropna().astype(str).tolist() if len(kw.strip()) > 1] # Ensure keyword has some length
        if not keywords: st.warning("No valid keywords (length > 1) found in the first column of the Excel file.")
        return keywords
    except Exception as e:
        st.error(f"Error reading Excel: {e}. Ensure 'openpyxl' is installed if not already.", icon="❌")
        return []

def load_links_from_file(uploaded_file):
    if uploaded_file is None: return []
    try:
        content = uploaded_file.getvalue()
        text_content = None
        # Try common encodings
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                text_content = content.decode(encoding)
                st.sidebar.info(f"Successfully decoded file '{uploaded_file.name}' with {encoding}.")
                break
            except UnicodeDecodeError:
                continue
        
        if text_content is None:
            st.error(f"Could not decode file '{uploaded_file.name}' with common encodings. Please ensure it's UTF-8 or similar.", icon="❌")
            return []

        if uploaded_file.name.endswith('.csv'):
            try:
                # Use StringIO to treat the decoded string content as a file for pandas
                df = pd.read_csv(io.StringIO(text_content))
                if df.empty: st.warning("CSV file is empty."); return []
                # Assuming links are in the first column, ensure they are strings and look like URLs
                return [link.strip() for link in df.iloc[:, 0].dropna().astype(str).tolist() if link.strip().startswith(('http://', 'https://'))]
            except Exception as e:
                st.error(f"Error reading CSV content from '{uploaded_file.name}': {e}.", icon="❌")
                return []
        else: # Assume TXT file
            return [line.strip() for line in text_content.splitlines() if line.strip() and line.strip().startswith(('http://', 'https://'))]
    except Exception as e:
        st.error(f"Error processing file '{uploaded_file.name}': {e}", icon="❌")
        return []

# --- Core Logic Functions ---
def validate_link(link):
    # MODIFIED: Validation Logic based on user's older version principles + new requirements
    result = {"Group Name": UNNAMED_GROUP_PLACEHOLDER, "Group Link": link, "Logo URL": "", "Status": "Inactive"}

    try:
        response = requests.get(link, headers=get_random_headers_general(), timeout=20, allow_redirects=True)
        response.encoding = 'utf-8' # Ensure UTF-8 encoding

        if response.status_code != 200:
            result["Status"] = "Expired (404 Not Found)" if response.status_code == 404 else f"HTTP Error {response.status_code}"
            return result
        
        # Check final URL after potential redirects
        if WHATSAPP_DOMAIN not in response.url:
            final_netloc = urlparse(response.url).netloc or 'Unknown Site'
            result["Status"] = f"Redirected Away ({final_netloc})"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # --- 1. Extract Group Name ---
        group_name_str = ""
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            name_from_meta = html.unescape(meta_title['content']).strip()
            if name_from_meta: # Ensure it's not an empty string
                group_name_str = name_from_meta
        
        if not group_name_str: # Fallback if OG:Title not found or empty
            # Try common elements (more specific class search and text filtering)
            potential_name_tags = soup.find_all(
                ['h2', 'h3', 'strong', 'span', 'div'], 
                class_=re.compile(r'\b(group-name|name|_2v6EX|_1VzZY)\b', re.IGNORECASE) # Added some observed WhatsApp web classes
            ) + soup.find_all('div', attrs={'data-testid': 'group-name'}) # For newer data-testid attributes

            for tag in potential_name_tags:
                text = tag.get_text(separator=' ', strip=True)
                # Filter out common placeholder/generic texts more robustly
                if text and len(text) > 2 and len(text) < 100 and \
                   text.lower() not in ["whatsapp group invite", "whatsapp", "join group", 
                                         "invite link", "open chat", "open this link", "group invite"]:
                    group_name_str = text
                    break
        result["Group Name"] = group_name_str

        # --- 2. Extract Logo URL ---
        logo_url_str = ""
        # Prioritize specific WhatsApp PPS images (spirit of older version)
        for img in soup.find_all('img', src=True):
            src = html.unescape(img['src'])
            if src.startswith('https://pps.whatsapp.net/') and IMAGE_PATTERN_PPS.match(src):
                logo_url_str = src
                break
        
        if not logo_url_str: # Fallback to OG:Image if no PPS match
            meta_image = soup.find('meta', property='og:image')
            if meta_image and meta_image.get('content'):
                src = html.unescape(meta_image['content'])
                if OG_IMAGE_PATTERN.match(src): # Check if it looks like a valid image URL
                    logo_url_str = src
        result["Logo URL"] = logo_url_str

        # --- 3. Determine Status (incorporating older logic principles) ---
        page_text_lower = soup.get_text().lower()
        expired_phrases = [
            "invite link is invalid", "invite link was reset", 
            "group doesn't exist", "this group is no longer available",
            "couldn't join this group because the invite link is no longer active",
            "you can't join this group because it's full" # Also a form of "not joinable"
        ]
        is_explicitly_expired_or_full = any(phrase in page_text_lower for phrase in expired_phrases)

        # Rule 1: No valid name found -> Inactive
        if not group_name_str:
            result["Status"] = "Inactive"
        # Rule 2: Explicitly expired or full text found -> Expired
        elif is_explicitly_expired_or_full:
            result["Status"] = "Expired"
            if "group because it's full" in page_text_lower: # More specific status
                 result["Status"] = "Full"
        # Rule 3: Has name, not explicitly expired/full
        elif logo_url_str: # AND has logo -> Active (mimicking older version's core)
            result["Status"] = "Active"
        else: # Has name, not explicitly expired/full, BUT NO logo -> Expired (mimicking older version)
            result["Status"] = "Expired"

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
        response.encoding = 'utf-8' # Ensure correct encoding
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find in <a> tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                parsed_url = urlparse(href)
                # Keep only scheme, netloc, path; remove query params and fragments for uniqueness
                clean_path = parsed_url.path.split(';')[0].split('?')[0].split('&')[0] # Clean common trackers/params
                links.add(f"{parsed_url.scheme}://{parsed_url.netloc}{clean_path}")

        # Find in raw text content (more robust regex)
        text_content = soup.get_text()
        if WHATSAPP_DOMAIN in text_content: # Quick check
            # Regex to find WhatsApp chat URLs, ensuring they have a path component (invite code)
            # Invite codes are typically 22 alphanumeric characters (old format) or longer mixed-case (newer)
            raw_found_links = re.findall(r'(https?://chat\.whatsapp\.com/([A-Za-z0-9_-]{16,}))', text_content)
            for match in raw_found_links:
                link_url = match[0] # The full URL matched
                parsed_url = urlparse(link_url)
                clean_path = parsed_url.path.split(';')[0].split('?')[0].split('&')[0]
                # Further check on path length/structure if needed
                if len(clean_path.replace('/', '')) >= 16: # Typical invite codes are longer
                    links.add(f"{parsed_url.scheme}://{parsed_url.netloc}{clean_path}")
    
    except requests.exceptions.Timeout: st.sidebar.warning(f"Scrape Timeout: {url[:50]}...", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Scrape HTTP Err {e.response.status_code}: {url[:50]}...", icon="⚠️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape Net Err ({type(e).__name__}): {url[:50]}...", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Scrape Parse Err ({type(e).__name__}): {url[:50]}...", icon="💣")
    return list(links)


def google_search_and_scrape(query, top_n=5):
    st.info(f"Googling '{query}' (top {top_n} results)...")
    all_scraped_wa_links = set()
    try:
        search_page_urls = list(google_search_function_actual(query, num_results=top_n, lang="en", sleep_interval=2)) # Added sleep_interval
        
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
                    # Basic validation that it's a WhatsApp domain link before adding
                    if link.startswith(WHATSAPP_DOMAIN) and link not in all_scraped_wa_links:
                        all_scraped_wa_links.add(link)
                        newly_found_count += 1
                if newly_found_count > 0:
                    st.sidebar.info(f"Found {newly_found_count} new WA links on {url_from_google[:30]}...")
                prog_bar.progress((i+1)/len(search_page_urls))
                time.sleep(0.1) # Small delay between page scrapes
        
        stat_txt.success(f"Scraping of Google results complete. Found {len(all_scraped_wa_links)} unique WhatsApp links from '{query}'.")
        return list(all_scraped_wa_links)

    except TypeError as e: 
        st.error(f"Google search TypeError: {e}. Check `googlesearch-python` version/parameters, or for Google blocking.", icon="❌")
        return []
    except Exception as e: 
        st.error(f"Unexpected Google search/scrape error for '{query}': {e}. Check connection/library, or for Google blocking.", icon="❌")
        return []

def crawl_website(start_url, max_depth=2, max_pages=50):
    scraped_whatsapp_links = set()
    if not start_url.strip(): return scraped_whatsapp_links

    if not start_url.startswith(('http://', 'https://')):
        start_url = 'https://' + start_url
        st.sidebar.warning(f"Prepending 'https://' to: {start_url}", icon="🔗")

    parsed_start_url = urlparse(start_url)
    if not parsed_start_url.netloc: # Basic validation
        st.sidebar.error(f"Invalid start URL: {start_url}. Please include a domain name.", icon="🚫")
        return scraped_whatsapp_links
    
    base_domain = parsed_start_url.netloc.replace('www.', '') # Normalize domain

    # Store (url_to_visit, current_depth)
    queue_list = [(start_url, 0)] 
    # Store visited URLs (normalized: scheme + netloc + path, no query/fragment) to avoid re-processing same page content
    visited_normalized_urls = {urljoin(start_url, parsed_start_url.path or '/')}
    # Store full URLs added to queue to avoid adding exact same URL multiple times if linked from different places
    urls_in_queue_full = {start_url}


    page_count = 0
    max_q_size = max_pages * 100 # Heuristic to prevent excessively large queues

    with requests.Session() as session, st.spinner(f"Crawling {base_domain}... (Max Depth: {max_depth}, Max Pages: {max_pages})"):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size: # Safety break for queue size
                st.sidebar.warning(f"Crawl queue exceeded {max_q_size}. Stopping early to manage resources.", icon="❗️")
                queue_list = queue_list[:max_q_size] # Trim queue
            
            current_url, depth = queue_list.pop(0)
            
            # Already checked visited_normalized_urls before adding to queue, but double check for safety
            normalized_current_url_for_check = urljoin(current_url, urlparse(current_url).path or '/')
            if normalized_current_url_for_check in visited_normalized_urls and current_url != start_url : # Allow start_url to be processed once
                 continue # Already processed content of this path

            st.sidebar.text(f"Crawl (D:{depth},P:{page_count+1}/{max_pages},Q:{len(queue_list)}): {current_url[:50]}...")
            
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10)
                response.raise_for_status() # Check for HTTP errors

                if 'text/html' not in response.headers.get('Content-Type', '').lower():
                    continue # Skip non-HTML content
                
                page_count += 1 
                visited_normalized_urls.add(normalized_current_url_for_check) # Mark content as processed

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

                            # Stay within the same domain, valid scheme, ignore fragments for queue uniqueness
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain:
                                
                                # Normalize URL for visited check (path part only)
                                normalized_abs_url_for_visited = urljoin(abs_url, parsed_abs_url.path or '/')
                                
                                if normalized_abs_url_for_visited not in visited_normalized_urls and \
                                   abs_url not in urls_in_queue_full: # Check if this exact full URL is new for queue
                                    if len(queue_list) < max_q_size: # Add if queue not full
                                        queue_list.append((abs_url, depth + 1))
                                        urls_in_queue_full.add(abs_url)
                                    else:
                                        st.sidebar.warning("Crawl queue full, some potential pages skipped.", icon="🟡")
                                        break # Stop adding new links from this page if queue is full
                    if len(queue_list) >= max_q_size: break # Break outer loop if queue still full after processing a page's links
            
            except requests.exceptions.RequestException as e:
                st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: 
                st.sidebar.error(f"Crawl Gen Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
            time.sleep(0.2) # Politeness delay between requests during crawl
    
    st.sidebar.success(f"Crawl complete. Scraped {page_count} pages, found {len(scraped_whatsapp_links)} unique WhatsApp links.")
    if page_count >= max_pages: st.sidebar.warning(f"Reached max pages limit ({max_pages}). Crawl may be incomplete.", icon="❗️")
    return scraped_whatsapp_links

def generate_styled_html_table(data_df_for_table):
    # Input `data_df_for_table` is already filtered for 'Active' status.
    # UNNAMED_GROUP_PLACEHOLDER is "", so groups with no name are naturally handled if 'Active' status was assigned
    # However, 'Active' status now requires a group name.
    
    if data_df_for_table.empty: # This df should only contain 'Active' groups
        return "<p style='text-align:center; color:#777; margin-top:20px;'><i>No 'Active' groups match the current display filters. Try adjusting them or broadening your search.</i></p>"

    html_string = '<table class="whatsapp-groups-table" aria-label="List of Active WhatsApp Groups">'
    html_string += '<caption>Filtered Active WhatsApp Groups</caption>'
    html_string += '<thead><tr>'
    html_string += '<th scope="col">Logo</th>'
    html_string += '<th scope="col">Group Name</th>'
    html_string += '<th scope="col">Group Link</th>'
    html_string += '</tr></thead>'
    html_string += '<tbody>'
    for _, row in data_df_for_table.iterrows():
        logo_url = row.get("Logo URL", "")
        # Group name should exist if status is 'Active' based on new validate_link logic
        group_name = row.get("Group Name", "Error: Name Missing for Active Group") 
        group_link = row.get("Group Link", "")
        
        html_string += '<tr>'
        html_string += '<td class="group-logo-cell">'
        alt_text = f"{html.escape(group_name)} Group Logo" if group_name else "Group Logo"
        if logo_url:
            display_logo_url = append_query_param(logo_url, 'w', '96') if logo_url.startswith('https://pps.whatsapp.net/') else logo_url
            # Added onerror to hide broken image and show placeholder div instead
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img" loading="lazy" onerror="this.style.display=\'none\'; this.nextElementSibling.style.display=\'flex\';">'
            html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:none; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        else: # Should not happen for 'Active' groups if logo is required
            html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">? (No Logo)</div>'
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
    
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()
    if 'styled_table_name_keywords' not in st.session_state: st.session_state.styled_table_name_keywords = ""
    if 'styled_table_current_limit_value' not in st.session_state: st.session_state.styled_table_current_limit_value = 50
    if 'adv_filter_status' not in st.session_state: st.session_state.adv_filter_status = [] # Default for advanced filter status
    if 'adv_filter_name_keywords' not in st.session_state: st.session_state.adv_filter_name_keywords = ""
    if 'adv_filter_status_default_applied' not in st.session_state: st.session_state.adv_filter_status_default_applied = False


    if not isinstance(st.session_state.processed_links_in_session, set):
        st.session_state.processed_links_in_session = set()
    # Populate processed_links_in_session from existing results if any (e.g., on rerun)
    # This helps avoid re-validating links already processed in the current session's results list
    temp_processed_links = set()
    if isinstance(st.session_state.results, list):
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and res_item['Group Link']:
                try:
                    parsed_link = urlparse(res_item['Group Link'])
                    normalized_link = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
                    temp_processed_links.add(normalized_link)
                except Exception: # Fallback if parsing fails
                    temp_processed_links.add(res_item['Group Link'])
    st.session_state.processed_links_in_session.update(temp_processed_links)


    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV/Excel)"
        ], key="input_method_main_select", index=0)

        gs_top_n = 5 
        if input_method in ["Search and Scrape from Google", 
                            "Search & Scrape from Google (Bulk via Excel)", 
                            "Upload Link File (TXT/CSV/Excel)"]: # If Excel is used for keywords
            # MODIFIED: Google Search Limit increased to 100
            gs_top_n = st.slider(
                "Google Results to Scrape (per keyword)", 
                min_value=1, max_value=100, value=5, key="gs_top_n_slider",
                help="Number of Google search result pages to analyze per keyword. Higher values (>20) may be slow or lead to Google blocks."
            )
            # MODIFIED: Google Search Limit - Added warning
            if gs_top_n > 20:
                st.warning(f"Searching for {gs_top_n} results per keyword may be very slow and significantly increase the risk of temporary Google blocks. Recommended: <= 20.", icon="⚠️")
        
        crawl_depth, crawl_pages = 2, 50 
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be very slow and resource-intensive. Use with caution on large websites.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider", help="0 for current page only. Higher values increase time.")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 300, 50, key="crawl_pages_slider", help="Limits total pages crawled to manage time.")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.session_state.styled_table_name_keywords = ""
            st.session_state.styled_table_current_limit_value = 50
            st.session_state.adv_filter_status = [] # Reset advanced filter
            st.session_state.adv_filter_name_keywords = ""
            st.session_state.adv_filter_status_default_applied = False # Reset default filter flag
            # st.cache_data.clear() # If using st.cache_data elsewhere
            st.success("Results, processed link history, and all filters cleared successfully!")
            st.rerun()

    current_action_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input")
            if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button"):
                if query: current_action_scraped_links.update(google_search_and_scrape(query, gs_top_n))
                else: st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            file_gs_bulk = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if file_gs_bulk and st.button("Process Excel, Scrape & Validate", use_container_width=True, key="gs_bulk_button"):
                keywords_gs_bulk = load_keywords_from_excel(file_gs_bulk)
                if keywords_gs_bulk:
                    st.info(f"Processing {len(keywords_gs_bulk)} keywords from Excel...")
                    prog_b_gs, stat_b_gs = st.progress(0), st.empty()
                    total_l_gs_bulk = 0
                    for i, kw_gs in enumerate(keywords_gs_bulk):
                        stat_b_gs.text(f"Keyword: '{kw_gs}' ({i+1}/{len(keywords_gs_bulk)}). Total links found so far: {total_l_gs_bulk}")
                        links_from_kw_gs = google_search_and_scrape(kw_gs, gs_top_n)
                        current_action_scraped_links.update(links_from_kw_gs)
                        total_l_gs_bulk = len(current_action_scraped_links) 
                        prog_b_gs.progress((i+1)/len(keywords_gs_bulk))
                    stat_b_gs.success(f"Bulk Excel processing complete. Found {total_l_gs_bulk} unique WhatsApp links across all keywords.")
                else: st.warning("No valid keywords found in the uploaded Excel file.")

        elif input_method == "Scrape from Specific Webpage URL":
            url_specific = st.text_input("Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if url_specific and (url_specific.startswith("http://") or url_specific.startswith("https://")):
                    with st.spinner(f"Scraping {url_specific}..."):
                        current_action_scraped_links.update(scrape_whatsapp_links_from_page(url_specific))
                    st.success(f"Scraping of {url_specific} complete. Found {len(current_action_scraped_links)} unique WhatsApp links on this page.")
                else: st.warning("Please enter a valid URL (starting with http:// or https://).")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_crawl = st.text_input("Base Domain URL:", placeholder="example.com (e.g., mysite.com or newsportal.org)", key="crawl_domain_input")
            if st.button("Crawl & Scrape Website", use_container_width=True, key="crawl_button"):
                if domain_crawl:
                    st.info(f"Starting website crawl for '{domain_crawl}'. Progress will be shown in the sidebar.")
                    current_action_scraped_links.update(crawl_website(domain_crawl, crawl_depth, crawl_pages))
                    st.success(f"Website crawl for '{domain_crawl}' complete. Found {len(current_action_scraped_links)} unique WhatsApp links.")
                else: st.warning("Please enter a base domain URL to crawl.")

        elif input_method == "Enter Links Manually (for Validation)":
            text_manual = st.text_area("WhatsApp Links (one per line):", height=200, key="manual_links_area", placeholder=f"e.g., {WHATSAPP_DOMAIN}xxxxxxxxxxxxxx\n{WHATSAPP_DOMAIN}yyyyyyyyyyyyyy")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                links_manual = [line.strip() for line in text_manual.split('\n') if line.strip()]
                if links_manual:
                    valid_links_manual = {l for l in links_manual if l.startswith(WHATSAPP_DOMAIN)}
                    invalid_format_count_manual = len(links_manual) - len(valid_links_manual)
                    if invalid_format_count_manual > 0:
                        st.warning(f"Skipped {invalid_format_count_manual} entries that do not look like valid WhatsApp group links.")
                    if valid_links_manual:
                        current_action_scraped_links.update(valid_links_manual)
                        st.info(f"{len(valid_links_manual)} WhatsApp links queued for validation.")
                    else:
                        st.warning("No valid WhatsApp group links found in the input.")
                else: st.warning("Please enter some WhatsApp links to validate.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file_upload = st.file_uploader("Upload TXT/CSV (links) or Excel (keywords)", type=["txt", "csv", "xlsx"], key="upload_file_input")
            if file_upload and st.button("Process File & Validate/Scrape", use_container_width=True, key="upload_process_button"):
                if file_upload.name.endswith('.xlsx'):
                    st.info("Processing Excel file for keywords to search on Google...")
                    keywords_excel_upload = load_keywords_from_excel(file_upload)
                    if keywords_excel_upload:
                        prog_e_upload, stat_e_upload = st.progress(0), st.empty()
                        total_le_upload = 0
                        for i, kw_upload in enumerate(keywords_excel_upload):
                            stat_e_upload.text(f"Keyword: {kw_upload} ({i+1}/{len(keywords_excel_upload)}). Links found so far: {total_le_upload}")
                            links_from_kw_upload = google_search_and_scrape(kw_upload, gs_top_n)
                            current_action_scraped_links.update(links_from_kw_upload)
                            total_le_upload = len(current_action_scraped_links)
                            prog_e_upload.progress((i+1)/len(keywords_excel_upload))
                        stat_e_upload.success(f"Excel (keywords) processing complete. Found {total_le_upload} unique WhatsApp links.")
                    else: st.warning("No valid keywords found in the Excel file.")
                elif file_upload.name.endswith(('.txt', '.csv')):
                    st.info("Processing TXT/CSV file for direct WhatsApp links...")
                    links_file_upload = load_links_from_file(file_upload)
                    if links_file_upload:
                        valid_links_file_upload = {l for l in links_file_upload if l.startswith(WHATSAPP_DOMAIN)}
                        invalid_format_count_file = len(links_file_upload) - len(valid_links_file_upload)
                        if invalid_format_count_file > 0:
                            st.warning(f"Skipped {invalid_format_count_file} entries from the file that do not look like valid WhatsApp group links.")
                        if valid_links_file_upload:
                            current_action_scraped_links.update(valid_links_file_upload)
                            st.info(f"{len(valid_links_file_upload)} WhatsApp links from file queued for validation.")
                        else:
                            st.warning("No valid WhatsApp group links found in the uploaded file.")
                    else: st.warning("No links found or able to be processed from the uploaded file.")
                else: st.warning("Unsupported file type. Please upload a .txt, .csv, or .xlsx file.")
    except Exception as e: st.error(f"An error occurred during the input/scraping phase: {e}", icon="💥")

    links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)

    if links_to_validate_now:
        st.success(f"Total {len(current_action_scraped_links)} unique links gathered. Validating {len(links_to_validate_now)} new/unprocessed links...")
        prog_val, stat_val = st.progress(0), st.empty()
        new_results_this_run = []
        
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            future_to_link_map = {executor.submit(validate_link, link): link for link in links_to_validate_now}
            for i, future_item in enumerate(as_completed(future_to_link_map)):
                link_validated_item = future_to_link_map[future_item]
                try:
                    result_validated_item = future_item.result()
                    new_results_this_run.append(result_validated_item)
                    parsed_url_val_item = urlparse(link_validated_item)
                    normalized_link_val_item = f"{parsed_url_val_item.scheme}://{parsed_url_val_item.netloc}{parsed_url_val_item.path}"
                    st.session_state.processed_links_in_session.add(normalized_link_val_item)
                except Exception as val_exc_item: 
                    st.warning(f"Critical error validating {link_validated_item[:40]}...: {val_exc_item}", icon="⚠️")
                    parsed_url_val_err_item = urlparse(link_validated_item) # type: ignore
                    normalized_link_val_err_item = f"{parsed_url_val_err_item.scheme}://{parsed_url_val_err_item.netloc}{parsed_url_val_err_item.path}" # type: ignore
                    st.session_state.processed_links_in_session.add(normalized_link_val_err_item) # type: ignore
                    new_results_this_run.append({"Group Name": "Validation Error", "Group Link": link_validated_item, "Logo URL": "", "Status": f"CritValidationFail: {type(val_exc_item).__name__}"}) # type: ignore
                
                prog_val.progress((i+1)/len(links_to_validate_now))
                status_update_text = f"Validated {i+1}/{len(links_to_validate_now)} links. Last: ...{link_validated_item[-25:]}, Status: {new_results_this_run[-1]['Status']}"
                stat_val.text(status_update_text)
        
        if new_results_this_run:
            st.session_state.results.extend(new_results_this_run)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
    elif current_action_scraped_links and not links_to_validate_now:
        st.info("All WhatsApp links found in this action were already processed in this session. No new links to validate.")

    if 'results' in st.session_state and st.session_state.results:
        unique_results_df_master = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = unique_results_df_master.to_dict('records') 
        
        df_display_master_final = unique_results_df_master.reset_index(drop=True)

        active_df_master = df_display_master_final[df_display_master_final['Status'] == 'Active'].copy()
        expired_df_master = df_display_master_final[df_display_master_final['Status'].isin(['Expired', 'Full'])].copy() # Include 'Full' as a type of Expired
        inactive_df_master = df_display_master_final[df_display_master_final['Status'] == 'Inactive'].copy()
        error_df_master = df_display_master_final[
            ~df_display_master_final['Status'].isin(['Active', 'Inactive', 'Expired', 'Full'])
        ].copy()

        st.subheader("📊 Results Summary")
        summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
        summary_col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master_final)}</div></div>', unsafe_allow_html=True)
        summary_col2.markdown(f'<div class="metric-card">Active Groups<br><div class="metric-value">{len(active_df_master)}</div></div>', unsafe_allow_html=True)
        summary_col3.markdown(f'<div class="metric-card">Inactive (Nameless)<br><div class="metric-value">{len(inactive_df_master)}</div></div>', unsafe_allow_html=True)
        summary_col4.markdown(f'<div class="metric-card">Expired/Error/Other<br><div class="metric-value">{len(expired_df_master) + len(error_df_master)}</div></div>', unsafe_allow_html=True)


        st.subheader("✨ Active Groups Display (Styled Table)")
        with st.expander("View and Filter Active Groups", expanded=True):
            if not active_df_master.empty:
                st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                st.markdown("#### Filter Displayed Active Groups:")

                with st.form("styled_table_filters_form_active"):
                    name_kw_styled = st.text_input(
                        "Filter by Group Name Keywords (comma-separated):",
                        value=st.session_state.styled_table_name_keywords,
                        placeholder="e.g., study, fun, tech",
                        help="Shows active groups matching ANY keyword in their name."
                    ).strip()
                    limit_styled = st.number_input(
                        "Max Active Groups to Display:",
                        min_value=1, max_value=max(1, len(active_df_master)), 
                        value=st.session_state.styled_table_current_limit_value,
                        step=10,
                        help="Set max active groups for styled table display."
                    )
                    apply_styled_filters_btn = st.form_submit_button("Apply Filters to Styled Table")

                if apply_styled_filters_btn:
                    st.session_state.styled_table_name_keywords = name_kw_styled
                    st.session_state.styled_table_current_limit_value = limit_styled
                    st.rerun() 

                if st.button("Reset Styled Table Filters", key="reset_styled_table_filters_btn"):
                    st.session_state.styled_table_name_keywords = ""
                    st.session_state.styled_table_current_limit_value = 50
                    st.rerun()

                active_df_for_styled = active_df_master.copy()
                if st.session_state.styled_table_name_keywords:
                    kw_list_styled = [kw.strip().lower() for kw in st.session_state.styled_table_name_keywords.split(',') if kw.strip()]
                    if kw_list_styled:
                        active_df_for_styled['Group Name'] = active_df_for_styled['Group Name'].astype(str) # Ensure string
                        regex_pat_styled = '|'.join(map(re.escape, kw_list_styled))
                        active_df_for_styled = active_df_for_styled[
                            active_df_for_styled['Group Name'].str.lower().str.contains(regex_pat_styled, na=False, regex=True)
                        ]
                
                num_match_styled = len(active_df_for_styled)
                num_disp_styled = min(num_match_styled, st.session_state.styled_table_current_limit_value)
                active_df_styled_final = active_df_for_styled.head(num_disp_styled)

                if num_match_styled > 0:
                    st.write(f"Showing {num_disp_styled} of {num_match_styled} matching active groups.")
                else:
                    st.write("No active groups match the current filters for the styled table.")

                html_table_output = generate_styled_html_table(active_df_styled_final)
                st.markdown(html_table_output, unsafe_allow_html=True)
                st.markdown("---")
                st.text_area("Copy Raw HTML Code (for above table):", value=html_table_output, height=150, key="styled_html_export_key", help="Ctrl+A, Ctrl+C")
                st.markdown('</div>', unsafe_allow_html=True) 
            else:
                st.info("No 'Active' groups found yet to display here. Try scraping/validating more links.")

        with st.expander("🔬 Advanced Filtering for Downloads & Full Dataset View", expanded=False):
            st.markdown('<div class="filter-container" style="border-style:solid;">', unsafe_allow_html=True)
            st.markdown("#### Filter Full Dataset (for Download/Analysis):")
            
            # MODIFIED: CSV Outputs - Base for this download excludes "Inactive"
            df_base_adv_dl = df_display_master_final[df_display_master_final['Status'] != 'Inactive'].copy()
            
            all_statuses_adv = sorted(list(df_base_adv_dl['Status'].unique()))

            # MODIFIED: Default Filter - Apply "Active" as default if not manually set and "Active" is an option
            if not st.session_state.adv_filter_status_default_applied and \
               not st.session_state.adv_filter_status and \
               "Active" in all_statuses_adv:
                st.session_state.adv_filter_status = ["Active"]
                st.session_state.adv_filter_status_default_applied = True 

            adv_status_sel = st.multiselect(
                "Filter by Status (for 'All Processed Results' CSV):", options=all_statuses_adv,
                default=st.session_state.adv_filter_status,
                key="adv_status_filter_ms_key",
                help="Select statuses. 'Inactive' groups are already excluded from this output."
            )
            if adv_status_sel != st.session_state.adv_filter_status: # User interaction
                st.session_state.adv_filter_status = adv_status_sel
                st.session_state.adv_filter_status_default_applied = True # Mark as interacted
            
            adv_name_kw_sel = st.text_input(
                "Filter by Group Name Keywords (for 'All Processed Results' CSV, comma-separated):", 
                value=st.session_state.adv_filter_name_keywords,
                key="adv_name_kw_filter_input_key_dl", placeholder="e.g., news, jobs, global",
                help="Applies to 'All Processed Results' CSV. Comma-separated."
            ).strip()
            if adv_name_kw_sel != st.session_state.adv_filter_name_keywords: # User interaction
                 st.session_state.adv_filter_name_keywords = adv_name_kw_sel

            st.markdown('</div>', unsafe_allow_html=True)

            df_for_adv_dl_view = df_base_adv_dl.copy()
            adv_filters_applied_dl_flag = False

            if st.session_state.adv_filter_status:
                df_for_adv_dl_view = df_for_adv_dl_view[df_for_adv_dl_view['Status'].isin(st.session_state.adv_filter_status)]
                adv_filters_applied_dl_flag = True
            
            if st.session_state.adv_filter_name_keywords:
                adv_kw_list_dl = [kw.strip().lower() for kw in st.session_state.adv_filter_name_keywords.split(',') if kw.strip()]
                if adv_kw_list_dl:
                    df_for_adv_dl_view['Group Name'] = df_for_adv_dl_view['Group Name'].astype(str)
                    adv_regex_pat_dl = '|'.join(map(re.escape, adv_kw_list_dl))
                    df_for_adv_dl_view = df_for_adv_dl_view[
                        df_for_adv_dl_view['Group Name'].str.lower().str.contains(adv_regex_pat_dl, na=False, regex=True)
                    ]
                    adv_filters_applied_dl_flag = True
            
            preview_lbl_dl = "All (Excluding Inactive)"
            if adv_filters_applied_dl_flag: preview_lbl_dl = "Filtered (Excluding Inactive)"
            
            st.markdown(f"**Preview of Data for 'All Processed Results' CSV ({preview_lbl_dl} - {len(df_for_adv_dl_view)} rows):**")
            st.dataframe(df_for_adv_dl_view, column_config={
                "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Link", width="medium"),
                "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View Logo", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small")
            }, hide_index=True, height=300, use_container_width=True)

        st.subheader("📥 Download Results (CSV)")
        dl_col_main1, dl_col_main2 = st.columns(2)
        
        if not active_df_master.empty:
            dl_col_main1.download_button(
                label=f"Active Groups ({len(active_df_master)} rows) (CSV)", 
                data=active_df_master.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'), # utf-8-sig for Excel
                file_name="active_whatsapp_groups.csv", 
                mime="text/csv", 
                use_container_width=True, 
                key="dl_active_csv_btn_key"
            )
        else:
            dl_col_main1.button("Active Groups (CSV)", disabled=True, use_container_width=True, help="No 'Active' groups found.")

        # MODIFIED: CSV Outputs filename and label logic
        if not df_for_adv_dl_view.empty:
            dl_lbl_all_proc = "All Processed Results (Excl. Inactive)"
            if adv_filters_applied_dl_flag: 
                dl_lbl_all_proc = f"Filtered Processed Results (Excl. Inactive) ({len(df_for_adv_dl_view)} rows)"
            else:
                 dl_lbl_all_proc = f"All Processed Results (Excl. Inactive) ({len(df_for_adv_dl_view)} rows)"

            dl_col_main2.download_button(
                label=f"{dl_lbl_all_proc} (CSV)",
                data=df_for_adv_dl_view.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig'),
                file_name="processed_whatsapp_groups.csv", # Consistent filename
                mime="text/csv",
                use_container_width=True,
                key="dl_all_filt_csv_btn_key"
            )
        elif not df_base_adv_dl.empty() and df_for_adv_dl_view.empty() and adv_filters_applied_dl_flag:
            dl_col_main2.button("No Results Match Adv. Filters (Processed CSV)", disabled=True, use_container_width=True)
        else: 
            dl_col_main2.button("All Processed Results (CSV)", disabled=True, use_container_width=True, help="No results (excl. Inactive) to download.")
            
    else: 
        st.info("Start by searching, entering, or uploading links to see results and download options!", icon="ℹ️")

if __name__ == "__main__":
    main()
