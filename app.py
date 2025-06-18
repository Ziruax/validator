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
            st.warning("fake-useragent failed to get a User-Agent. Using fallback.", icon="⚠️")
            return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception as e_random: # Catch other potential errors during .random call
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
    st.warning(f"Error initializing fake-useragent (FakeUserAgentError): {e_init}. Using default User-Agent.", icon="⚠️")
    def get_random_headers_general():
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except Exception as e_general_init: # Catch any other error during UserAgent() initialization
    st.warning(f"Error initializing fake-useragent (General Exception): {e_general_init}. Using default User-Agent.", icon="⚠️")
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
UNNAMED_GROUP_PLACEHOLDER = "Unnamed Group" # May still be used for display logic if a name is truly empty string
IMAGE_PATTERN_PPS = re.compile(r'https://pps.whatsapp.net/v/t\d+/[-\w]+/\d+.jpg?')
OG_IMAGE_PATTERN = re.compile(r'https?://[^\/\s]+/[^\/\s]+.(jpg|jpeg|png|gif)(?[^\s]*)?') # Added gif
MAX_VALIDATION_WORKERS = 8
GOOGLE_SEARCH_MAX_RESULTS_SOFT_CAP = 20 # For warning

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
.st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F8F9FA; border-radius: 6px; } /* May need to check Streamlit versions for these class names */
.stExpander { border: 1px solid #E9ECEF; border-radius: 8px; padding: 12px; margin-top: 15px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; font-size: 1.2em; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #1EBE5A; font-size: 1.1em; } /* Might need adjustment if label isn't strong */
.filter-container { background-color: #FDFDFD; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px dashed #DDE2E5; }
/* .filter-container .stTextInput input, .filter-container .stNumberInput input { background-color: #fff;zczy} /* zczy seems like a typo, removed */
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
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        query_params[param_name] = [param_value]
        new_query_string = urlencode(query_params, doseq=True)
        url_without_fragment = parsed_url._replace(query=new_query_string, fragment='').geturl()
        return f"{url_without_fragment}#{parsed_url.fragment}" if parsed_url.fragment else url_without_fragment
    except Exception:
        return url # Fallback to original URL on parsing error

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
        for encoding in ['utf-8', 'latin-1', 'cp1252']: # Common encodings
            try:
                text_content = content.decode(encoding)
                st.sidebar.info(f"Decoded file with {encoding}.")
                break
            except UnicodeDecodeError:
                continue
        
        if text_content is None:
            st.error(f"Could not decode file {uploaded_file.name}. Try saving as UTF-8.", icon="❌")
            return []

        if uploaded_file.name.endswith('.csv'):
            try:
                # Use StringIO to handle the decoded text content as a file for pandas
                df = pd.read_csv(io.StringIO(text_content))
                if df.empty: st.warning("CSV file is empty."); return []
                # Assuming links are in the first column
                return [link.strip() for link in df.iloc[:, 0].dropna().astype(str).tolist() if link.strip().startswith(('http://', 'https://'))]
            except Exception as e:
                st.error(f"Error reading CSV: {e}.", icon="❌")
                return []
        else: # Assume TXT
            return [line.strip() for line in text_content.splitlines() if line.strip()]
    except Exception as e:
        st.error(f"Error processing file {uploaded_file.name}: {e}", icon="❌")
        return []

# --- Core Logic Functions ---
def validate_link(link):
    result = {"Group Name": "", "Group Link": link, "Logo URL": "", "Status": "Inactive"}

    try:
        response = requests.get(link, headers=get_random_headers_general(), timeout=20, allow_redirects=True)
        response.encoding = 'utf-8' # Ensure correct encoding

        if response.status_code != 200:
            result["Status"] = "Expired (404 Not Found)" if response.status_code == 404 else f"HTTP Error {response.status_code}"
            return result

        # Check if the final URL is still on WhatsApp domain
        if WHATSAPP_DOMAIN not in response.url:
            final_netloc = urlparse(response.url).netloc or 'Unknown Site'
            result["Status"] = f"Redirected Away ({final_netloc})"
            return result

        # --- Process page content if 200 OK and on WhatsApp domain ---
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check for common expired/invalid link phrases in page text
        page_text_lower = soup.get_text().lower()
        expired_phrases = [
            "invite link is invalid", "invite link was reset", "group doesn't exist", 
            "this group is no longer available", "n\u00e3o existe", "link de convite inv\u00e1lido",
            "this invite link was reset", "you can't join this group because this invite link is no longer active"
        ]
        if any(phrase in page_text_lower for phrase in expired_phrases):
            result["Status"] = "Expired"
            # Try to get name/logo even if expired, for completeness, but status remains Expired.
        
        # Extract Group Name
        group_name_found = False
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name_content = html.unescape(meta_title['content']).strip()
            if group_name_content: # Ensure it's not an empty string
                result["Group Name"] = group_name_content
                group_name_found = True
        
        if not group_name_found:
            # Try other common tags or patterns if og:title fails or is generic
            # Example: Look for h2, strong tags, or specific class names (these are illustrative)
            potential_name_elements = soup.select('h2, h3, strong, span[class*="name"], div[class*="name"]') # Simplified selector
            for element in potential_name_elements:
                text = element.get_text(strip=True)
                # Avoid generic phrases that might be caught
                if text and len(text) > 2 and text.lower() not in ["whatsapp group invite", "whatsapp", "join group", "invite link"]:
                    result["Group Name"] = text
                    group_name_found = True
                    break
        
        # Extract Group Logo URL
        logo_found = False
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            src = html.unescape(meta_image['content'])
            if OG_IMAGE_PATTERN.match(src) or IMAGE_PATTERN_PPS.match(src): # Check against both patterns
                result["Logo URL"] = src
                logo_found = True
        
        if not logo_found:
            img_tags = soup.find_all('img', src=True)
            for img in img_tags:
                src = html.unescape(img['src'])
                if IMAGE_PATTERN_PPS.match(src): # Specifically look for WhatsApp CDN images
                    result["Logo URL"] = src
                    logo_found = True
                    break
        
        # Final Status Determination based on extracted info and requirements
        # If status is still "Inactive" (initial default) and not "Expired" (from phrases)
        if result["Status"] == "Inactive": # Not an error, not explicitly expired by text
            if result["Group Name"]: # A valid, non-empty group name was found
                result["Status"] = "Active"
            else: # No valid group name found, status remains "Inactive"
                result["Status"] = "Inactive"
                result["Group Name"] = "" # Ensure it's an empty string

    except requests.exceptions.Timeout:
        result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError:
        result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e:
        result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e:
        # Catch any other parsing or unexpected errors
        result["Status"] = f"Processing Error ({type(e).__name__})"
    
    return result


def scrape_whatsapp_links_from_page(url, session=None):
    links = set()
    try:
        headers = get_random_headers_general()
        # Use provided session or create a new request
        response = session.get(url, headers=headers, timeout=15) if session else requests.get(url, headers=headers, timeout=15)
        response.encoding = 'utf-8' # Ensure correct encoding
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find links in <a> tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                parsed_url = urlparse(href)
                # Add only the base path, remove query params for uniqueness
                clean_link = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                if len(parsed_url.path.replace('/', '')) > 15 : # Basic check for valid-looking ID length
                    links.add(clean_link)

        # Find links in plain text using regex (more comprehensive)
        text_content = soup.get_text() # Get all text content from the page
        if WHATSAPP_DOMAIN in text_content: # Quick check if domain is present
            # Regex to find WhatsApp chat URLs
            # Improved regex to handle variations and avoid trailing punctuation
            found_text_links = re.findall(r'(https?://chat\.whatsapp\.com/([A-Za-z0-9_-]{18,25}))', text_content)
            for full_link, _ in found_text_links:
                parsed_url = urlparse(full_link)
                clean_link = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                links.add(clean_link)
                
    except requests.exceptions.Timeout:
        st.sidebar.warning(f"Scrape Timeout: {url[:50]}...", icon="⏱️")
    except requests.exceptions.HTTPError as e:
        st.sidebar.warning(f"Scrape HTTP Err {e.response.status_code}: {url[:50]}...", icon="⚠️")
    except requests.exceptions.RequestException as e: # Catch other network errors
        st.sidebar.warning(f"Scrape Net Err ({type(e).__name__}): {url[:50]}...", icon="⚠️")
    except Exception as e: # Catch parsing errors or others
        st.sidebar.warning(f"Scrape Parse Err ({type(e).__name__}): {url[:50]}...", icon="💣")
    return list(links)


def google_search_and_scrape(query, top_n=5):
    st.info(f"Googling '{query}' (top {top_n} results)...")
    all_scraped_wa_links = set()
    try:
        # Using the actual imported function
        search_page_urls = list(google_search_function_actual(query=query, num_results=top_n, lang="en"))
        
        if not search_page_urls:
            st.warning(f"No Google results for '{query}'. Possible reasons: "
                       f"1. Query yields no results. "
                       f"2. Google blocking (try VPN/wait). "
                       f"3. googlesearch-python library issue.", icon="🤔")
            return []

        st.success(f"Found {len(search_page_urls)} pages from Google. Scraping them for WhatsApp links...")
        prog_bar, stat_txt = st.progress(0), st.empty()
        
        with requests.Session() as scrape_session: # Use a session for multiple scrapes
            for i, url_from_google in enumerate(search_page_urls):
                stat_txt.text(f"Scraping page {i+1}/{len(search_page_urls)}: {url_from_google[:60]}...")
                wa_links_from_page = scrape_whatsapp_links_from_page(url_from_google, session=scrape_session)
                
                newly_found_count = 0
                for link in wa_links_from_page:
                    # Basic validation of link format before adding
                    if link.startswith(WHATSAPP_DOMAIN) and link not in all_scraped_wa_links:
                        all_scraped_wa_links.add(link)
                        newly_found_count += 1
                
                if newly_found_count > 0:
                    st.sidebar.info(f"Found {newly_found_count} new WA links on {url_from_google[:30]}...")
                prog_bar.progress((i+1)/len(search_page_urls))
        
        stat_txt.success(f"Scraping of Google results complete. Found {len(all_scraped_wa_links)} unique WhatsApp links from '{query}'.")
        return list(all_scraped_wa_links)

    except TypeError as e: # Handle potential TypeError from googlesearch library if parameters are wrong
        st.error(f"Google search TypeError: {e}. This might be an issue with the "
                 f"googlesearch-python library version or parameters.", icon="❌")
        return []
    except Exception as e: # Catch-all for other unexpected errors during search/scrape
        st.error(f"An unexpected error occurred during Google search/scrape for '{query}': {e}. "
                 f"Check your internet connection or the library.", icon="❌")
        return []


def crawl_website(start_url, max_depth=2, max_pages=50):
    scraped_whatsapp_links = set()
    if not start_url.strip(): return scraped_whatsapp_links

    # Prepend scheme if missing
    if not start_url.startswith(('http://', 'https://')):
        start_url = 'https://' + start_url
        st.sidebar.warning(f"Prepending 'https://' to start URL: {start_url}", icon="🔗")

    try:
        parsed_start_url = urlparse(start_url)
        if not parsed_start_url.netloc: # Check for a valid domain
            st.sidebar.error(f"Invalid start URL: {start_url}. Please provide a valid domain.", icon="🚫")
            return scraped_whatsapp_links
    except ValueError: # Catch errors from urlparse if URL is fundamentally malformed
        st.sidebar.error(f"Malformed start URL: {start_url}. Cannot parse.", icon="🚫")
        return scraped_whatsapp_links
        
    base_domain = parsed_start_url.netloc.replace('www.', '') # Normalize domain

    urls_in_queue_tuples = set() # To keep track of (url, depth) tuples in queue for uniqueness
    visited_urls = set()         # To keep track of normalized URLs already visited
    queue_list = []              # Using list as a queue: append to add, pop(0) to get

    queue_list.append((start_url, 0))
    urls_in_queue_tuples.add((start_url, 0))
    
    page_count = 0
    max_q_size = max_pages * 10 # Heuristic limit for queue size to prevent memory issues

    with requests.Session() as session, st.spinner(f"Crawling {base_domain}..."):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size: # Safety break for excessively large queues
                st.sidebar.warning(f"Crawl queue exceeded {max_q_size} items. Stopping early to prevent memory issues.", icon="❗️")
                queue_list = queue_list[:max_q_size] # Trim queue if needed, though loop will break

            current_url, depth = queue_list.pop(0)
            
            # Normalize URL for visited check (path only, no query/fragment)
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')

            if normalized_current_url in visited_urls or depth > max_depth:
                continue
            
            visited_urls.add(normalized_current_url)

            if page_count >= max_pages: break # Ensure we don't exceed max_pages

            st.sidebar.text(f"Crawl (D:{depth},P:{page_count+1},Q:{len(queue_list)}): {current_url[:50]}...")
            
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10)
                response.raise_for_status() # Check for HTTP errors

                # Only process HTML content
                if 'text/html' not in response.headers.get('Content-Type', '').lower():
                    continue
                
                page_count += 1
                
                wa_links_from_page = scrape_whatsapp_links_from_page(current_url, session=session) # Pass session
                newly_found_count = 0
                for link in wa_links_from_page:
                    if link.startswith(WHATSAPP_DOMAIN) and link not in scraped_whatsapp_links:
                        scraped_whatsapp_links.add(link)
                        newly_found_count += 1
                
                if newly_found_count > 0:
                    st.sidebar.info(f"Crawl: Found {newly_found_count} new WA links on {current_url[:30]}...")

                # Find new links to add to queue if depth allows
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag.get('href')
                        if href:
                            abs_url = urljoin(current_url, href) # Resolve relative URLs
                            parsed_abs_url = urlparse(abs_url)

                            # Validate new URL before adding to queue
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain and \
                               not parsed_abs_url.fragment: # Ignore fragments for queue uniqueness
                                
                                normalized_abs_url = urljoin(abs_url, parsed_abs_url.path or '/')
                                if normalized_abs_url not in visited_urls and (abs_url, depth + 1) not in urls_in_queue_tuples:
                                    if len(queue_list) < max_q_size : # Only add if queue is not overly full
                                        queue_list.append((abs_url, depth + 1))
                                        urls_in_queue_tuples.add((abs_url, depth + 1))
                                    else:
                                        st.sidebar.warning(f"Crawl queue full ({max_q_size}). Not adding more URLs.", icon="❗️")
                                        break # Stop adding new URLs if queue is full

            except requests.exceptions.RequestException as e:
                st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: # Catch other errors like parsing
                st.sidebar.error(f"Crawl Process Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
    
    st.sidebar.success(f"Crawl finished. Scraped {page_count} pages, found {len(scraped_whatsapp_links)} unique WhatsApp links.")
    if page_count >= max_pages: st.sidebar.warning(f"Crawl stopped at max {max_pages} pages.", icon="❗️")
    if len(urls_in_queue_tuples) > max_q_size : st.sidebar.warning(f"Crawl queue processing was capped.", icon="❗️") # len(queue_list) might be 0 here

    return list(scraped_whatsapp_links) # Return as list

def generate_styled_html_table(data_df_for_table):
    # Filter out groups with empty names, as "Active" groups should have names.
    # UNNAMED_GROUP_PLACEHOLDER is less relevant here if data has "" for no name.
    df_to_display = data_df_for_table[data_df_for_table['Group Name'] != ""].copy()
    
    if df_to_display.empty:
        return "<p style='text-align:center; color:#777; margin-top:20px;'><i>No groups with names match the current display filters. Try adjusting them.</i></p>"

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
        group_name = row.get("Group Name", "") # Default to empty string if somehow missing
        group_link = row.get("Group Link", "")
        
        html_string += '<tr>'
        html_string += '<td class="group-logo-cell">'
        alt_text = f"{html.escape(group_name) if group_name else 'Group'} Logo"
        if logo_url:
            # Append query param for WhatsApp profile pictures if applicable
            display_logo_url = append_query_param(logo_url, 'w', '96') if logo_url.startswith('https://pps.whatsapp.net/') else logo_url
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img" loading="lazy">'
        else:
            # Placeholder for missing logo
            html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        html_string += '</td>'
        
        safe_group_name = html.escape(group_name) if group_name else "<i>Unnamed</i>"
        html_string += f'<td class="group-name-cell">{safe_group_name}</td>'
        
        html_string += '<td class="join-button-cell">'
        if group_link and group_link.startswith(WHATSAPP_DOMAIN):
            html_string += f'<a href="{html.escape(group_link)}" class="join-button" target="_blank" rel="noopener noreferrer">Join Group</a>'
        else:
            html_string += '<span style="color:#888; font-size:0.9em;">N/A</span>' # Invalid link
        html_string += '</td></tr>'
        
    html_string += '</tbody></table>'
    return html_string


# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, Scrape, Validate, and Manage WhatsApp Group Links with Enhanced Filtering.</p>', unsafe_allow_html=True)
    
    # Initialize session state
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()
    if 'styled_table_name_keywords' not in st.session_state: st.session_state.styled_table_name_keywords = ""
    if 'styled_table_current_limit_value' not in st.session_state: st.session_state.styled_table_current_limit_value = 50
    
    # Default filter for advanced section: Active
    if 'adv_filter_status' not in st.session_state: 
        st.session_state.adv_filter_status = ["Active"] 
    if 'adv_filter_name_keywords' not in st.session_state: st.session_state.adv_filter_name_keywords = ""

    # Ensure processed_links_in_session is a set and populate it (idempotent)
    if not isinstance(st.session_state.processed_links_in_session, set):
        st.session_state.processed_links_in_session = set()
    if isinstance(st.session_state.results, list): # Check if results is a list
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and res_item['Group Link']:
                try:
                    # Normalize link before adding to processed set (scheme + netloc + path)
                    parsed_link = urlparse(res_item['Group Link'])
                    normalized_link = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
                    st.session_state.processed_links_in_session.add(normalized_link)
                except Exception: # Fallback if URL parsing fails
                    st.session_state.processed_links_in_session.add(res_item['Group Link'])

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
            # Google Search Limit: Increased to 100
            gs_top_n = st.slider("Google Results to Scrape (per keyword)", 1, 100, 5, key="gs_top_n_slider", 
                                 help="Number of Google search result pages to analyze per keyword.")
            # Added warning for selections over 20
            if gs_top_n > GOOGLE_SEARCH_MAX_RESULTS_SOFT_CAP:
                st.sidebar.warning(f"Warning: Selecting over {GOOGLE_SEARCH_MAX_RESULTS_SOFT_CAP} results may be slow and increase the risk of Google rate-limiting.", icon="⚠️")
        
        crawl_depth, crawl_pages = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be slow and resource-intensive. Use with caution.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider", help="0 for current page only.")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 300, 50, key="crawl_pages_slider")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.session_state.styled_table_name_keywords = ""
            st.session_state.styled_table_current_limit_value = 50
            st.session_state.adv_filter_status = ["Active"] # Reset to default "Active"
            st.session_state.adv_filter_name_keywords = ""
            st.cache_data.clear() # Clear any st.cache_data usage if added elsewhere
            st.success("All results and filter settings have been cleared!")
            st.rerun()

    # Action Zone
    current_action_scraped_links = set() # Use a set to store links from the current action
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
                    st.info(f"Processing {len(keywords)} keywords...")
                    prog_b, stat_b = st.progress(0), st.empty()
                    total_links_found_bulk = 0
                    for i, kw in enumerate(keywords):
                        stat_b.text(f"Keyword: '{kw}' ({i+1}/{len(keywords)}). Total unique links so far: {len(current_action_scraped_links)}")
                        links_from_kw = google_search_and_scrape(kw, gs_top_n)
                        current_action_scraped_links.update(links_from_kw)
                        prog_b.progress((i+1)/len(keywords))
                    total_links_found_bulk = len(current_action_scraped_links)
                    stat_b.success(f"Bulk processing complete. Found {total_links_found_bulk} unique WhatsApp links from {len(keywords)} keywords.")
                else: st.warning("No valid keywords found in the Excel file.")

        elif input_method == "Scrape from Specific Webpage URL":
            url = st.text_input("Webpage URL:", placeholder="https://example.com/page-with-whatsapp-links", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if url and (url.startswith("http://") or url.startswith("https://")):
                    with st.spinner(f"Scraping {url}..."):
                        found_links_page = scrape_whatsapp_links_from_page(url)
                        current_action_scraped_links.update(found_links_page)
                    st.success(f"Scraping from page done. Found {len(found_links_page)} WhatsApp links.")
                else: st.warning("Please enter a valid URL (starting with http:// or https://).")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain = st.text_input("Base Domain URL for Crawling:", placeholder="example.com (don't include http/https here unless specific)", key="crawl_domain_input")
            if st.button("Crawl Website & Scrape Links", use_container_width=True, key="crawl_button"):
                if domain:
                    st.info(f"Starting extensive crawl for '{domain}'. Progress will be shown in the sidebar.")
                    found_links_crawl = crawl_website(domain, crawl_depth, crawl_pages)
                    current_action_scraped_links.update(found_links_crawl)
                    st.success(f"Website crawl complete. Found {len(found_links_crawl)} unique WhatsApp links.")
                else: st.warning("Please enter a base domain URL to start crawling.")

        elif input_method == "Enter Links Manually (for Validation)":
            text_area_links = st.text_area("Enter WhatsApp Links (one per line):", height=200, key="manual_links_area", placeholder="https://chat.whatsapp.com/...")
            if st.button("Validate Manually Entered Links", use_container_width=True, key="manual_validate_button"):
                links_from_text_area = [line.strip() for line in text_area_links.split('\n') if line.strip()]
                if links_from_text_area:
                    valid_format_links = {l for l in links_from_text_area if l.startswith(WHATSAPP_DOMAIN)}
                    skipped_count = len(links_from_text_area) - len(valid_format_links)
                    if skipped_count > 0:
                        st.warning(f"Skipped {skipped_count} entries that do not look like WhatsApp links.")
                    current_action_scraped_links.update(valid_format_links)
                else: st.warning("Please enter at least one WhatsApp link.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file = st.file_uploader("Upload TXT/CSV (links) or Excel (keywords in 1st col)", type=["txt", "csv", "xlsx"], key="upload_file_input")
            if file and st.button("Process Uploaded File", use_container_width=True, key="upload_process_button"):
                if file.name.endswith('.xlsx'):
                    st.info("Processing Excel file for keywords to search on Google...")
                    keywords = load_keywords_from_excel(file)
                    if keywords:
                        prog_e, stat_e = st.progress(0), st.empty()
                        for i, kw in enumerate(keywords):
                            stat_e.text(f"Processing keyword: '{kw}' ({i+1}/{len(keywords)}). Links found so far: {len(current_action_scraped_links)}")
                            links_from_kw = google_search_and_scrape(kw, gs_top_n)
                            current_action_scraped_links.update(links_from_kw)
                            prog_e.progress((i+1)/len(keywords))
                        stat_e.success(f"Excel keyword processing complete. Found {len(current_action_scraped_links)} unique links.")
                    else: st.warning("No valid keywords found in the Excel file.")
                elif file.name.endswith(('.txt', '.csv')):
                    st.info("Processing TXT/CSV file for direct WhatsApp links...")
                    links_from_file = load_links_from_file(file)
                    if links_from_file:
                        valid_format_links = {l for l in links_from_file if l.startswith(WHATSAPP_DOMAIN)}
                        skipped_count = len(links_from_file) - len(valid_format_links)
                        if skipped_count > 0:
                            st.warning(f"Skipped {skipped_count} entries from file that do not look like WhatsApp links.")
                        current_action_scraped_links.update(valid_format_links)
                        st.success(f"Loaded {len(valid_format_links)} valid-format WhatsApp links from the file.")
                    else: st.warning("No valid WhatsApp links found in the uploaded file.")
                else: st.warning("Unsupported file type. Please use .txt, .csv, or .xlsx.")
    except Exception as e:
        st.error(f"An error occurred in the Action Zone: {e}", icon="💥")

    # Validation
    # Normalize current action links before comparing with processed_links_in_session
    normalized_current_action_links = set()
    for link in current_action_scraped_links:
        try:
            parsed = urlparse(link)
            normalized_current_action_links.add(f"{parsed.scheme}://{parsed.netloc}{parsed.path}")
        except Exception: # Fallback for unparseable links
            normalized_current_action_links.add(link)

    links_to_validate_now = list(normalized_current_action_links - st.session_state.processed_links_in_session)
    
    if links_to_validate_now:
        st.success(f"Collected {len(normalized_current_action_links)} unique links from current action. "
                   f"Validating {len(links_to_validate_now)} new links...")
        prog_val, stat_val = st.progress(0), st.empty()
        new_results_this_run = []
        
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
            for i, future in enumerate(as_completed(future_to_link)):
                link_validated = future_to_link[future]
                try:
                    result_validated = future.result()
                    new_results_this_run.append(result_validated)
                    # Add successfully processed link to session tracker (normalized)
                    st.session_state.processed_links_in_session.add(link_validated) # link_validated is already normalized here
                except Exception as val_exc:
                    st.warning(f"Critical error validating {link_validated[:40]}...: {val_exc}", icon="⚠️")
                    # Add to session tracker even if validation fails to avoid re-processing error links
                    st.session_state.processed_links_in_session.add(link_validated)
                    new_results_this_run.append({
                        "Group Name": "", "Group Link": link_validated, "Logo URL": "", 
                        "Status": f"Validation Crash: {type(val_exc).__name__}"
                    })
                prog_val.progress((i+1)/len(links_to_validate_now))
                stat_val.text(f"Validated {i+1}/{len(links_to_validate_now)} links: {link_validated[:50]}...")
        
        if new_results_this_run:
            st.session_state.results.extend(new_results_this_run)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
    elif current_action_scraped_links and not links_to_validate_now: # Links were found, but all were already processed
        st.info("All WhatsApp links found in this action were previously processed. No new links to validate.")

    # Results Display
    if 'results' in st.session_state and st.session_state.results:
        # Deduplicate results based on 'Group Link' ensuring we keep the first occurrence (or 'last' if re-validation updates)
        # For simplicity, keeping 'first' is fine if re-validation isn't a primary feature for existing links.
        unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = unique_results_df.to_dict('records') # Update session state with deduped list
        df_display_master = unique_results_df.reset_index(drop=True)

        active_df_all_master = df_display_master[df_display_master['Status'] == 'Active'].copy() # Strictly 'Active'
        expired_df_master = df_display_master[df_display_master['Status'] == 'Expired'].copy()
        # "Other" includes "Inactive", "HTTP Error", "Timeout Error", etc.
        other_status_df_master = df_display_master[
            ~df_display_master['Status'].isin(['Active', 'Expired'])
        ].copy()

        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master)}</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df_all_master)}</div></div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="metric-card">Expired Links<br><div class="metric-value">{len(expired_df_master)}</div></div>', unsafe_allow_html=True)
        col4.markdown(f'<div class="metric-card">Other Statuses<br><div class="metric-value">{len(other_status_df_master)}</div></div>', unsafe_allow_html=True)

        # Styled Table with Filters (Primarily for Active Groups)
        st.subheader("✨ Active Groups Display (Styled Table)")
        with st.expander("View and Filter Active Groups Table", expanded=True):
            if not active_df_all_master.empty:
                st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                st.markdown("#### Filter Displayed Active Groups (in table below):")

                with st.form("styled_table_filters_form"):
                    name_keywords_input_styled = st.text_input(
                        "Filter by Group Name Keywords (comma-separated):",
                        value=st.session_state.styled_table_name_keywords,
                        placeholder="e.g., study, fun, tech (applies to table)",
                        help="Enter keywords (comma-separated). Shows groups matching ANY keyword in this table."
                    ).strip()
                    limit_input_styled = st.number_input(
                        "Max Groups to Display in This Table:",
                        min_value=1, max_value=1000,
                        value=st.session_state.styled_table_current_limit_value, step=10,
                        help="Set max groups for this styled table display."
                    )
                    apply_styled_filters = st.form_submit_button("Apply Filters to Table")

                if apply_styled_filters:
                    st.session_state.styled_table_name_keywords = name_keywords_input_styled
                    st.session_state.styled_table_current_limit_value = limit_input_styled
                    st.rerun() # Rerun to apply filters immediately

                if st.button("Reset Table Filters", key="reset_styled_table_filters_button"):
                    st.session_state.styled_table_name_keywords = ""
                    st.session_state.styled_table_current_limit_value = 50
                    st.rerun()

                active_df_for_styled_table = active_df_all_master.copy()
                if st.session_state.styled_table_name_keywords:
                    keywords_list_styled = [kw.strip().lower() for kw in st.session_state.styled_table_name_keywords.split(',') if kw.strip()]
                    if keywords_list_styled:
                        regex_pattern_styled = '|'.join(map(re.escape, keywords_list_styled))
                        active_df_for_styled_table = active_df_for_styled_table[
                            active_df_for_styled_table['Group Name'].str.lower().str.contains(regex_pattern_styled, na=False, regex=True)
                        ]
                
                num_matching_styled = len(active_df_for_styled_table)
                num_displayed_styled = min(num_matching_styled, st.session_state.styled_table_current_limit_value)
                active_df_for_styled_table_final = active_df_for_styled_table.head(num_displayed_styled)

                st.write(f"Showing {num_displayed_styled} of {num_matching_styled} matching active groups in the table.")
                html_out = generate_styled_html_table(active_df_for_styled_table_final)
                st.markdown(html_out, unsafe_allow_html=True)
                st.markdown("---")
                st.text_area("Copy Raw HTML Code (for above table):", value=html_out, height=150, key="styled_html_export_area_key", help="Ctrl+A, Ctrl+C to copy.")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("No active groups found yet to display in the styled table.")

        # Advanced Filtering for Downloads & Full Dataset Analysis
        with st.expander("🔬 Advanced Filtering (Full Dataset for Download/Analysis)", expanded=False):
            st.markdown('<div class="filter-container" style="border-style:solid;">', unsafe_allow_html=True)
            st.markdown("#### Filter Full Dataset (for Download/Raw Data View):")
            
            all_statuses_master = sorted(list(df_display_master['Status'].unique()))
            
            # Default Filter: Set "Active" as the default for status_filter
            # st.session_state.adv_filter_status is initialized to ["Active"]
            # If "Active" is not in all_statuses_master, it will just be an ignored default.
            selected_statuses_adv = st.multiselect(
                "Filter by Status (for download/analysis):", options=all_statuses_master,
                default=st.session_state.adv_filter_status, # Uses session state, initialized to ["Active"]
                key="adv_status_filter_multiselect_key",
                help="Select statuses to include in the dataset view below and for 'Filtered Processed Results' download."
            )
            if selected_statuses_adv != st.session_state.adv_filter_status: # Check if changed
                st.session_state.adv_filter_status = selected_statuses_adv
                st.rerun()


            name_keywords_adv = st.text_input(
                "Filter by Group Name Keywords (for download/analysis, comma-separated):", 
                value=st.session_state.adv_filter_name_keywords,
                key="adv_name_keyword_filter_input_key", placeholder="e.g., news, jobs (applies to dataset)",
                help="Applies to the entire dataset for download/analysis. Comma-separated."
            ).strip()
            if name_keywords_adv != st.session_state.adv_filter_name_keywords: # Check if changed
                st.session_state.adv_filter_name_keywords = name_keywords_adv
                st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)

            # --- Logic for df_for_adv_download_or_view ---
            df_for_adv_download_or_view = df_display_master.copy()
            user_filters_applied_adv = False

            if st.session_state.adv_filter_status: # If user has selected any status filters
                df_for_adv_download_or_view = df_for_adv_download_or_view[
                    df_for_adv_download_or_view['Status'].isin(st.session_state.adv_filter_status)
                ]
                user_filters_applied_adv = True
            else: # No status filters selected by user (cleared the default "Active")
                  # In this case, we show ALL results, including Inactive for the download.
                  # The requirement "Exclude "Inactive" groups from the default "All Results" CSV"
                  # implies that IF NO filter is set, "Inactive" is out.
                  # If user CLEARS filter, they might want all.
                  # Let's stick to: if st.session_state.adv_filter_status is empty, show all.
                  # The default ["Active"] handles the "exclude Inactive by default".
                  pass # Show all if status filter is empty


            if st.session_state.adv_filter_name_keywords:
                adv_keywords_list = [kw.strip().lower() for kw in st.session_state.adv_filter_name_keywords.split(',') if kw.strip()]
                if adv_keywords_list:
                    adv_regex_pattern = '|'.join(map(re.escape, adv_keywords_list))
                    df_for_adv_download_or_view = df_for_adv_download_or_view[
                        df_for_adv_download_or_view['Group Name'].str.lower().str.contains(adv_regex_pattern, na=False, regex=True)
                    ]
                    user_filters_applied_adv = True
            
            st.markdown(f"**Preview of Data for Download/Analysis ({'User Filters Applied' if user_filters_applied_adv else 'Default View: Active only (or all if Active was cleared)'} - {len(df_for_adv_download_or_view)} rows):**")
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
            active_csv_data = active_df_all_master.to_csv(index=False).encode('utf-8')
            dl_col1.download_button("Active Groups (CSV)", active_csv_data, "active_groups.csv", 
                                    "text/csv", use_container_width=True, key="dl_active_csv_main_key")
        else:
            dl_col1.button("Active Groups (CSV)", disabled=True, use_container_width=True, 
                           help="No active groups available to download.")

        # For "All Processed Results" or "Filtered Processed Results"
        # The df_for_adv_download_or_view is now determined by user's explicit filter choices.
        # If st.session_state.adv_filter_status is ["Active"] (default), then it's effectively "Active plus keywords".
        # If user clears all filters, it's truly all.
        
        download_label_all = "All Processed Results (CSV)"
        df_for_download_all = df_display_master.copy() # Start with everything for the "All Processed" base

        # CSV Outputs: Exclude "Inactive" groups from the default "All Results" CSV.
        # This means if user_filters_applied_adv is FALSE, then we exclude Inactive.
        # If user_filters_applied_adv is TRUE, then df_for_adv_download_or_view respects user's choices.

        if not user_filters_applied_adv: # No explicit user filters set in advanced section
            # Default "All Processed" excludes Inactive
            df_final_download = df_display_master[df_display_master['Status'] != 'Inactive'].copy()
            download_label_all = f"All Processed Results (excl. Inactive) ({len(df_final_download)} rows) (CSV)"
        else: # User has applied filters
            df_final_download = df_for_adv_download_or_view.copy() # This df already reflects user's choices
            download_label_all = f"Filtered Processed Results ({len(df_final_download)} rows) (CSV)"


        if not df_final_download.empty:
            all_csv_data = df_final_download.to_csv(index=False).encode('utf-8')
            dl_col2.download_button(download_label_all, all_csv_data, "processed_whatsapp_groups.csv", 
                                    "text/csv", use_container_width=True, key="dl_all_or_filtered_csv_key")
        elif not df_display_master.empty() and df_final_download.empty() and user_filters_applied_adv:
            # Data exists, but current advanced filters yield no results
            dl_col2.button("No Results Match Advanced Filters", disabled=True, use_container_width=True)
        else: # No data at all, or no data for default "All Processed (excl. Inactive)"
            dl_col2.button(download_label_all, disabled=True, use_container_width=True, 
                           help="No results to download based on current view/filters.")
            
    else: # No results in session state yet
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file to see results!", icon="ℹ️")

if __name__ == "__main__":
    main()
