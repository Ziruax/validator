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
    from googlesearch import search as google_search_function_actual
except ImportError:
    st.error("The `googlesearch-python` library is not installed. Please install it: `pip install googlesearch-python`")
    def google_search_function_actual(query, num_results, lang, **kwargs): # type: ignore
        st.error("`googlesearch-python` library not found. Cannot perform Google searches.")
        return []

# --- Import Fake User Agent Library & Define Header Function ---
_ua_object = None
_fake_useragent_initialized_successfully = False
_DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

try:
    from fake_useragent import UserAgent, FakeUserAgentError
    _ua_object = UserAgent(fallback=_DEFAULT_USER_AGENT) # Provide fallback directly
    _fake_useragent_initialized_successfully = True
except ImportError:
    st.warning("`fake-useragent` library not found. Install with `pip install fake-useragent`. Using default User-Agent.", icon="⚠️")
except FakeUserAgentError as e_init:
    st.warning(f"Error initializing fake-useragent (will use fallback): {e_init}", icon="⚠️")
except Exception as e_general_init: # Catch any other unexpected error during UserAgent() init
    st.warning(f"Unexpected error initializing fake-useragent (will use fallback): {e_general_init}", icon="⚠️")

def get_random_headers_general():
    user_agent_str = _DEFAULT_USER_AGENT
    if _fake_useragent_initialized_successfully and _ua_object:
        try:
            user_agent_str = _ua_object.random
        except FakeUserAgentError:
             # This might happen if the cache is empty and network fails for fake-useragent
             st.sidebar.warning("fake-useragent failed to get a User-Agent on-the-fly. Using fallback.", icon="⚠️") # More specific warning
        except Exception:
            # Silently use fallback for any other error during .random
            pass
    return {
        "User-Agent": user_agent_str,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        # "DNT": "1" # Do Not Track - Optional
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
IMAGE_PATTERN_PPS = re.compile(r'https:\/\/pps\.whatsapp\.net\/v\/t\d+\/[-\w]+\/\d+\.jpg\?') # Unused currently but kept
OG_IMAGE_PATTERN = re.compile(r'https?:\/\/[^\/\s]+\/[^\/\s]+\.(jpg|jpeg|png)(\?[^\s]*)?')
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
    if not url or not isinstance(url, str): return ""
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        query_params[param_name] = [param_value]
        new_query_string = urlencode(query_params, doseq=True)
        url_without_fragment = parsed_url._replace(query=new_query_string, fragment='').geturl()
        return f"{url_without_fragment}#{parsed_url.fragment}" if parsed_url.fragment else url_without_fragment
    except Exception:
        return url

def load_keywords_from_excel(uploaded_file):
    if uploaded_file is None: return []
    try:
        uploaded_file.seek(0)
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
        uploaded_file.seek(0)
        content = uploaded_file.getvalue()
        text_content = None
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                text_content = content.decode(encoding)
                break
            except UnicodeDecodeError: continue
        if text_content is None:
             st.error(f"Could not decode file {uploaded_file.name}.", icon="❌"); return []

        if uploaded_file.name.endswith('.csv'):
            try:
                 df = pd.read_csv(io.StringIO(text_content), header=None)
                 if df.empty: st.warning("CSV file is empty."); return []
                 return [
                    link.strip() for link in df.iloc[:, 0].dropna().astype(str).tolist() 
                    if link.strip().startswith(('http://', 'https://'))
                 ]
            except Exception as e:
                 st.error(f"Error reading CSV: {e}.", icon="❌"); return []
        else: # Assume TXT
             return [line.strip() for line in text_content.splitlines() if line.strip() and line.strip().startswith(('http://', 'https://'))]
    except Exception as e:
        st.error(f"Error processing file {uploaded_file.name}: {e}", icon="❌"); return []

# --- Core Logic Functions ---
def validate_link(link):
    result = {
        "Group Name": UNNAMED_GROUP_PLACEHOLDER,
        "Group Link": link,
        "Logo URL": "",
        "Status": "Error_Initial" # Default status before processing
    }
    group_name_found_flag = False # Flag to track if a meaningful name was scraped
    logo_found_flag = False       # Flag to track if a logo was scraped

    try:
        response = requests.get(link, headers=get_random_headers_general(), timeout=20, allow_redirects=True)
        response.encoding = 'utf-8'
        
        if response.status_code != 200:
            result["Status"] = "Expired (404 Not Found)" if response.status_code == 404 else f"HTTP Error {response.status_code}"
            return result
        
        final_url_domain = urlparse(response.url).netloc.lower()
        if "chat.whatsapp.com" not in final_url_domain:
            result["Status"] = f"Redirected Away ({final_url_domain or 'Unknown Site'})"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        page_text_lower = soup.get_text().lower()
        
        is_expired_by_text_phrase = False
        expired_phrases = [
            "invite link is invalid", "invite link was reset", 
            "group doesn't exist", "this group is no longer available",
            "this group is full", "you can't join this group because it's full"
        ]
        if any(phrase in page_text_lower for phrase in expired_phrases):
            is_expired_by_text_phrase = True
            result["Status"] = "Expired/Full" 

        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name_text = meta_title['content']
            if group_name_text and isinstance(group_name_text, str):
                group_name = html.unescape(group_name_text).strip()
                if group_name: 
                    result["Group Name"] = group_name
                    group_name_found_flag = True
        
        if not group_name_found_flag:
             potential_name_tags = soup.find_all(['h2', 'strong', 'span'], class_=re.compile('group-name', re.IGNORECASE)) + \
                                   soup.find_all('div', class_=re.compile('name', re.IGNORECASE))
             for tag in potential_name_tags:
                 text = tag.get_text().strip()
                 if text and len(text) > 2 and text.lower() not in ["whatsapp group invite", "whatsapp", "join group", "invite link", "group invite"]:
                     result["Group Name"] = html.unescape(text)
                     group_name_found_flag = True
                     break
        
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            og_image_src = meta_image['content']
            if og_image_src and isinstance(og_image_src, str):
                src = html.unescape(og_image_src)
                if OG_IMAGE_PATTERN.match(src) or src.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                    result["Logo URL"] = src
                    logo_found_flag = True
        
        if not logo_found_flag:
            for img_tag in soup.find_all('img', src=True):
                img_src_attr = img_tag.get('src')
                if img_src_attr and isinstance(img_src_attr, str):
                    src = html.unescape(img_src_attr)
                    if src.startswith('https://pps.whatsapp.net/'):
                        result["Logo URL"] = src
                        logo_found_flag = True
                        break
        
        if is_expired_by_text_phrase:
            action_button = soup.find('a', attrs={'id': 'action-button'})
            if action_button and action_button.get('href'):
                try:
                    button_href_path = urlparse(action_button.get('href')).path
                    original_link_path = urlparse(link).path
                    if button_href_path == original_link_path:
                        result["Status"] = "Active (Action Button Present)" 
                except ValueError: 
                    pass 
        else:
            if group_name_found_flag or logo_found_flag:
                result["Status"] = "Active"
            else:
                result["Status"] = "Expired (No Name/Logo)"

    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError: result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: result["Status"] = f"Parsing Error ({type(e).__name__})"
    
    if result["Status"] == "Error_Initial":
        if not (group_name_found_flag or logo_found_flag) and not is_expired_by_text_phrase : # Check if an error happened AND no metadata was found and no expired text
             result["Status"] = "Error (No Name/Logo)"
        else:
            result["Status"] = "Unknown Processing Error" # Generic error if not covered above

    if not group_name_found_flag: # Ensure placeholder if no specific name found
        result["Group Name"] = UNNAMED_GROUP_PLACEHOLDER

    return result

def scrape_whatsapp_links_from_page(url, session=None):
    links = set()
    try:
        headers = get_random_headers_general()
        response = session.get(url, headers=headers, timeout=15, allow_redirects=True) if session else requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and isinstance(href, str) and href.startswith(WHATSAPP_DOMAIN):
                try:
                    parsed_url = urlparse(href)
                    clean_link = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                    if len(parsed_url.path.replace('/', '')) > 15:
                        links.add(clean_link)
                except Exception:
                    pass
        
        text_content = soup.get_text()
        if WHATSAPP_DOMAIN in text_content:
            raw_found_links = re.findall(r'(https?://chat\.whatsapp\.com/[^\s"\'<>()\[\]{}]+)', text_content)
            for link_url in raw_found_links:
                try:
                    clean_link_text = re.sub(r'[.,;!?"\'<>)]+$', '', link_url)
                    clean_link_text = clean_link_text.split('&')[0]
                    parsed_url = urlparse(clean_link_text)
                    normalized_link = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                    if len(parsed_url.path.replace('/', '')) > 15:
                        links.add(normalized_link)
                except Exception:
                    pass
                    
    except requests.exceptions.Timeout: st.sidebar.warning(f"Scrape Timeout: {url[:50]}...", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Scrape HTTP Err {e.response.status_code}: {url[:50]}...", icon="⚠️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape Net Err ({type(e).__name__}): {url[:50]}...", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Scrape Parse Err ({type(e).__name__}): {url[:50]}...", icon="💣")
    return list(links)

def google_search_and_scrape(query, top_n=5):
    st.info(f"Googling '{query}' (top {top_n} results)...")
    all_scraped_wa_links = set()
    try:
        search_page_urls = list(google_search_function_actual(query, num_results=top_n, lang="en", pause=2.0))
        
        if not search_page_urls:
            st.warning(f"No Google results for '{query}'. Possible reasons: "
                       f"1. Query yields no results. "
                       f"2. Google blocking (try VPN/wait, increase pause). "
                       f"3. `googlesearch-python` library issue.", icon="🤔")
            return []

        st.success(f"Found {len(search_page_urls)} pages from Google. Scraping them for WhatsApp links...")
        prog_bar, stat_txt = st.progress(0.0), st.empty()
        with requests.Session() as scrape_session:
            for i, url_from_google in enumerate(search_page_urls):
                url_str = str(url_from_google) # Ensure it's a string
                stat_txt.text(f"Scraping page {i+1}/{len(search_page_urls)}: {url_str[:60]}...")
                wa_links_from_page = scrape_whatsapp_links_from_page(url_str, session=scrape_session)
                newly_found_count = 0
                for link in wa_links_from_page:
                    if isinstance(link, str) and link.startswith(WHATSAPP_DOMAIN) and link not in all_scraped_wa_links:
                        all_scraped_wa_links.add(link)
                        newly_found_count +=1
                if newly_found_count > 0:
                    st.sidebar.info(f"Found {newly_found_count} new WA links on {url_str[:30]}...")
                prog_bar.progress(float(i+1)/len(search_page_urls))
        stat_txt.success(f"Scraping of Google results complete. Found {len(all_scraped_wa_links)} unique WhatsApp links from '{query}'.")
        return list(all_scraped_wa_links)
    except TypeError as e:
        st.error(f"Google search TypeError for '{query}': {e}. This might indicate Google blocking or a library issue. Try a VPN or wait.", icon="❌")
        return []
    except Exception as e:
        st.error(f"Unexpected Google search/scrape error for '{query}': {e}. Check connection/library.", icon="❌")
        return []

def crawl_website(start_url, max_depth=2, max_pages=50):
    scraped_whatsapp_links = set()
    if not start_url or not isinstance(start_url, str) or not start_url.strip():
        st.sidebar.error("Invalid start URL provided for crawl.", icon="🚫")
        return list(scraped_whatsapp_links) # Return list
    
    start_url = start_url.strip()
    if not start_url.startswith(('http://', 'https://')):
         start_url = 'https://' + start_url
         st.sidebar.warning(f"Prepending 'https://': {start_url}", icon="🔗")

    try:
        parsed_start_url = urlparse(start_url)
        if not parsed_start_url.netloc:
            st.sidebar.error(f"Invalid start URL (missing domain): {start_url}", icon="🚫")
            return list(scraped_whatsapp_links)
    except ValueError:
        st.sidebar.error(f"Could not parse start URL: {start_url}", icon="🚫")
        return list(scraped_whatsapp_links)
        
    base_domain = parsed_start_url.netloc.replace('www.', '')
    urls_in_queue_tuples, visited_urls, queue_list = set(), set(), [] 
    
    queue_list.append((start_url, 0))
    urls_in_queue_tuples.add((start_url, 0)) 
    
    page_count = 0
    max_q_size = max_pages * 10 

    with requests.Session() as session, st.spinner(f"Crawling {base_domain}..."):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size:
                 st.sidebar.warning(f"Crawl queue exceeded {max_q_size}. Stopping early.", icon="❗️")
                 break 

            current_url, depth = queue_list.pop(0)
            
            try:
                parsed_current = urlparse(current_url)
                normalized_current_url = urljoin(current_url, parsed_current.path or '/')
            except ValueError:
                st.sidebar.warning(f"Crawl: Malformed URL in queue, skipping: {current_url[:50]}...", icon="🕸️")
                continue

            if normalized_current_url in visited_urls or depth > max_depth:
                continue
            
            visited_urls.add(normalized_current_url)

            if page_count >= max_pages: break

            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}/{max_pages}, Q:{len(queue_list)}): {current_url[:50]}...")
            
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10, allow_redirects=True)
                response.raise_for_status()
                
                final_url_netloc = urlparse(response.url).netloc.replace('www.', '')
                if final_url_netloc != base_domain:
                    st.sidebar.info(f"Crawl: Redirected off-domain from {current_url[:30]} to {response.url[:30]}. Not following.", icon="↪️")
                    continue

                if 'text/html' not in response.headers.get('Content-Type', '').lower():
                    st.sidebar.info(f"Crawl: Skipping non-HTML content at {current_url[:30]}...", icon="📄")
                    continue
                
                page_count += 1
                wa_links_from_page = scrape_whatsapp_links_from_page(response.url, session=session)
                newly_found_count = 0
                for link in wa_links_from_page:
                    if link.startswith(WHATSAPP_DOMAIN) and link not in scraped_whatsapp_links:
                        scraped_whatsapp_links.add(link)
                        newly_found_count +=1
                if newly_found_count > 0:
                    st.sidebar.info(f"Crawl: Found {newly_found_count} new WA links on {response.url[:30]}...")

                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href_attr = link_tag.get('href')
                        if href_attr and isinstance(href_attr, str):
                            try:
                                abs_url = urljoin(response.url, href_attr.strip())
                                parsed_abs_url = urlparse(abs_url)
                                
                                if parsed_abs_url.scheme in ['http', 'https'] and \
                                   parsed_abs_url.netloc and \
                                   parsed_abs_url.netloc.replace('www.', '') == base_domain:
                                    normalized_abs_url_for_visited = urljoin(abs_url, parsed_abs_url.path or '/')
                                    if normalized_abs_url_for_visited not in visited_urls and \
                                       (abs_url, depth + 1) not in urls_in_queue_tuples and \
                                       len(queue_list) < max_q_size:
                                        queue_list.append((abs_url, depth + 1))
                                        urls_in_queue_tuples.add((abs_url, depth + 1))
                            except ValueError:
                                pass 

            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl Page Parse Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
    
    st.sidebar.success(f"Crawl done. Scraped {page_count} pages, found {len(scraped_whatsapp_links)} unique WA links.")
    if page_count >= max_pages: st.sidebar.warning(f"Crawl stopped: Reached max pages limit ({max_pages}).", icon="❗️")
    if len(queue_list) >= max_q_size and page_count < max_pages : st.sidebar.warning(f"Crawl stopped: Queue size limit hit.", icon="❗️")
    return list(scraped_whatsapp_links)


def generate_styled_html_table(data_df_for_table):
    df_to_display = data_df_for_table[data_df_for_table['Group Name'] != UNNAMED_GROUP_PLACEHOLDER].copy()
    
    if df_to_display.empty:
        return "<p style='text-align:center; color:#777; margin-top:20px;'><i>No groups match the current display filters or all are 'Unnamed Group'.</i></p>"

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
        group_name = str(row.get("Group Name", UNNAMED_GROUP_PLACEHOLDER))
        group_link = str(row.get("Group Link", ""))

        html_string += '<tr>'
        html_string += '<td class="group-logo-cell">'
        safe_group_name_escaped = html.escape(group_name, quote=True)
        alt_text = f"{safe_group_name_escaped} Group Logo"
        
        if logo_url and isinstance(logo_url, str) and logo_url.strip():
            display_logo_url = logo_url 
            if logo_url.startswith('https://pps.whatsapp.net/'):
                display_logo_url = append_query_param(logo_url, 'w', '96')
            html_string += f'<img src="{html.escape(display_logo_url, quote=True)}" alt="{alt_text}" class="group-logo-img" loading="lazy">'
        else:
             html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        html_string += '</td>'
        html_string += f'<td class="group-name-cell">{safe_group_name_escaped}</td>'
        html_string += '<td class="join-button-cell">'
        if group_link and group_link.startswith(WHATSAPP_DOMAIN):
             html_string += f'<a href="{html.escape(group_link, quote=True)}" class="join-button" target="_blank" rel="noopener noreferrer">Join Group</a>'
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
    if 'styled_table_limit_active' not in st.session_state: st.session_state.styled_table_limit_active = False
    if 'styled_table_current_limit_value' not in st.session_state: st.session_state.styled_table_current_limit_value = 50
    if 'adv_filter_status' not in st.session_state: st.session_state.adv_filter_status = []
    if 'adv_filter_name_keywords' not in st.session_state: st.session_state.adv_filter_name_keywords = ""
    
    if not isinstance(st.session_state.processed_links_in_session, set):
        st.session_state.processed_links_in_session = set()
    if isinstance(st.session_state.results, list) and \
       len(st.session_state.processed_links_in_session) < len(st.session_state.results):
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and isinstance(res_item['Group Link'], str) and res_item['Group Link']:
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

        gs_top_n_default = 5
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            gs_top_n_default = st.slider("Google Results to Scrape (per keyword)", 1, 20, 5, key="gs_top_n_slider", help="Number of Google search result pages to analyze per keyword.")
        
        crawl_depth_default, crawl_pages_default = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be slow and resource-intensive. Use with caution.", icon="🚨")
            crawl_depth_default = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider", help="How many links deep to follow from the start page.")
            crawl_pages_default = st.slider("Max Pages to Crawl", 10, 300, 50, key="crawl_pages_slider", help="Maximum number of pages to fetch and parse.")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button_main"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.session_state.styled_table_name_keywords = ""
            st.session_state.styled_table_limit_active = False
            st.session_state.styled_table_current_limit_value = 50
            st.session_state.adv_filter_status = []
            st.session_state.adv_filter_name_keywords = ""
            if hasattr(st, 'cache_data') and callable(st.cache_data.clear):
                st.cache_data.clear()
            elif hasattr(st, 'cache') and callable(st.cache.clear): # For older Streamlit versions
                 st.cache.clear()
            st.success("All results and filters have been cleared!")
            st.rerun()

    current_action_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input_single")
            if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button_single"):
                if query and query.strip():
                    current_action_scraped_links.update(google_search_and_scrape(query.strip(), gs_top_n_default))
                else: st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            uploaded_excel_keywords = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload_keywords")
            if uploaded_excel_keywords and st.button("Process Excel, Scrape & Validate", use_container_width=True, key="gs_bulk_button_process"):
                keywords = load_keywords_from_excel(uploaded_excel_keywords)
                if keywords:
                    st.info(f"Processing {len(keywords)} keywords from Excel...")
                    prog_b, stat_b = st.progress(0.0), st.empty()
                    total_links_found_bulk = 0
                    for i, kw in enumerate(keywords):
                        stat_b.text(f"Keyword: '{kw}' ({i+1}/{len(keywords)}). Total links so far: {total_links_found_bulk}")
                        links_from_kw = google_search_and_scrape(kw, gs_top_n_default)
                        current_action_scraped_links.update(links_from_kw)
                        total_links_found_bulk = len(current_action_scraped_links)
                        prog_b.progress(float(i+1)/len(keywords))
                    stat_b.success(f"Bulk keyword processing complete. Found {total_links_found_bulk} unique WhatsApp links.")
                else: st.warning("No valid keywords found in the uploaded Excel file.")

        elif input_method == "Scrape from Specific Webpage URL":
            url_specific_page = st.text_input("Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input_page")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button_scrape"):
                if url_specific_page and url_specific_page.strip().startswith(('http://', 'https://')):
                    with st.spinner(f"Scraping {url_specific_page.strip()}..."):
                        current_action_scraped_links.update(scrape_whatsapp_links_from_page(url_specific_page.strip()))
                    st.success(f"Scraping from specific page done. Found {len(current_action_scraped_links)} potential links.")
                else: st.warning("Please enter a valid URL (starting with http:// or https://).")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_to_crawl = st.text_input("Base Domain URL to Crawl:", placeholder="example.com (without http/https)", key="crawl_domain_input_base")
            if st.button("Crawl Website & Scrape Links", use_container_width=True, key="crawl_button_start"):
                if domain_to_crawl and domain_to_crawl.strip():
                    st.info(f"Starting extensive crawl of '{domain_to_crawl.strip()}'. Progress will be shown in the sidebar.")
                    current_action_scraped_links.update(crawl_website(domain_to_crawl.strip(), crawl_depth_default, crawl_pages_default))
                    st.success(f"Website crawl finished. Found {len(current_action_scraped_links)} potential WhatsApp links.")
                else: st.warning("Please enter a base domain to crawl.")

        elif input_method == "Enter Links Manually (for Validation)":
            manual_links_text = st.text_area("Enter WhatsApp Links (one per line):", height=200, key="manual_links_input_area", placeholder="https://chat.whatsapp.com/...\nhttps://chat.whatsapp.com/...")
            if st.button("Validate Manually Entered Links", use_container_width=True, key="manual_validate_button_submit"):
                links_from_textarea = [line.strip() for line in manual_links_text.split('\n') if line.strip()]
                if links_from_textarea:
                    valid_format_links = {link for link in links_from_textarea if link.startswith(WHATSAPP_DOMAIN)}
                    invalid_format_count = len(links_from_textarea) - len(valid_format_links)
                    if invalid_format_count > 0:
                        st.warning(f"Skipped {invalid_format_count} entered lines that do not look like WhatsApp links.")
                    current_action_scraped_links.update(valid_format_links)
                    if not valid_format_links: st.warning("No valid WhatsApp link formats entered.")
                else: st.warning("Please enter some WhatsApp links in the text area.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            uploaded_file_links_or_keywords = st.file_uploader("Upload TXT/CSV (for links) or Excel (for keywords)", type=["txt", "csv", "xlsx"], key="upload_file_combined_input")
            if uploaded_file_links_or_keywords and st.button("Process Uploaded File", use_container_width=True, key="upload_process_button_file"):
                file_name = uploaded_file_links_or_keywords.name.lower()
                if file_name.endswith('.xlsx'):
                    st.info("Processing Excel file: Assuming keywords for Google Search...")
                    keywords_from_excel_upload = load_keywords_from_excel(uploaded_file_links_or_keywords)
                    if keywords_from_excel_upload:
                        prog_e, stat_e = st.progress(0.0), st.empty()
                        total_links_from_excel_keywords = 0
                        for i, kw in enumerate(keywords_from_excel_upload):
                            stat_e.text(f"Keyword: {kw} ({i+1}/{len(keywords_from_excel_upload)}). Links found: {total_links_from_excel_keywords}")
                            links_from_kw_upload = google_search_and_scrape(kw, gs_top_n_default)
                            current_action_scraped_links.update(links_from_kw_upload)
                            total_links_from_excel_keywords = len(current_action_scraped_links)
                            prog_e.progress(float(i+1)/len(keywords_from_excel_upload))
                        stat_e.success(f"Excel (keywords) processing done. Found {total_links_from_excel_keywords} unique links.")
                    else: st.warning("No valid keywords found in the uploaded Excel file.")
                elif file_name.endswith(('.txt', '.csv')):
                    st.info("Processing TXT/CSV file: Assuming direct links for validation...")
                    links_from_uploaded_file = load_links_from_file(uploaded_file_links_or_keywords)
                    if links_from_uploaded_file:
                        valid_format_links_file = {link for link in links_from_uploaded_file if link.startswith(WHATSAPP_DOMAIN)}
                        invalid_format_count_file = len(links_from_uploaded_file) - len(valid_format_links_file)
                        if invalid_format_count_file > 0:
                             st.warning(f"Skipped {invalid_format_count_file} lines from file that do not look like WhatsApp links or are not http/https.")
                        current_action_scraped_links.update(valid_format_links_file)
                        if not valid_format_links_file: st.warning("No valid WhatsApp link formats found in the uploaded file.")
                    else: st.warning("No processable links found in the uploaded TXT/CSV file.")
                else: st.error("Unsupported file type. Please upload .txt, .csv, or .xlsx.")
    except Exception as e_input_scrape:
        st.error(f"An error occurred during the input/scraping phase: {e_input_scrape}", icon="💥")

    normalized_current_action_links = set()
    for link in current_action_scraped_links:
        try:
            if isinstance(link, str): # Ensure link is a string
                parsed = urlparse(link)
                normalized_current_action_links.add(f"{parsed.scheme}://{parsed.netloc}{parsed.path}")
        except Exception: 
            if isinstance(link, str): normalized_current_action_links.add(link)

    links_to_validate_now = list(normalized_current_action_links - st.session_state.processed_links_in_session)
    
    if links_to_validate_now:
        st.success(f"Found {len(current_action_scraped_links)} unique links from this action. "
                   f"Validating {len(links_to_validate_now)} new links...")
        prog_val, stat_val = st.progress(0.0), st.empty()
        new_results_this_run = []
        
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            future_to_link = {executor.submit(validate_link, link_to_val): link_to_val for link_to_val in links_to_validate_now}
            for i, future in enumerate(as_completed(future_to_link)):
                original_link_for_validation = future_to_link[future]
                try:
                    result_validated = future.result()
                    new_results_this_run.append(result_validated)
                    st.session_state.processed_links_in_session.add(original_link_for_validation)
                except Exception as val_exc:
                    st.warning(f"Critical error during validation task for {str(original_link_for_validation)[:40]}...: {val_exc}", icon="⚠️")
                    st.session_state.processed_links_in_session.add(original_link_for_validation)
                    new_results_this_run.append({
                        "Group Name": "Validation Task Error", 
                        "Group Link": original_link_for_validation, 
                        "Logo URL": "", 
                        "Status": f"Validation Failed ({type(val_exc).__name__})"
                    })
                prog_val.progress(float(i+1)/len(links_to_validate_now))
                stat_val.text(f"Validated {i+1}/{len(links_to_validate_now)} links...")
        
        if new_results_this_run:
            st.session_state.results.extend(new_results_this_run)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
    elif current_action_scraped_links and not links_to_validate_now:
         st.info("No *new* WhatsApp links found from this action that require validation. All were previously processed or yielded no new links.")

    if 'results' in st.session_state and st.session_state.results:
        unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = unique_results_df.to_dict('records')
        df_display_master = unique_results_df.reset_index(drop=True)

        active_statuses = ['Active', 'Active (Action Button Present)'] # Main active categories
        active_df_all_master = df_display_master[df_display_master['Status'].isin(active_statuses)].copy()
        
        expired_statuses = ['Expired/Full', 'Expired (404 Not Found)', 'Expired (No Name/Logo)']
        expired_df_master = df_display_master[df_display_master['Status'].isin(expired_statuses)].copy()
        
        # Other/Error statuses
        other_error_statuses = ~df_display_master['Status'].isin(active_statuses + expired_statuses)
        error_df_master = df_display_master[other_error_statuses].copy()

        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master)}</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df_all_master)}</div></div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="metric-card">Expired Links<br><div class="metric-value">{len(expired_df_master)}</div></div>', unsafe_allow_html=True) # Combined expired
        col4.markdown(f'<div class="metric-card">Other/Error<br><div class="metric-value">{len(error_df_master)}</div></div>', unsafe_allow_html=True)

        st.subheader("✨ Active Groups Display (Styled Table)")
        with st.expander("View and Filter Active Groups", expanded=True):
            if not active_df_all_master.empty:
                st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                st.markdown("#### Filter Displayed Active Groups:")
                
                name_keywords_styled_input = st.text_input(
                    "Filter by Group Name Keywords (comma-separated, case-insensitive):",
                    value=st.session_state.styled_table_name_keywords,
                    key="styled_table_name_keywords_input_key_v2", # Ensure unique key
                    placeholder="e.g., study, fun, tech",
                    help="Enter keywords (comma-separated). Shows groups matching ANY keyword."
                ).strip()

                active_df_for_styled_table = active_df_all_master.copy()
                
                # Apply name filter immediately for available_for_limit calculation
                if name_keywords_styled_input: # Use the current input value, not session state yet for this part
                    keywords_list_current = [kw.strip().lower() for kw in name_keywords_styled_input.split(',') if kw.strip()]
                    if keywords_list_current:
                        regex_pattern_current = '|'.join(map(re.escape, keywords_list_current))
                        active_df_for_styled_table = active_df_for_styled_table[
                            active_df_for_styled_table['Group Name'].astype(str).str.lower().str.contains(regex_pattern_current, na=False, regex=True)
                        ]
                
                available_for_limit = len(active_df_for_styled_table)
                
                if name_keywords_styled_input != st.session_state.styled_table_name_keywords:
                    st.session_state.styled_table_name_keywords = name_keywords_styled_input
                    st.session_state.styled_table_limit_active = False 
                    # Default limit needs to be re-evaluated based on new `available_for_limit`
                    st.session_state.styled_table_current_limit_value = min(available_for_limit if available_for_limit > 0 else 50, 50)
                    st.rerun() # Rerun to apply filter and reset limit properly

                if available_for_limit == 0:
                    st.markdown('</div>', unsafe_allow_html=True)
                    st.markdown(generate_styled_html_table(pd.DataFrame()), unsafe_allow_html=True) # Pass empty DF
                else:
                    if not st.session_state.styled_table_limit_active:
                        st.session_state.styled_table_current_limit_value = min(available_for_limit, 50) 

                    limit_value_input = st.number_input(
                        "Max Groups to Display in Table:",
                        min_value=1,
                        max_value=max(1, available_for_limit),
                        value=min(st.session_state.styled_table_current_limit_value, max(1, available_for_limit)),
                        step=10,
                        key="styled_table_limit_input_key_v2", # Ensure unique key
                        help=f"Set max groups to show. Available after name filter: {available_for_limit}"
                    )
                    st.markdown('</div>', unsafe_allow_html=True)

                    if limit_value_input != st.session_state.styled_table_current_limit_value:
                        st.session_state.styled_table_current_limit_value = limit_value_input
                        st.session_state.styled_table_limit_active = True
                        st.rerun()
                    
                    # Final DF for styled table after name and limit application
                    # Re-apply name filter using session state to ensure consistency after potential reruns
                    final_df_for_table = active_df_all_master.copy()
                    if st.session_state.styled_table_name_keywords:
                        keywords_list_session = [kw.strip().lower() for kw in st.session_state.styled_table_name_keywords.split(',') if kw.strip()]
                        if keywords_list_session:
                            regex_pattern_session = '|'.join(map(re.escape, keywords_list_session))
                            final_df_for_table = final_df_for_table[
                                final_df_for_table['Group Name'].astype(str).str.lower().str.contains(regex_pattern_session, na=False, regex=True)
                            ]
                    
                    final_df_for_table = final_df_for_table.head(st.session_state.styled_table_current_limit_value)
                    html_out = generate_styled_html_table(final_df_for_table)
                    st.markdown(html_out, unsafe_allow_html=True)
                    st.markdown("---")
                    st.text_area("Copy Raw HTML Code (for above table):", value=html_out, height=150, key="styled_html_export_area_key_main_v2", help="Ctrl+A, Ctrl+C to copy the HTML for embedding.")
            else:
                st.info("No active groups found yet to display here. Try scraping or validating some links.")
        
        with st.expander("🔬 Advanced Filtering for Downloads & Full Data Analysis", expanded=False):
            st.markdown('<div class="filter-container" style="border-style:solid;">', unsafe_allow_html=True)
            st.markdown("#### Filter Full Dataset (for Download/Analysis):")
            
            all_statuses_master = sorted(list(df_display_master['Status'].astype(str).unique()))
            selected_adv_statuses = st.multiselect(
                "Filter by Status:", options=all_statuses_master,
                default=st.session_state.adv_filter_status, key="adv_status_filter_multiselect_key_main_v2"
            )
            if selected_adv_statuses != st.session_state.adv_filter_status:
                st.session_state.adv_filter_status = selected_adv_statuses
                st.rerun()

            adv_name_keywords_input = st.text_input(
                "Filter by Group Name Keywords (comma-separated, case-insensitive):", 
                value=st.session_state.adv_filter_name_keywords,
                key="adv_name_keyword_filter_input_key_main_v2", 
                placeholder="e.g., news, jobs, global",
                help="Applies to the entire dataset for download/analysis. Comma-separated."
            ).strip()
            if adv_name_keywords_input != st.session_state.adv_filter_name_keywords:
                st.session_state.adv_filter_name_keywords = adv_name_keywords_input
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

            df_for_adv_download_or_view = df_display_master.copy()
            adv_filters_applied_flag = False
            
            if st.session_state.adv_filter_status:
                df_for_adv_download_or_view = df_for_adv_download_or_view[df_for_adv_download_or_view['Status'].isin(st.session_state.adv_filter_status)]
                adv_filters_applied_flag = True
            
            if st.session_state.adv_filter_name_keywords:
                adv_keywords_list = [kw.strip().lower() for kw in st.session_state.adv_filter_name_keywords.split(',') if kw.strip()]
                if adv_keywords_list:
                    adv_regex_pattern = '|'.join(map(re.escape, adv_keywords_list))
                    df_for_adv_download_or_view = df_for_adv_download_or_view[
                        df_for_adv_download_or_view['Group Name'].astype(str).str.lower().str.contains(adv_regex_pattern, na=False, regex=True)
                    ]
                    adv_filters_applied_flag = True
            
            st.markdown(f"**Preview of Data for Download/Analysis ({'Filtered' if adv_filters_applied_flag else 'All Processed'} - {len(df_for_adv_download_or_view)} rows):**")
            st.dataframe(df_for_adv_download_or_view, column_config={
                "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Open Link", width="medium"),
                "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View Logo", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small")
            }, hide_index=True, height=300, use_container_width=True, key="adv_dataframe_preview_v2")

        st.subheader("📥 Download Results (CSV)")
        dl_col1, dl_col2 = st.columns(2)
        
        if not active_df_all_master.empty:
            dl_col1.download_button(
                label=f"Download All Active Groups ({len(active_df_all_master)} CSV)", 
                data=active_df_all_master.to_csv(index=False).encode('utf-8'), 
                file_name="active_whatsapp_groups.csv", 
                mime="text/csv", 
                use_container_width=True, 
                key="dl_active_csv_main_key_btn_v2"
            )
        else:
            dl_col1.button("Download All Active Groups (CSV)", disabled=True, use_container_width=True, help="No active groups available to download.")

        if not df_for_adv_download_or_view.empty:
            download_label_adv = f"Filtered Processed Results ({len(df_for_adv_download_or_view)} CSV)" if adv_filters_applied_flag else f"All Processed Results ({len(df_for_adv_download_or_view)} CSV)"
            dl_col2.download_button(
                label=download_label_adv, 
                data=df_for_adv_download_or_view.to_csv(index=False).encode('utf-8'), 
                file_name="filtered_processed_whatsapp_results.csv" if adv_filters_applied_flag else "all_processed_whatsapp_results.csv", 
                mime="text/csv", 
                use_container_width=True, 
                key="dl_all_or_filtered_csv_key_btn_v2"
            )
        elif not df_display_master.empty() and df_for_adv_download_or_view.empty() and adv_filters_applied_flag:
             dl_col2.button("No Results Match Advanced Filters", disabled=True, use_container_width=True)
        else:
            dl_col2.button("Download Processed Results (CSV)", disabled=True, use_container_width=True, help="No processed results available to download.")
            
    else:
        st.info("Start by searching, scraping, entering, or uploading links to see results and enable download options!", icon="ℹ️")

if __name__ == "__main__":
    main()
