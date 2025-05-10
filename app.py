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
    def google_search_function_actual(query, num_results, lang, **kwargs):
        st.error("`googlesearch-python` library not found. Cannot perform Google searches.")
        return []

# --- Import Fake User Agent Library ---
try:
    from fake_useragent import UserAgent
    from fake_useragent.errors import FakeUserAgentError # Import specific error
    ua_general = UserAgent()
    def get_random_headers_general():
        try:
            return {
                "User-Agent": ua_general.random,
                "Accept-Language": "en-US,en;q=0.9"
            }
        except FakeUserAgentError: # Catch specific error if ua_general.random fails
             st.warning("fake-useragent failed to get a User-Agent (data update issue?). Using fallback.", icon="⚠️")
             return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception as e_random: # Catch other errors from ua_general.random
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
except FakeUserAgentError as e_init: # Catch specific error from UserAgent() constructor
     st.warning(f"Error initializing fake-useragent (data update issue?): {e_init}. Using default User-Agent.", icon="⚠️")
     def get_random_headers_general():
         return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except Exception as e_general_init: # Catch other errors from UserAgent() constructor
     st.warning(f"Error initializing fake-useragent: {e_general_init}. Using default User-Agent.", icon="⚠️")
     def get_random_headers_general():
         return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

# --- Streamlit Configuration & Constants ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator Pro",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
UNNAMED_GROUP_PLACEHOLDER = "Unnamed Group"
# IMAGE_PATTERN_PPS = re.compile(r'https:\/\/pps\.whatsapp\.net\/v\/t\d+\/[-\w]+\/\d+\.jpg\?') # Defined but not directly used, can be removed if not used
OG_IMAGE_PATTERN = re.compile(r'https?:\/\/[^\/\s]+\/[^\/\s]+\.(jpg|jpeg|png)(\?[^\s]*)?')
MAX_VALIDATION_WORKERS = 10
DEFAULT_CRAWL_PAGES = 200
MAX_CRAWL_PAGES_LIMIT = 2000


# --- Custom CSS ---
st.markdown("""
<style>
body { font-family: 'Roboto', 'Arial', sans-serif; }
.main-title { font-size: 2.8em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: 700; letter-spacing: -1px; }
.subtitle { font-size: 1.3em; color: #555; text-align: center; margin-top: 5px; margin-bottom: 30px; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 18px; margin: 8px 0; transition: background-color 0.3s ease, transform 0.1s ease; }
.stButton>button:hover { background-color: #1EBE5A; transform: scale(1.03); }
.stButton>button:active { transform: scale(0.98); }
.stProgress > div > div > div > div { background-color: #25D366; border-radius: 4px; }
.metric-card { background-color: #F8F9FA; padding: 15px; border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); color: #333; text-align: center; margin-bottom: 15px; border: 1px solid #E9ECEF; }
.metric-card .metric-value { font-size: 2em; font-weight: 700; margin-top: 5px; margin-bottom: 0; line-height: 1.2; color: #25D366; }
.stTextInput > div > div > input, .stTextArea > div > textarea, .stNumberInput > div > div > input, .stMultiSelect > div[data-baseweb="select"] > div { border: 1px solid #CED4DA !important; border-radius: 6px !important; padding: 10px !important; box-shadow: inset 0 1px 2px rgba(0,0,0,0.075); }
.stTextInput > div > div > input:focus, .stTextArea > div > textarea:focus, .stNumberInput > div > div > input:focus, .stMultiSelect > div[data-baseweb="select"] > div:focus-within { border-color: #25D366 !important; box-shadow: 0 0 0 0.2rem rgba(37, 211, 102, 0.25) !important; }
.st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F8F9FA; border-radius: 6px; }
.stExpander { border: 1px solid #E9ECEF; border-radius: 8px; padding: 12px; margin-top: 15px; margin-bottom: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; font-size: 1.2em; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #1EBE5A; font-size: 1.1em; }

.filter-container { background-color: #FFFFFF; padding: 20px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #E0E0E0; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }
.filter-container .stTextInput input, .filter-container .stNumberInput input, .filter-container .stMultiSelect div[data-baseweb="select"] > div { background-color: #FDFDFD; }
h4.filter-title { color: #259952; margin-top:0px; margin-bottom:15px; border-left: 4px solid #25D366; padding-left: 10px; font-size: 1.1em; font-weight:600;}

.whatsapp-groups-table { border-collapse: collapse; width: 100%; margin-top: 15px; box-shadow: 0 3px 6px rgba(0,0,0,0.08); border-radius: 8px; overflow: hidden; border: 1px solid #DEE2E6; }
.whatsapp-groups-table caption { caption-side: top; text-align: left; font-weight: 600; padding: 12px 15px; font-size: 1.15em; color: #343A40; background-color: #F8F9FA; border-bottom: 1px solid #DEE2E6;}
.whatsapp-groups-table th { background-color: #343A40; color: white; padding: 14px 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; font-size: 0.9em; }
.whatsapp-groups-table th:nth-child(1) { text-align: center; width: 80px; }
.whatsapp-groups-table th:nth-child(2) { text-align: left; }
.whatsapp-groups-table th:nth-child(3) { text-align: right; width: 150px; }
.whatsapp-groups-table tr { border-bottom: 1px solid #EAEEF2; }
.whatsapp-groups-table tr:last-child { border-bottom: none; }
.whatsapp-groups-table tr:nth-child(even) { background-color: #F9FAFB; }
.whatsapp-groups-table tr:hover { background-color: #E9F5FF; }
.whatsapp-groups-table td { padding: 12px; vertical-align: middle; text-align: left; font-size: 0.95em; }
.whatsapp-groups-table td:nth-child(1) { width: 60px; padding-right: 8px; text-align: center; }
.whatsapp-groups-table td:nth-child(2) { padding-left: 8px; padding-right: 12px; word-break: break-word; font-weight: 500; color: #212529; }
.whatsapp-groups-table td:nth-child(3) { width: 140px; text-align: right; padding-left: 12px; }
.group-logo-img { width: 45px; height: 45px; border-radius: 50%; object-fit: cover; display: block; margin: 0 auto; border: 2px solid #F0F0F0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
.join-button { display: inline-block; background-color: #25D366; color: #FFFFFF !important; padding: 7px 14px; border-radius: 6px; text-decoration: none; font-weight: 500; text-align: center; white-space: nowrap; font-size: 0.85em; transition: background-color 0.2s ease, transform 0.1s ease; }
.join-button:hover { background-color: #1DB954; color: #FFFFFF !important; text-decoration: none; transform: translateY(-1px); }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); white-space: nowrap; border-width: 0; }
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

@st.cache_data
def load_keywords_from_excel_cached(uploaded_file_bytes):
    if uploaded_file_bytes is None: return []
    try:
        df = pd.read_excel(io.BytesIO(uploaded_file_bytes), engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        keywords = [kw.strip() for kw in df.iloc[:, 0].dropna().astype(str).tolist() if len(kw.strip()) > 1]
        if not keywords: st.warning("No valid keywords found in the first column of the Excel file.")
        return keywords
    except Exception as e:
        st.error(f"Error reading Excel: {e}. Ensure 'openpyxl' is installed.", icon="❌")
        return []

@st.cache_data
def load_links_from_file_cached(uploaded_file_bytes, filename):
    if uploaded_file_bytes is None: return []
    try:
        content = uploaded_file_bytes
        text_content = None
        for encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                text_content = content.decode(encoding)
                break
            except UnicodeDecodeError: continue
        if text_content is None:
             st.error(f"Could not decode file {filename}.", icon="❌"); return []

        if filename.endswith('.csv'):
            try:
                 df = pd.read_csv(io.StringIO(text_content))
                 if df.empty: st.warning("CSV file is empty."); return []
                 return [link.strip() for link in df.iloc[:, 0].dropna().astype(str).tolist() if link.strip().startswith(('http://', 'https://'))]
            except Exception as e:
                 st.error(f"Error reading CSV: {e}.", icon="❌"); return []
        else: # Assume TXT
             return [line.strip() for line in text_content.splitlines() if line.strip()]
    except Exception as e:
        st.error(f"Error processing file {filename}: {e}", icon="❌"); return []

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
            for img_tag in soup.find_all('img', src=True): # Renamed to avoid conflict
                src = html.unescape(img_tag['src'])
                if src.startswith('https://pps.whatsapp.net/'):
                    result["Logo URL"] = src; logo_found = True; break
        
        if result["Status"] == "Error": # Default to Active if no other status set
            result["Status"] = "Active"
        elif result["Status"] == "Expired" and (group_name_found or logo_found): # If marked expired but details found
            if soup.find('a', attrs={'id': 'action-button', 'href': link}): # Check for join button
                 result["Status"] = "Active"

    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError: result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: # Catch any other exception during parsing
        # Log the actual error for debugging if needed, e.g., print(f"Parsing exception for {link}: {e}")
        result["Status"] = f"Parsing Error ({type(e).__name__})" # This is where your NameError was reported, not caused.
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
                        newly_found_count +=1
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

def crawl_website(start_url, max_depth=2, max_pages=DEFAULT_CRAWL_PAGES):
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
    with requests.Session() as session, st.spinner(f"Crawling {base_domain}... (Max pages: {max_pages})"):
        while queue_list and page_count < max_pages:
            if len(queue_list) > max_q_size:
                 st.sidebar.warning(f"Crawl queue exceeded {max_q_size}. Stopping discovery phase.", icon="❗️"); queue_list = queue_list[:max_q_size]
            current_url, depth = queue_list.pop(0)
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')
            if normalized_current_url in visited_urls or depth > max_depth: continue
            visited_urls.add(normalized_current_url)
            if page_count >= max_pages: break
            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}/{max_pages}, Q:{len(queue_list)}): {current_url[:50]}...")
            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=12)
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type:
                    st.sidebar.info(f"Skipping non-HTML: {current_url[:40]} ({content_type})")
                    continue
                page_count += 1
                wa_links_from_page = scrape_whatsapp_links_from_page(current_url, session=session)
                newly_found_count = 0
                for link in wa_links_from_page:
                    if link.startswith(WHATSAPP_DOMAIN) and link not in scraped_whatsapp_links:
                        scraped_whatsapp_links.add(link)
                        newly_found_count +=1
                if newly_found_count > 0:
                    st.sidebar.info(f"Crawl: Found {newly_found_count} new WA links on {current_url[:30]}...")

                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag_crawl in soup.find_all('a', href=True): # Renamed to avoid conflict
                        href = link_tag_crawl.get('href')
                        if href:
                            abs_url = urljoin(current_url, href)
                            parsed_abs_url = urlparse(abs_url)
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain and \
                               not parsed_abs_url.fragment:
                                normalized_abs_url = urljoin(abs_url, parsed_abs_url.path or '/')
                                if normalized_abs_url not in visited_urls and (abs_url, depth + 1) not in urls_in_queue_tuples:
                                     if len(queue_list) < max_q_size:
                                        queue_list.append((abs_url, depth + 1)); urls_in_queue_tuples.add((abs_url, depth + 1))
                                     else:
                                        st.sidebar.warning("Crawl queue full, not adding more URLs.", icon="🈵")
                                        break 
            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl Req Err ({type(e).__name__}): {current_url[:50]}...", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl Parse Err ({type(e).__name__}): {current_url[:50]}...", icon="💥")
    st.sidebar.success(f"Crawl finished. Visited {page_count} pages, found {len(scraped_whatsapp_links)} unique WhatsApp links.")
    if page_count >= max_pages: st.sidebar.warning(f"Crawl stopped at max pages: {max_pages}.", icon="❗️")
    return scraped_whatsapp_links

def generate_styled_html_table(data_df_for_table):
    df_to_display = data_df_for_table[data_df_for_table['Group Name'] != UNNAMED_GROUP_PLACEHOLDER].copy()
    if df_to_display.empty:
        return "<p style='text-align:center; color:#777; margin-top:20px;'><i>No groups match the current display filters. Try adjusting them or check if names are available.</i></p>"
    
    caption_id = "active-groups-table-caption"
    html_string = f'<table class="whatsapp-groups-table" aria-label="List of Active WhatsApp Groups" aria-describedby="{caption_id}">'
    html_string += f'<caption id="{caption_id}">Filtered Active WhatsApp Groups ({len(df_to_display)} shown)</caption>'
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
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img" loading="lazy" width="45" height="45">'
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
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator Pro 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, Scrape, Validate, and Manage WhatsApp Group Links with Powerful, Independent Filters.</p>', unsafe_allow_html=True)

    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()
    if 'styled_table_name_keywords' not in st.session_state: st.session_state.styled_table_name_keywords = ""
    if 'styled_table_current_limit' not in st.session_state: st.session_state.styled_table_current_limit = 0
    if 'adv_filter_status' not in st.session_state: st.session_state.adv_filter_status = []
    if 'adv_filter_name_keywords' not in st.session_state: st.session_state.adv_filter_name_keywords = ""

    if not isinstance(st.session_state.processed_links_in_session, set): st.session_state.processed_links_in_session = set()
    if isinstance(st.session_state.results, list):
        for res_item in st.session_state.results:
            if isinstance(res_item, dict) and 'Group Link' in res_item and res_item['Group Link']:
                try:
                    parsed_link = urlparse(res_item['Group Link'])
                    normalized_link = f"{parsed_link.scheme}://{parsed_link.netloc}{parsed_link.path}"
                    st.session_state.processed_links_in_session.add(normalized_link)
                except Exception: st.session_state.processed_links_in_session.add(res_item['Group Link'])

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV/Excel)"
        ], key="input_method_main_select_key")

        gs_top_n = 5
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            gs_top_n = st.slider("Google Results to Scrape (per keyword)", 1, 20, 5, key="gs_top_n_slider_key", help="Number of Google search result pages to analyze per keyword.")
        
        crawl_depth_default, crawl_pages_default = 2, DEFAULT_CRAWL_PAGES
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning(f"⚠️ Extensive crawl can be slow. Max {MAX_CRAWL_PAGES_LIMIT} pages. Use with caution.", icon="🚨")
            crawl_depth_default = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider_key")
            crawl_pages_default = st.slider("Max Pages to Crawl", 1, MAX_CRAWL_PAGES_LIMIT, DEFAULT_CRAWL_PAGES, step=50, key="crawl_pages_slider_key") # Increased step
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button_key"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.session_state.styled_table_name_keywords = ""
            st.session_state.styled_table_current_limit = 0 
            st.session_state.adv_filter_status = []
            st.session_state.adv_filter_name_keywords = ""
            st.cache_data.clear(); st.success("Results & filters cleared!"); st.rerun()

    current_action_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input_key")
            if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button_key"):
                if query: current_action_scraped_links.update(google_search_and_scrape(query, gs_top_n))
                else: st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            file_gs_bulk = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload_key")
            if file_gs_bulk and st.button("Process Excel, Scrape & Validate", use_container_width=True, key="gs_bulk_button_key"):
                keywords = load_keywords_from_excel_cached(file_gs_bulk.getvalue())
                if keywords:
                    st.info(f"Processing {len(keywords)} keywords...")
                    prog_b_gs, stat_b_gs = st.progress(0), st.empty() # Unique names
                    total_l_gs = 0
                    for i_gs, kw_gs in enumerate(keywords): # Unique iterators
                        stat_b_gs.text(f"Keyword: '{kw_gs}' ({i_gs+1}/{len(keywords)}). Total links: {total_l_gs}")
                        links_from_kw = google_search_and_scrape(kw_gs, gs_top_n)
                        current_action_scraped_links.update(links_from_kw)
                        total_l_gs = len(current_action_scraped_links)
                        prog_b_gs.progress((i_gs+1)/len(keywords))
                    stat_b_gs.success(f"Bulk Google Search done. Found {total_l_gs} links.")
                else: st.warning("No valid keywords in Excel.")

        elif input_method == "Scrape from Specific Webpage URL":
            url_specific = st.text_input("Webpage URL:", placeholder="https://example.com/page", key="specific_url_input_key") # Unique name
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button_key"):
                if url_specific and (url_specific.startswith("http://") or url_specific.startswith("https://")):
                    with st.spinner(f"Scraping {url_specific}..."): current_action_scraped_links.update(scrape_whatsapp_links_from_page(url_specific))
                    st.success(f"Scraping done. Found {len(current_action_scraped_links)} links from {url_specific}.")
                else: st.warning("Please enter a valid URL.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_crawl = st.text_input("Base Domain URL:", placeholder="example.com", key="crawl_domain_input_key") # Unique name
            if st.button("Crawl & Scrape", use_container_width=True, key="crawl_button_key"):
                if domain_crawl:
                    st.info(f"Starting crawl of '{domain_crawl}'. Max pages: {crawl_pages_default}, Depth: {crawl_depth_default}. Progress in sidebar.")
                    current_action_scraped_links.update(crawl_website(domain_crawl, crawl_depth_default, crawl_pages_default))
                    st.success(f"Crawl done. Found {len(current_action_scraped_links)} links from {domain_crawl}.")
                else: st.warning("Please enter a domain.")

        elif input_method == "Enter Links Manually (for Validation)":
            text_manual = st.text_area("WhatsApp Links (one per line):", height=150, key="manual_links_area_key") # Unique name
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button_key"):
                links_manual = [line.strip() for line in text_manual.split('\n') if line.strip()] # Unique name
                if links_manual:
                    valid_links_manual = {l for l in links_manual if l.startswith(WHATSAPP_DOMAIN)} # Unique name
                    if len(valid_links_manual) < len(links_manual): st.warning(f"Skipped {len(links_manual)-len(valid_links_manual)} non-WhatsApp links.")
                    current_action_scraped_links.update(valid_links_manual)
                    if not valid_links_manual: st.warning("No valid WhatsApp link formats found.")
                else: st.warning("Please enter links.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file_upload_multi = st.file_uploader("Upload TXT/CSV (links) or Excel (keywords)", type=["txt", "csv", "xlsx"], key="upload_file_input_key") # Unique name
            if file_upload_multi and st.button("Process File", use_container_width=True, key="upload_process_button_key"):
                file_bytes_multi = file_upload_multi.getvalue() # Unique name
                filename_multi = file_upload_multi.name # Unique name
                if filename_multi.endswith('.xlsx'):
                    keywords_excel_multi = load_keywords_from_excel_cached(file_bytes_multi) # Unique name
                    if keywords_excel_multi:
                        st.info(f"Processing {len(keywords_excel_multi)} keywords from Excel...")
                        prog_e_multi, stat_e_multi = st.progress(0), st.empty() # Unique names
                        total_le_multi = 0
                        for i_multi, kw_multi in enumerate(keywords_excel_multi): # Unique iterators
                            stat_e_multi.text(f"Keyword: {kw_multi} ({i_multi+1}/{len(keywords_excel_multi)}). Links: {total_le_multi}")
                            links_from_kw_multi = google_search_and_scrape(kw_multi, gs_top_n) # Unique name
                            current_action_scraped_links.update(links_from_kw_multi)
                            total_le_multi = len(current_action_scraped_links)
                            prog_e_multi.progress((i_multi+1)/len(keywords_excel_multi))
                        stat_e_multi.success(f"Excel (keywords) processing done. Found {total_le_multi} links.")
                    else: st.warning("No keywords in Excel.")
                elif filename_multi.endswith(('.txt', '.csv')):
                    links_file_multi = load_links_from_file_cached(file_bytes_multi, filename_multi) # Unique name
                    if links_file_multi:
                        valid_links_file_multi = {l for l in links_file_multi if l.startswith(WHATSAPP_DOMAIN)} # Unique name
                        if len(valid_links_file_multi) < len(links_file_multi): st.warning(f"Skipped {len(links_file_multi)-len(valid_links_file_multi)} non-WhatsApp links.")
                        current_action_scraped_links.update(valid_links_file_multi)
                        if not valid_links_file_multi: st.warning("No valid WhatsApp link formats found in file.")
                        else: st.success(f"Loaded {len(valid_links_file_multi)} WhatsApp links from file.")
                    else: st.warning("No links in file.")
                else: st.warning("Unsupported file. Use .txt, .csv, or .xlsx.")
    except Exception as e_input: st.error(f"Input/Scraping Error: {e_input}", icon="💥")


    links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)
    if links_to_validate_now:
        st.success(f"Found {len(current_action_scraped_links)} links from this action. Validating {len(links_to_validate_now)} new (unprocessed) links...")
        prog_val, stat_val = st.progress(0), st.empty()
        new_results_this_run = []
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
            for i_val, future_val in enumerate(as_completed(future_to_link)): # Unique iterators
                link_validated = future_to_link[future_val]
                try:
                    result_validated = future_val.result()
                    new_results_this_run.append(result_validated)
                    parsed_url_val = urlparse(link_validated)
                    normalized_link_val = f"{parsed_url_val.scheme}://{parsed_url_val.netloc}{parsed_url_val.path}"
                    st.session_state.processed_links_in_session.add(normalized_link_val)
                except Exception as val_exc:
                    st.warning(f"Error validating {link_validated[:40]}...: {val_exc}", icon="⚠️")
                    # Still add to processed to avoid re-validating a problematic link
                    parsed_url_val_err = urlparse(link_validated)
                    normalized_link_val_err = f"{parsed_url_val_err.scheme}://{parsed_url_val_err.netloc}{parsed_url_val_err.path}"
                    st.session_state.processed_links_in_session.add(normalized_link_val_err)
                    new_results_this_run.append({"Group Name": "Validation Error", "Group Link": link_validated, "Logo URL": "", "Status": f"Validation Failed ({type(val_exc).__name__})"})

                prog_val.progress((i_val+1)/len(links_to_validate_now))
                stat_val.text(f"Validated {i_val+1}/{len(links_to_validate_now)} new links: {link_validated[:50]}...")
        
        if new_results_this_run:
            st.session_state.results.extend(new_results_this_run)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
        # Reset styled table limit to "all" to ensure newly added active groups are considered for display
        st.session_state.styled_table_current_limit = 0 
    elif current_action_scraped_links and not links_to_validate_now:
         st.info("No *new* WhatsApp links found from this action. All were previously processed in this session.")


    # --- Results Display Section ---
    if 'results' in st.session_state and st.session_state.results:
        unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = unique_results_df.to_dict('records')
        df_display_master = unique_results_df.reset_index(drop=True)

        active_df_all_master = df_display_master[df_display_master['Status'].str.contains('Active', na=False)].copy()
        expired_df_master = df_display_master[df_display_master['Status'] == 'Expired'].copy()
        error_df_master = df_display_master[~df_display_master['Status'].str.contains('Active', na=False) & (df_display_master['Status'] != 'Expired')].copy()

        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display_master)}</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df_all_master)}</div></div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="metric-card">Expired Links<br><div class="metric-value">{len(expired_df_master)}</div></div>', unsafe_allow_html=True)
        col4.markdown(f'<div class="metric-card">Other Status<br><div class="metric-value">{len(error_df_master)}</div></div>', unsafe_allow_html=True)

        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown('<div role="region" aria-labelledby="styled-table-heading">', unsafe_allow_html=True)
        st.subheader("✨ Active Groups Display (Styled Table)")
        st.markdown('<h3 id="styled-table-heading" class="sr-only">Active Groups Table and Filters</h3>', unsafe_allow_html=True)

        with st.expander("View and Filter Active Groups", expanded=True):
            if not active_df_all_master.empty:
                st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                st.markdown("<h4 class='filter-title'>Filter Displayed Active Groups:</h4>", unsafe_allow_html=True)
                
                # Name Keyword Filter for Styled Table
                # Use a new session state variable if the previous one caused issues, or ensure it's reset correctly.
                name_keywords_styled_input_val = st.text_input(
                    "Filter by Group Name Keywords (comma-separated, OR logic):",
                    value=st.session_state.styled_table_name_keywords, # Use the correctly initialized session state
                    key="styled_table_name_keywords_input_unique_key", 
                    placeholder="e.g., study, fun, tech",
                    help="Enter keywords (e.g., 'tech, news'). Shows groups matching ANY keyword."
                ).strip()

                # Update session state only if the input actually changed
                if name_keywords_styled_input_val != st.session_state.styled_table_name_keywords:
                    st.session_state.styled_table_name_keywords = name_keywords_styled_input_val
                    st.session_state.styled_table_current_limit = 0 # Reset limit to "all" when name filter changes
                    st.rerun() # Rerun to apply name filter and update limit options

                # --- Apply name filter for styled table ---
                active_df_for_styled_table_filtered_by_name = active_df_all_master.copy()
                if st.session_state.styled_table_name_keywords:
                    keywords_list_styled = [kw.strip().lower() for kw in st.session_state.styled_table_name_keywords.split(',') if kw.strip()]
                    if keywords_list_styled:
                        try:
                            regex_pattern_styled = '|'.join(map(re.escape, keywords_list_styled))
                            active_df_for_styled_table_filtered_by_name = active_df_for_styled_table_filtered_by_name[
                                active_df_for_styled_table_filtered_by_name['Group Name'].str.contains(regex_pattern_styled, case=False, na=False, regex=True)
                            ]
                        except re.error as e_re_styled:
                            st.warning(f"Invalid characters in styled table name filter: {e_re_styled}. Using previous filter or no filter.", icon="⚠️")
                            # Fallback: use the dataframe before this problematic regex was applied
                            # This depends on how you want to handle it, for now, it might just use the previous valid filter state.
                
                available_for_limit_styled = len(active_df_for_styled_table_filtered_by_name)
                
                # Limit Filter for Styled Table
                if available_for_limit_styled > 0:
                    # If current_limit is 0 (meaning "all" or uninitialized for this filter set), default to showing all available
                    current_display_limit_val = available_for_limit_styled if st.session_state.styled_table_current_limit == 0 else st.session_state.styled_table_current_limit
                    
                    limit_value_styled_input = st.number_input(
                        "Max Groups to Display in Table (0 for all available):",
                        min_value=0,
                        max_value=available_for_limit_styled,
                        value=min(current_display_limit_val, available_for_limit_styled), # Ensure value is within new bounds
                        step=10,
                        key=f"styled_table_limit_input_unique_key", # Ensure this key is stable or managed with a counter if needed
                        help=f"Set max groups. 0 shows all. Available after name filter: {available_for_limit_styled}"
                    )
                    if limit_value_styled_input != st.session_state.styled_table_current_limit:
                        st.session_state.styled_table_current_limit = limit_value_styled_input
                        st.rerun()
                else: # No groups available after name filter, or no active groups at all
                    st.session_state.styled_table_current_limit = 0 # Ensure limit is 0

                st.markdown('</div>', unsafe_allow_html=True) # Close filter-container

                # Final DataFrame for styled table display
                if st.session_state.styled_table_current_limit > 0 and available_for_limit_styled > 0:
                    df_to_render_in_styled_table = active_df_for_styled_table_filtered_by_name.head(st.session_state.styled_table_current_limit)
                else:
                    df_to_render_in_styled_table = active_df_for_styled_table_filtered_by_name # Show all if limit is 0 or no groups

                html_out = generate_styled_html_table(df_to_render_in_styled_table)
                st.markdown(html_out, unsafe_allow_html=True)
                
                if "<td" in html_out:
                    st.markdown("---")
                    st.text_area("Copy Raw HTML Code (above table):", value=html_out, height=150, key="styled_html_export_area_unique_key", help="Ctrl+A, Ctrl+C")
            else:
                st.info("No active groups found yet to display here.")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("<hr>", unsafe_allow_html=True)
        with st.expander("🔬 Advanced Filtering for Downloads & Data Preview (Optional)", expanded=False):
            st.markdown('<div class="filter-container" style="border-style:solid;">', unsafe_allow_html=True)
            st.markdown("<h4 class='filter-title'>Filter Full Dataset (for Download/Preview):</h4>", unsafe_allow_html=True)
            
            all_statuses_master_adv = sorted(list(df_display_master['Status'].unique())) # Use a different var name
            adv_status_input_val = st.multiselect(
                "Filter by Status:", options=all_statuses_master_adv,
                default=st.session_state.adv_filter_status, key="adv_status_filter_multiselect_unique_key"
            )
            if adv_status_input_val != st.session_state.adv_filter_status:
                st.session_state.adv_filter_status = adv_status_input_val
                st.rerun()

            adv_name_keywords_input_val = st.text_input(
                "Filter by Group Name Keywords (comma-separated, OR logic):", value=st.session_state.adv_filter_name_keywords,
                key="adv_name_keyword_filter_input_unique_key", placeholder="e.g., news, jobs, global",
                help="Applies to the entire dataset. Comma-separated."
            ).strip()
            if adv_name_keywords_input_val != st.session_state.adv_filter_name_keywords:
                st.session_state.adv_filter_name_keywords = adv_name_keywords_input_val
                st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

            df_for_adv_display_filtered = df_display_master.copy() # Start with master
            adv_filters_applied_flag_dl = False # Unique flag name
            if st.session_state.adv_filter_status:
                df_for_adv_display_filtered = df_for_adv_display_filtered[df_for_adv_display_filtered['Status'].isin(st.session_state.adv_filter_status)]
                adv_filters_applied_flag_dl = True
            if st.session_state.adv_filter_name_keywords:
                adv_keywords_list_dl = [kw.strip().lower() for kw in st.session_state.adv_filter_name_keywords.split(',') if kw.strip()]
                if adv_keywords_list_dl:
                    try:
                        adv_regex_pattern_dl = '|'.join(map(re.escape, adv_keywords_list_dl))
                        df_for_adv_display_filtered = df_for_adv_display_filtered[
                            df_for_adv_display_filtered['Group Name'].str.contains(adv_regex_pattern_dl, case=False, na=False, regex=True)
                        ]
                        adv_filters_applied_flag_dl = True
                    except re.error as e_re_adv:
                         st.warning(f"Invalid characters in advanced name filter: {e_re_adv}", icon="⚠️")
            
            st.markdown(f"**Preview of Data for Download/Analysis ({'Filtered' if adv_filters_applied_flag_dl else 'All'} - {len(df_for_adv_display_filtered)} rows):**")
            st.dataframe(df_for_adv_display_filtered, column_config={
                "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join", width="medium"),
                "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View", width="small"),
                "Status": st.column_config.TextColumn("Status", width="small")
            }, hide_index=True, height=300, use_container_width=True)

        st.markdown("<hr>", unsafe_allow_html=True)
        st.subheader("📥 Download Results (CSV)")
        dl_col1, dl_col2 = st.columns(2)
        
        if not active_df_all_master.empty:
            dl_col1.download_button("Active Groups (All) (CSV)", active_df_all_master.to_csv(index=False).encode('utf-8'), "active_groups_all.csv", "text/csv", use_container_width=True, key="dl_active_csv_main_unique_key")
        else:
            dl_col1.button("Active Groups (All) (CSV)", disabled=True, use_container_width=True, help="No active groups to download.")

        if not df_for_adv_display_filtered.empty:
            download_label_adv_csv = "All Processed Results (CSV)"
            if adv_filters_applied_flag_dl: download_label_adv_csv = f"Filtered Processed Results (CSV - {len(df_for_adv_display_filtered)} rows)"
            dl_col2.download_button(download_label_adv_csv, df_for_adv_display_filtered.to_csv(index=False).encode('utf-8'), "processed_results_custom.csv", "text/csv", use_container_width=True, key="dl_all_or_filtered_csv_unique_key")
        elif not df_display_master.empty() and df_for_adv_display_filtered.empty() and adv_filters_applied_flag_dl:
             dl_col2.button("No Results Match Advanced Filters", disabled=True, use_container_width=True)
        else:
            dl_col2.button("All Processed Results (CSV)", disabled=True, use_container_width=True, help="No results processed yet.")
            
    else:
        st.info("👋 Welcome! Start by searching, entering, or uploading links to see results here!", icon="ℹ️")

if __name__ == "__main__":
    main()
