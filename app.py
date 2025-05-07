import streamlit as st
import pandas as pd
import requests
import html # For html.escape()
from bs4 import BeautifulSoup
import re
import time # Keep for other potential uses, though not directly for Google search pause now
import io
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Import Google Search Library ---
try:
    from googlesearch import search as google_search_function_actual
except ImportError:
    st.error("The `googlesearch-python` library is not installed. Please install it: `pip install googlesearch-python`")
    # Dummy function if import fails, matches expected signature
    def google_search_function_actual(query, num_results, lang, **kwargs): # Removed pause from dummy
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
UNNAMED_GROUP_PLACEHOLDER = "Unnamed Group"
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
.metric-card .metric-value { font-size: 1.8em; font-weight: bold; margin-top: 5px; margin-bottom: 0; line-height: 1.2; }
.stTextInput > div > div > input, .stTextArea > div > textarea { border: 1px solid #25D366 !important; border-radius: 5px !important; padding: 8px !important; }
.st-emotion-cache-1v3rj08, .st-emotion-cache-gh2jqd, .streamlit-expanderHeader { background-color: #F5F6F5; }
.stExpander { border: 1px solid #E0E0E0; border-radius: 5px; padding: 10px; margin-top: 10px; margin-bottom: 10px; }
.stExpander div[data-testid="stExpanderToggleIcon"] { color: #25D366; }
.stExpander div[data-testid="stExpanderLabel"] strong { color: #25D366; }

.whatsapp-groups-table { border-collapse: collapse; width: 100%; margin-top: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; border: 1px solid #eee; }
.whatsapp-groups-table caption { caption-side: top; text-align: left; font-weight: bold; padding: 10px 12px; font-size: 1.1em; color: #333; background-color: #F5F6F5; border-bottom: 1px solid #eee;}
.whatsapp-groups-table th { background-color: #25D366; color: white; padding: 12px; font-weight: bold; }
.whatsapp-groups-table th:nth-child(1) { text-align: center; width: 80px; } /* Logo column */
.whatsapp-groups-table th:nth-child(2) { text-align: left; } /* Group Name column */
.whatsapp-groups-table th:nth-child(3) { text-align: right; width: 150px; }  /* Group Link (Join) column */
.whatsapp-groups-table tr { border-bottom: 1px solid #eee; }
.whatsapp-groups-table tr:last-child { border-bottom: none; }
.whatsapp-groups-table tr:hover { background-color: #f9f9f9; }
.whatsapp-groups-table td { padding: 12px; vertical-align: middle; text-align: left; }
.whatsapp-groups-table td:nth-child(1) { width: 60px; padding-right: 8px; text-align: center; } /* Logo */
.whatsapp-groups-table td:nth-child(2) { padding-left: 8px; padding-right: 12px; word-break: break-word; font-weight: 500; color: #333; text-align: left; } /* Name */
.whatsapp-groups-table td:nth-child(3) { width: 140px; text-align: right; padding-left: 12px; } /* Join */
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

def google_search_and_scrape(query, top_n=5): # Removed pause_duration from parameters
    st.info(f"Googling '{query}' (top {top_n} results)...") # Updated info message
    all_scraped_wa_links = set()
    try:
        search_page_urls = list(google_search_function_actual(
            query,
            num_results=top_n,
            lang="en"
            # No 'pause' parameter explicitly passed; library will use its default
        ))

        if not search_page_urls:
            st.warning(f"No Google results for '{query}'. This could be due to: "
                       f"1. The query genuinely yielding no results. "
                       f"2. Google blocking the request (try changing IP/VPN, or waiting). "
                       f"3. The `googlesearch-python` library needing an update or a specific version.", icon="🤔")
            return []

        st.success(f"Found {len(search_page_urls)} pages from Google. Now scraping these pages for WhatsApp links...")
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
        st.error(f"Google search error (TypeError): {e}. "
                   f"This might mean the installed 'googlesearch-python' library version expects different parameters "
                   f"or has an issue. Please check the library's documentation for the version you have installed.", icon="❌")
        return []
    except Exception as e:
        st.error(f"An unexpected error occurred during Google search/scraping for '{query}': {e}. "
                   f"This could be due to network issues, Google blocking, or other library problems. "
                   f"Ensure you have a stable internet connection. "
                   f"If the issue persists, the `googlesearch-python` library might be outdated or temporarily broken.", icon="❌")
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
    df_to_display = active_results_df[active_results_df['Group Name'] != UNNAMED_GROUP_PLACEHOLDER].copy()
    if df_to_display.empty:
        return "<p>No named active groups found to display in the styled table.</p>"

    html_string = '<table class="whatsapp-groups-table" aria-label="List of Active WhatsApp Groups">'
    html_string += '<caption>Active WhatsApp Groups</caption>'
    html_string += '<thead><tr>'
    html_string += '<th scope="col">Logo</th>'
    html_string += '<th scope="col">Group Name</th>'
    html_string += '<th scope="col">Group Link</th>' # UPDATED HEADER
    html_string += '</tr></thead>'
    html_string += '<tbody>'

    for _, row in df_to_display.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", UNNAMED_GROUP_PLACEHOLDER)
        group_link = row.get("Group Link", "")
        
        html_string += '<tr>'
        # Logo column
        html_string += '<td class="group-logo-cell">'
        alt_text = f"{html.escape(group_name)} Group Logo" if group_name != UNNAMED_GROUP_PLACEHOLDER else "WhatsApp Group Logo"
        if logo_url:
            display_logo_url = append_query_param(logo_url, 'w', '96') if logo_url.startswith('https://pps.whatsapp.net/') else logo_url
            html_string += f'<img src="{html.escape(display_logo_url)}" alt="{alt_text}" class="group-logo-img" loading="lazy">'
        else:
             html_string += f'<div class="group-logo-img" style="background-color:#e0e0e0; display:flex; align-items:center; justify-content:center; font-size:0.8em; color:#888;" aria-label="{alt_text}">?</div>'
        html_string += '</td>'
        
        # Group Name column
        safe_group_name = html.escape(group_name)
        html_string += f'<td class="group-name-cell">{safe_group_name}</td>'
        
        # Group Link (Join) column
        html_string += '<td class="join-button-cell">' # class name kept for styling continuity of the button
        if group_link and group_link.startswith(WHATSAPP_DOMAIN):
             # The text of the button remains "Join Group" for user action, but the header is "Group Link"
             html_string += f'<a href="{html.escape(group_link)}" class="join-button" target="_blank" rel="noopener noreferrer">Join Group</a>'
        else:
             html_string += '<span style="color:#888; font-size:0.9em;">N/A</span>'
        html_string += '</td></tr>'

    html_string += '</tbody></table>'
    return html_string

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
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

        gs_top_n = 5 # Default value
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)", "Upload Link File (TXT/CSV/Excel)"]:
            gs_top_n = st.slider("Google Results to Scrape (per keyword)", 1, 20, 5, key="gs_top_n_slider", help="Number of Google search result pages to analyze for each keyword.")
            # gs_pause slider REMOVED
        
        crawl_depth, crawl_pages = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive crawl can be slow and resource-intensive. Use with caution.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth", 0, 5, 2, key="crawl_depth_slider", help="How many links deep to follow from the start page within the same domain.")
            crawl_pages = st.slider("Max Pages to Crawl", 1, 300, 50, key="crawl_pages_slider", help="Maximum number of pages to fetch and parse during the crawl.")
        
        st.markdown("---")
        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.cache_data.clear(); st.success("Results & cache cleared!"); st.rerun()

    current_action_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    try:
        if input_method == "Search and Scrape from Google":
            query = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_query_input")
            if st.button("Search, Scrape & Validate", use_container_width=True, key="gs_button"):
                if query:
                    current_action_scraped_links.update(google_search_and_scrape(query, gs_top_n)) # gs_pause removed
                else: st.warning("Please enter a search query.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            file = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if file and st.button("Process Excel, Scrape & Validate", use_container_width=True, key="gs_bulk_button"):
                keywords = load_keywords_from_excel(file)
                if keywords:
                    st.info(f"Processing {len(keywords)} keywords from Excel...")
                    prog_bar_bulk, stat_txt_bulk = st.progress(0), st.empty()
                    total_links_from_bulk = 0
                    for i, kw in enumerate(keywords):
                        stat_txt_bulk.text(f"Keyword: '{kw}' ({i+1}/{len(keywords)}). Total links found so far: {total_links_from_bulk}")
                        links_from_kw = google_search_and_scrape(kw, gs_top_n) # gs_pause removed
                        current_action_scraped_links.update(links_from_kw)
                        total_links_from_bulk = len(current_action_scraped_links)
                        prog_bar_bulk.progress((i+1)/len(keywords))
                    stat_txt_bulk.success(f"Bulk keyword processing complete. Found {total_links_from_bulk} unique WhatsApp links in total.")
                else: st.warning("No valid keywords found in the uploaded Excel file.")

        elif input_method == "Scrape from Specific Webpage URL":
            url = st.text_input("Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if url and (url.startswith("http://") or url.startswith("https://")):
                    with st.spinner(f"Scraping {url}..."):
                        current_action_scraped_links.update(scrape_whatsapp_links_from_page(url))
                    st.success(f"Scraping of '{url}' done. Found {len(current_action_scraped_links)} potential WhatsApp links.")
                else: st.warning("Please enter a valid URL (starting with http:// or https://).")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain = st.text_input("Base Domain URL for Crawl:", placeholder="example.com or https://example.com", key="crawl_domain_input")
            if st.button("Crawl Website, Scrape & Validate", use_container_width=True, key="crawl_button"):
                if domain:
                    st.info(f"Starting extensive crawl of '{domain}'. Progress will be shown in the sidebar.")
                    current_action_scraped_links.update(crawl_website(domain, crawl_depth, crawl_pages))
                    st.success(f"Website crawl complete. Found {len(current_action_scraped_links)} potential WhatsApp links.")
                else: st.warning("Please enter a base domain or URL to start crawling.")

        elif input_method == "Enter Links Manually (for Validation)":
            text = st.text_area("WhatsApp Links (one per line):", height=200, key="manual_links_area", placeholder="https://chat.whatsapp.com/xxxxxxxxxx\nhttps://chat.whatsapp.com/yyyyyyyyyy")
            if st.button("Validate Manual Links", use_container_width=True, key="manual_validate_button"):
                raw_links = [line.strip() for line in text.split('\n') if line.strip()]
                if raw_links:
                    valid_format_links = {l for l in raw_links if l.startswith(WHATSAPP_DOMAIN)}
                    if len(valid_format_links) < len(raw_links):
                        st.warning(f"Ignored {len(raw_links)-len(valid_format_links)} entries that don't look like WhatsApp links.")
                    current_action_scraped_links.update(valid_format_links)
                    if not valid_format_links: st.warning("No valid WhatsApp link formats found in the input.")
                else: st.warning("Please enter at least one link.")

        elif input_method == "Upload Link File (TXT/CSV/Excel)":
            file = st.file_uploader("Upload File (TXT/CSV for links, XLSX for keywords)", type=["txt", "csv", "xlsx"], key="upload_file_input")
            if file and st.button("Process File & Validate/Scrape", use_container_width=True, key="upload_process_button"):
                if file.name.endswith('.xlsx'):
                    st.info("Processing Excel file for keywords to search on Google...")
                    keywords = load_keywords_from_excel(file)
                    if keywords:
                        prog_bar_excel_kw, stat_txt_excel_kw = st.progress(0), st.empty()
                        total_links_from_excel_kw = 0
                        for i, kw in enumerate(keywords):
                            stat_txt_excel_kw.text(f"Keyword from Excel: '{kw}' ({i+1}/{len(keywords)}). Total links: {total_links_from_excel_kw}")
                            links_from_kw = google_search_and_scrape(kw, gs_top_n) # gs_pause removed
                            current_action_scraped_links.update(links_from_kw)
                            total_links_from_excel_kw = len(current_action_scraped_links)
                            prog_bar_excel_kw.progress((i+1)/len(keywords))
                        stat_txt_excel_kw.success(f"Excel keyword processing complete. Found {total_links_from_excel_kw} links.")
                    else: st.warning("No valid keywords found in the uploaded Excel file.")
                elif file.name.endswith(('.txt', '.csv')):
                    st.info("Processing TXT/CSV file for direct WhatsApp links...")
                    raw_links = load_links_from_file(file)
                    if raw_links:
                        valid_format_links = {l for l in raw_links if l.startswith(WHATSAPP_DOMAIN)}
                        if len(valid_format_links) < len(raw_links):
                            st.warning(f"Ignored {len(raw_links)-len(valid_format_links)} entries from file that don't look like WhatsApp links.")
                        current_action_scraped_links.update(valid_format_links)
                        if not valid_format_links: st.warning("No valid WhatsApp link formats found in the file.")
                        else: st.success(f"Loaded {len(valid_format_links)} WhatsApp links from file for validation.")
                    else: st.warning("No links found or loaded from the uploaded file.")
                else: st.warning("Unsupported file type. Please use .txt, .csv for links, or .xlsx for keywords.")
    except Exception as e: st.error(f"An error occurred in the input/scraping phase: {e}", icon="💥")


    links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)
    if links_to_validate_now:
        st.success(f"Found {len(current_action_scraped_links)} total links from this action. "
                   f"Validating {len(links_to_validate_now)} new (unprocessed) links...")
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
                    st.error(f"Error validating link {link_validated}: {val_exc}")
                    parsed_url_val_err = urlparse(link_validated)
                    normalized_link_val_err = f"{parsed_url_val_err.scheme}://{parsed_url_val_err.netloc}{parsed_url_val_err.path}"
                    st.session_state.processed_links_in_session.add(normalized_link_val_err)
                    new_results_this_run.append({"Group Name": "Validation Error", "Group Link": link_validated, "Logo URL": "", "Status": f"Validation Failed: {type(val_exc).__name__}"})

                prog_val.progress((i+1)/len(links_to_validate_now))
                stat_val.text(f"Validated {i+1}/{len(links_to_validate_now)} new links: {link_validated[:50]}...")
        
        if new_results_this_run:
            st.session_state.results.extend(new_results_this_run)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")
    elif current_action_scraped_links and not links_to_validate_now:
         st.info("All WhatsApp links found in this action were already processed in this session.")

    if 'results' in st.session_state and st.session_state.results:
        unique_results_df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = unique_results_df.to_dict('records')
        
        df_display = unique_results_df.reset_index(drop=True)

        active_df = df_display[df_display['Status'].str.contains('Active', na=False)].copy()
        expired_df = df_display[df_display['Status'] == 'Expired'].copy()
        error_df = df_display[~df_display['Status'].str.contains('Active', na=False) & (df_display['Status'] != 'Expired')].copy()

        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        col1.markdown(f'<div class="metric-card">Total Processed<br><div class="metric-value">{len(df_display)}</div></div>', unsafe_allow_html=True)
        col2.markdown(f'<div class="metric-card">Active Links<br><div class="metric-value">{len(active_df)}</div></div>', unsafe_allow_html=True)
        col3.markdown(f'<div class="metric-card">Expired Links<br><div class="metric-value">{len(expired_df)}</div></div>', unsafe_allow_html=True)
        col4.markdown(f'<div class="metric-card">Other Status<br><div class="metric-value">{len(error_df)}</div></div>', unsafe_allow_html=True)


        with st.expander("🔎 View and Filter All Processed Results", expanded=True):
            all_statuses = sorted(list(df_display['Status'].unique()))
            default_statuses_filter = [s for s in all_statuses if 'Active' in s]
            if not default_statuses_filter and 'Expired' in all_statuses : default_statuses_filter.append('Expired')
            if not default_statuses_filter and all_statuses : default_statuses_filter = [all_statuses[0]]

            status_filter = st.multiselect("Filter by Status:", all_statuses, default=default_statuses_filter, key="status_filter_multiselect")
            search_term_df = st.text_input("Search in Group Name or Link:", key="df_search_term", placeholder="Type to filter...")
            filtered_df_status = df_display[df_display['Status'].isin(status_filter)] if status_filter else df_display.copy()
            
            if search_term_df:
                search_term_lower = search_term_df.lower()
                final_filtered_df = filtered_df_status[
                    filtered_df_status['Group Name'].str.lower().str.contains(search_term_lower, na=False) |
                    filtered_df_status['Group Link'].str.lower().str.contains(search_term_lower, na=False)
                ]
            else:
                final_filtered_df = filtered_df_status

            st.dataframe(final_filtered_df, column_config={
                "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join", width="medium"), # Dataframe display text can remain "Join" for brevity
                "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View Logo", width="small", help="Direct URL of the group logo, if found."),
                "Status": st.column_config.TextColumn("Status", width="small")
            }, hide_index=True, height=400, use_container_width=True)

        st.subheader("✨ Styled Output (Active Groups with Names)")
        if not active_df.empty:
            html_out = generate_styled_html_table(active_df) 
            if "<td" in html_out:
                with st.expander("View and Copy Styled HTML / Download", expanded=True):
                    st.markdown(html_out, unsafe_allow_html=True)
                    st.text_area("Raw HTML Code:", value=html_out, height=300, key="styled_html_export_area", help="Ctrl+A to select all, then Ctrl+C to copy.")
                    st.download_button("📥 Download Styled HTML", html_out.encode('utf-8'), "styled_whatsapp_groups.html", "text/html", use_container_width=True, key="styled_html_export_download")
            else:
                st.info("No active groups with recognized names found to display in the styled table. Unnamed groups are excluded from this view.")
        else: st.info("No active groups found in the results yet to generate styled output.")

        st.subheader("Downloads (CSV)")
        col_dl1, col_dl2 = st.columns(2)
        if not active_df.empty:
            col_dl1.download_button("📥 Active Groups (CSV)", active_df.to_csv(index=False).encode('utf-8'), "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv")
        else:
            col_dl1.button("📥 Active Groups (CSV)", disabled=True, use_container_width=True, help="No active groups to download.")

        if not df_display.empty:
            col_dl2.download_button("📥 All Processed Results (CSV)", df_display.to_csv(index=False).encode('utf-8'), "all_processed_results.csv", "text/csv", use_container_width=True, key="dl_all_csv")
        else:
            col_dl2.button("📥 All Processed Results (CSV)", disabled=True, use_container_width=True, help="No results to download.")
            
    else:
        st.info("Start by searching, entering, or uploading links to see results here!", icon="ℹ️")

if __name__ == "__main__":
    main()
