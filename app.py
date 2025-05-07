import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_library # Use the direct import from user's code
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io
from fake_useragent import UserAgent

# --- Initialize UserAgent (for non-Google-result scraping and validation) ---
try:
    ua_general = UserAgent()
except Exception as e:
    # st.error(f"Could not initialize Fake UserAgent for general scraping, using a default. Error: {e}") # Suppress this error in final app unless critical
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
# IMAGE_PATTERN for validate_link and enhanced_scrape (from user's working code)
IMAGE_PATTERN_SHARED = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
MAX_VALIDATION_WORKERS = 10

# --- Custom CSS ---
st.markdown("""
<style>
/* General Streamlit overrides */
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
/* Adjusted Streamlit button style for consistency, if needed elsewhere */
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; }
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }

/* Metric cards - from user example */
.metric-card { 
    padding: 10px; 
    border-radius: 5px; 
    border: 1px solid #e0e0e0; 
    text-align: center;
    margin-bottom: 10px;
}
.metric-card div[data-testid="stMetricValue"] { font-size: 1.5em; }
.metric-card div[data-testid="stMetricLabel"] { font-size: 0.9em; color: #555; }


/* --- CSS for the Styled HTML Table Output --- */
.whatsapp-groups-table {
    border-collapse: collapse; /* Remove space between borders */
    width: 100%; /* Make table responsive to container width */
    margin-top: 20px; /* Add space above the table */
    box-shadow: 0 2px 5px rgba(0,0,0,0.1); /* Subtle shadow */
    border-radius: 8px; /* Rounded corners for the table */
    overflow: hidden; /* Ensures border-radius works with collapse */
}

.whatsapp-groups-table tr {
    border-bottom: 1px solid #eee; /* Separator line for rows */
}

/* Style for the last row */
.whatsapp-groups-table tr:last-child {
    border-bottom: none;
}

.whatsapp-groups-table td {
    padding: 10px; /* Padding inside cells */
    vertical-align: middle; /* Vertically align content */
    text-align: left; /* Default text alignment */
}

/* Column widths (optional, adjust as needed) */
.whatsapp-groups-table td:nth-child(1) { /* Logo column */
    width: 50px; /* Fixed width for logo cell */
    padding-right: 5px; /* Space between logo and name */
    text-align: center; /* Center logo horizontally in its cell */
}

.whatsapp-groups-table td:nth-child(2) { /* Name column */
    flex-grow: 1; /* Allow name column to take available space */
    padding-left: 5px; /* Space between logo and name */
    padding-right: 10px; /* Space between name and button */
    word-break: break-word; /* Prevent long names from overflowing */
}

.whatsapp-groups-table td:nth-child(3) { /* Button column */
     width: 120px; /* Fixed width for button cell */
     text-align: right; /* Align button to the right */
     padding-left: 10px; /* Space between name and button */
}


/* Image styling */
.group-logo-img {
    width: 35px; /* Image width */
    height: 35px; /* Image height */
    border-radius: 50%; /* Make it circular */
    object-fit: cover; /* Crop image to fit circular container */
    display: block; /* Ensure width/height are respected */
    margin: 0 auto; /* Center the block image if cell allows */
}

/* Join Button styling */
.join-button {
    display: inline-block; /* Allows padding and margin */
    background-color: #25D366; /* WhatsApp Green */
    color: #FFFFFF !important; /* White text */
    padding: 8px 16px; /* Padding inside the button */
    border-radius: 8px; /* Rounded corners */
    text-decoration: none; /* Remove underline from link */
    font-weight: bold; /* Bold text */
    text-align: center; /* Center text inside the button */
    white-space: nowrap; /* Prevent button text from wrapping */
    /* Inherit font from Streamlit */
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
    font-size: 0.9em; /* Slightly smaller text */
}

.join-button:hover {
    background-color: #1EBE5A; /* Darker green on hover */
    color: #FFFFFF !important; /* Ensure text stays white on hover */
    text-decoration: none; /* Ensure no underline on hover */
}
/* --- End of Styled HTML Table CSS --- */

</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---
def get_random_headers_for_general_use():
    """Returns headers with a random User-Agent for general scraping/validation."""
    return {
        "User-Agent": ua_general.random,
        "Accept-Language": "en-US,en;q=0.9"
    }

def append_query_param(url, param_name, param_value):
    if not url: return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    # Reconstruct URL without fragment, then add fragment back if it existed
    url_without_fragment = parsed_url._replace(query=new_query_string, fragment='').geturl()
    if parsed_url.fragment:
         return f"{url_without_fragment}#{parsed_url.fragment}"
    return url_without_fragment


# --- Functions directly from USER'S WORKING EXAMPLE (for Google Search path) ---
def google_search_user_original(query, top_n=5, pause_duration=2.0): # Added pause_duration for consistency
    """Fetch URLs from Google's top N search results. (User's original function signature)"""
    try:
        # Using the direct 'search' import which is google_search_library
        st.sidebar.info(f"Googling (user original) '{query}' (top {top_n}, pause: {pause_duration}s)...")
        urls = list(google_search_library(query, num_results=top_n, lang="en", pause=pause_duration))
        if not urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms.")
        return urls
    except Exception as e:
        st.error(f"Google Search error (user original): {str(e)}")
        return []

def scrape_whatsapp_links_user_original(url):
    """Scrape WhatsApp group links from a webpage. (User's original function)
       This uses a FIXED User-Agent and direct requests.get()."""
    try:
        headers = { # Fixed User-Agent from user's working example
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8' # From user's example
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links.append(a['href'].split('?')[0]) # Normalize
        # Also look for links in text content, similar to enhanced but simpler
        for text in soup.stripped_strings:
             # Find potential links in the text, robustly handling surrounding punctuation
            found_links_in_text = re.findall(r'(https?://chat\.whatsapp\.com/[^\s"\'<>()]+)', text)
            for flink in found_links_in_text:
                links.append(flink.split('?')[0]) # Normalize

        return list(set(links))
    except Exception: # Broad catch as in user's example
        return []
# --- END of functions from USER'S WORKING EXAMPLE ---


# --- Enhanced scraping function (for Specific Page / Entire Website) ---
def scrape_whatsapp_links_enhanced(url, session):
    links = set()
    try:
        netloc_for_error = urlparse(url).netloc or url[:30]
        response = session.get(url, headers=get_random_headers_for_general_use(), timeout=15) # Uses fake UA
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find links in href attributes
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0]) # Normalize

        # Find links directly in text content
        # Process text nodes or stripped strings for potential links not in <a> tags
        text_content = ' '.join(soup.stripped_strings)
        if WHATSAPP_DOMAIN in text_content:
            # More robust regex to capture links potentially surrounded by other characters
            found_in_chunk = re.findall(r'(https?://chat\.whatsapp\.com/[^\s"\'<>()]+)', text_content)
            for link_url in found_in_chunk: 
                # Clean up potential trailing punctuation like periods or commas
                clean_link = re.sub(r'[.,;!?"\'<>)]+$', '', link_url)
                links.add(clean_link.split('?')[0])

    except requests.exceptions.Timeout: st.sidebar.warning(f"Timeout (enh) {netloc_for_error}", icon="⏱️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape err (enh) {netloc_for_error}: {type(e).__name__}", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Parse err (enh) {netloc_for_error}: {type(e).__name__}", icon="💣")
    return list(links)

# --- Validation function (uses fake UA) ---
def validate_link(link):
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        response = requests.get(link, headers=get_random_headers_for_general_use(), timeout=10, allow_redirects=True) # Uses fake UA
        response.encoding = 'utf-8'
        
        # Check for redirects away from WhatsApp domain first
        if response.status_code == 200 and WHATSAPP_DOMAIN not in response.url: 
             result["Status"] = f"Redirected Away (Final URL: {response.url[:50]}...)"
             return result
             
        # Check for 404, 403 etc.
        if response.status_code != 200: result["Status"] = f"HTTP Error {response.status_code}"; return result

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check for specific indicators of "Expired" page content if HTTP 200
        # Look for common text patterns on expired invite pages
        expired_patterns = [
            re.compile(r'invite link is invalid', re.IGNORECASE),
            re.compile(r'invite link was reset', re.IGNORECASE),
            re.compile(r'group doesn\'t exist', re.IGNORECASE), # Less common, but possible
        ]
        page_text = soup.get_text()
        if any(pattern.search(page_text) for pattern in expired_patterns):
             result["Status"] = "Expired (Content Match)"
             return result

        # Try to get Group Name from Open Graph title
        meta_title = soup.find('meta', property='og:title')
        result["Group Name"] = unescape(meta_title['content']).strip() or "Unnamed Group" if meta_title and meta_title.get('content') else "Unnamed Group"
        
        # Try to get Logo URL from Open Graph image or specific WhatsApp patterns
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
             src = unescape(meta_image['content'])
             # Validate if it looks like a WhatsApp profile image URL or just accept any og:image for now?
             # Let's stick closer to the user's original logic which focused on pps.whatsapp.net URLs.
             # The OG image is often a high-res version or different asset.
             pass # Let's rely on finding the specific IMG tag pattern instead for "Active" status check

        # Find specific IMG tag for profile picture
        img_tags = soup.find_all('img', src=True)
        found_logo_img = False
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN_SHARED.match(src): # Using shared pattern
                result["Logo URL"] = src
                result["Status"] = "Active" # Set active if logo found
                found_logo_img = True
                break
        
        # If after all checks, status is still "Error" or hasn't been set to "Active" or "Expired (Content Match)"
        # this could mean it's a valid link but perhaps the logo loading failed, or it's a less common error state.
        # The original user logic seemed to default to "Expired" if logo wasn't found.
        # Let's refine: If status is still "Error" at this point, it might be an "Active (No Logo Found)" state
        # or a different kind of parsing issue. For simplicity and alignment with user's demo, let's keep the
        # "Active" status tied to finding the logo and default others based on content/errors.
        if result["Status"] == "Error":
             # If we reached here, HTTP was 200, not redirected, but no logo found and no explicit expired text.
             # It's likely an active group where logo scrape failed, or a subtle page variant.
             # Let's call it active if we found a group name, otherwise unknown/error.
             if result["Group Name"] != "Unknown":
                 result["Status"] = "Active (No Logo Scraped)"
             else:
                 result["Status"] = "Parsing Error (No Name or Logo)" # Failed to find key info

    except requests.exceptions.ConnectionError: result["Status"] = "Connection Error"
    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.RequestException: result["Status"] = "Network Error" # Catch other request exceptions
    except Exception: result["Status"] = "Parsing Error (Unknown)" # General parsing or unexpected error
    return result


def crawl_website(start_url, max_depth=3, max_pages=100): # Uses fake UA via session.get
    # ... (same as previous, ensures session.get calls use get_random_headers_for_general_use()) ...
    if not start_url.strip(): return [], None # Handle empty input
    if not start_url.startswith(('http://', 'https://')): 
         # Attempt adding https://, but warn user
         start_url = 'https://' + start_url
         st.sidebar.warning(f"Prepending 'https://' to URL: {start_url}", icon="🔗")

    parsed_start_url = urlparse(start_url)
    # Basic validation for the start URL
    if not parsed_start_url.netloc:
        st.sidebar.error(f"Invalid start URL provided: {start_url}", icon="🚫")
        return [], None
        
    base_domain = parsed_start_url.netloc.replace('www.', '') # Use root domain for comparison
    urls_to_visit, visited_urls, scraped_content_urls = [(start_url, 0)], set(), set()
    
    # Ensure visited_urls set is populated correctly to avoid reprocessing
    visited_urls.add(urljoin(start_url, parsed_start_url.path or '/')) # Add normalized start URL path
    
    session = requests.Session() # New session for crawl
    st.sidebar.info(f"Starting crawl on `{base_domain}` (Max Depth: {max_depth}, Max Pages: {max_pages})...")
    
    page_count = 0
    
    # Add a check to prevent infinite loops on sites with complex link structures
    max_queue_size = max_pages * 5 # Simple heuristic to limit memory usage and potential endless queues

    with st.spinner(f"Crawling {base_domain}..."):
        while urls_to_visit and page_count < max_pages:
            # Prioritize deeper links slightly less? Or just pop(0) for BFS
            current_url_info = urls_to_visit.pop(0) # Use pop(0) for Breadth-First Search (common crawl)
            current_url, depth = current_url_info

            # Re-check visited after popping, as it might have been added by another path
            # Also, normalize the URL for consistent visited checks
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')
            if normalized_current_url in visited_urls or depth > max_depth: 
                st.sidebar.markdown(f"<span style='color:#888; font-size:0.8em;'>Skipping: {normalized_current_url[:50]}... (Visited/Depth)</span>", unsafe_allow_html=True)
                continue
            
            visited_urls.add(normalized_current_url) # Mark as visited *before* attempting request

            if page_count >= max_pages: break # Check again before making request

            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}): {current_url[:60]}...")
            
            try:
                response = session.get(current_url, headers=get_random_headers_for_general_use(), timeout=10) # Increased timeout slightly
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                
                # Check Content-Type to ensure it's HTML before parsing
                if 'text/html' not in response.headers.get('Content-Type', ''):
                     st.sidebar.markdown(f"<span style='color:#888; font-size:0.8em;'>Skipping: {current_url[:50]}... (Not HTML)</span>", unsafe_allow_html=True)
                     continue # Skip non-HTML content
                
                scraped_content_urls.add(current_url) # Add original URL to scrape list
                page_count += 1
                
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag.get('href')
                        if href:
                            abs_url = urljoin(current_url, href)
                            parsed_abs_url = urlparse(abs_url)
                            
                            # Basic checks for validity and being on the same base domain
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain and \
                               not parsed_abs_url.fragment and \
                               abs_url not in visited_urls and \
                               (abs_url, depth + 1) not in urls_to_visit and \
                               len(urls_to_visit) < max_queue_size: # Queue limit
                                
                                normalized_abs_url = urljoin(abs_url, parsed_abs_url.path or '/')
                                if normalized_abs_url not in visited_urls: # Final check after normalization
                                     urls_to_visit.append((abs_url, depth + 1))
                                     # Optional: Mark as visited in queue to prevent duplicates being added multiple times
                                     # visited_urls.add(normalized_abs_url) # Decide if marking visited on adding vs processing is better
                                     
            except requests.exceptions.Timeout: st.sidebar.markdown(f"<span style='color:orange; font-size:0.8em;'>Timeout: {current_url[:50]}...</span>", unsafe_allow_html=True)
            except requests.exceptions.HTTPError as e: st.sidebar.markdown(f"<span style='color:orange; font-size:0.8em;'>HTTP Error {e.response.status_code}: {current_url[:50]}...</span>", unsafe_allow_html=True)
            except requests.exceptions.ConnectionError: st.sidebar.markdown(f"<span style='color:red; font-size:0.8em;'>Conn Error: {current_url[:50]}...</span>", unsafe_allow_html=True)
            except requests.exceptions.RequestException as e: st.sidebar.markdown(f"<span style='color:orange; font-size:0.8em;'>Req Error ({type(e).__name__}): {current_url[:50]}...</span>", unsafe_allow_html=True)
            except Exception as e: st.sidebar.markdown(f"<span style='color:red; font-size:0.8em;'>Parse/Other Error ({type(e).__name__}): {current_url[:50]}...</span>", unsafe_allow_html=True)
            
            # Optional: Add a small delay between requests to be polite during crawl
            # import time
            # time.sleep(0.1) 

    st.sidebar.success(f"Crawl finished. Found {len(scraped_content_urls)} pages to scrape links from.")
    if len(urls_to_visit) >= max_queue_size:
        st.sidebar.warning(f"Crawl queue reached maximum size ({max_queue_size}). Some links may not have been followed.", icon="❗️")
    if page_count >= max_pages:
         st.sidebar.warning(f"Stopped crawl after reaching maximum pages ({max_pages}).", icon="❗️")

    return list(scraped_content_urls), session


def load_links_from_text_file(uploaded_file): # From user example
    if uploaded_file is None: return []
    try:
        # Read as bytes and then decode, better handles different text encodings
        content = uploaded_file.getvalue()
        # Attempt decoding with utf-8, fallback to latin-1 if needed
        try:
            text_content = content.decode('utf-8')
        except UnicodeDecodeError:
            text_content = content.decode('latin-1')
            st.warning("Detected non-UTF-8 encoding, falling back to latin-1.", icon="⚠️")
            
        links = [line.strip() for line in text_content.splitlines() if line.strip()]
        return links
    except Exception as e:
        st.error(f"Error reading file {uploaded_file.name}: {e}")
        return []


def load_keywords_from_excel(uploaded_file):
    if uploaded_file is None: return []
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        # Get the first column, drop NaNs, convert to string, then to list
        keywords = df.iloc[:, 0].dropna().astype(str).tolist()
        # Optional: Add a check for minimum keyword length or filter empty strings
        keywords = [kw for kw in keywords if len(kw.strip()) > 1]
        if not keywords: st.warning("No valid keywords found in the first column.")
        return keywords
    except Exception as e:
        st.error(f"Error reading Excel file {uploaded_file.name}: {e}. Ensure it's a valid .xlsx file and 'openpyxl' installed.", icon="❌")
        return []

# --- Function to generate the styled HTML table ---
def generate_styled_html_table(active_results_df):
    if active_results_df.empty: 
        return "<p>No active groups found to generate a styled table.</p>" # Return message as HTML paragraph

    # Start the HTML table with the custom class
    html_string = '<table class="whatsapp-groups-table"><tbody>' # Using tbody is good practice

    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", "Unnamed Group") # Default name
        group_link = row.get("Group Link", "")
        
        # Start a new table row
        html_string += '<tr>'
        
        # Cell 1: Logo
        html_string += '<td class="group-logo-cell">'
        if logo_url:
            # Request a slightly larger image from server for better downscaling quality
            resized_logo_url_server = append_query_param(logo_url, 'w', '80') # e.g., request 80px width
            # Use the custom image class for styling (size, circle, etc.)
            html_string += f'<img src="{resized_logo_url_server}" alt="Group Logo" class="group-logo-img">'
        else:
             # Add a placeholder div to maintain cell structure even without an image
             html_string += '<div class="group-logo-img" style="background-color:#e0e0e0;"></div>' # Placeholder grey circle
        html_string += '</td>'
        
        # Cell 2: Group Name
        # Sanitize group name for HTML display (basic entity escaping)
        safe_group_name = group_name.replace('&', '&').replace('<', '<').replace('>', '>').replace('"', '"').replace("'", ''')
        html_string += f'<td class="group-name-cell">{safe_group_name}</td>'
        
        # Cell 3: Join Button
        html_string += '<td class="join-button-cell">'
        # Use the custom button class for styling the link
        if group_link:
             html_string += f'<a href="{group_link}" class="join-button" target="_blank">Join Group</a>'
        else:
             # Handle case where link is missing (shouldn't happen for active groups, but defensive)
             html_string += 'N/A'
        html_string += '</td>'
        
        # End the table row
        html_string += '</tr>'

    # End the table body and table
    html_string += '</tbody></table>'

    return html_string

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Enhanced tool to find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

    # Initialize session state variables safely
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()
    # Convert initial results to a set of links for processed tracking if needed
    if isinstance(st.session_state.processed_links_in_session, list): # Handle potential legacy state format
         st.session_state.processed_links_in_session = set(st.session_state.processed_links_in_session)
         
    # Ensure results is a list of dicts
    if not isinstance(st.session_state.results, list):
         st.session_state.results = []


    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google", # Simplified name to match user's example for this context
            "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL",
            "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)",
            "Upload Link File (TXT/CSV for Validation)"
        ], key="input_method_main_select") # Unique key for selectbox

        # Settings for Google Search (User's original style)
        google_results_slider_top_n = 5
        google_search_pause = 2.0 # Default pause for googlesearch library

        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)"]:
            google_results_slider_top_n = st.slider(
                "Number of top Google results to scrape from",
                min_value=1, max_value=20, value=5, key="google_top_n_slider", # User's range extended slightly
                 help="How many top web results from Google Search to visit and scrape links from."
            )
            google_search_pause = st.slider(
                "Google Search Pause (seconds):", min_value=1.0, max_value=10.0, value=2.0, step=0.5,
                help="Pause between Google search API calls. Increase if encountering rate limits.", key="google_pause_slider"
            )

        # Settings for Extensive Crawl
        crawl_depth_val, max_crawl_pages_val = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive website crawling can be very slow and consume resources. Use with caution.", icon="🚨")
            st.info("Crawler will attempt to stay within the base domain of the provided URL.", icon="ℹ️")
            crawl_depth_val = st.slider("Max Crawl Depth:", min_value=0, max_value=5, value=2, key="crawl_depth_slider",
                                        help="Maximum number of link clicks away from the starting URL. Depth 0 is just the starting page.")
            max_crawl_pages_val = st.slider("Max Pages to Crawl:", min_value=1, max_value=200, value=50, key="crawl_pages_slider",
                                           help="Maximum number of unique pages on the website to attempt scraping.")

        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set() # Clear processed links set
            st.cache_data.clear() # Clear Streamlit's data cache just in case
            st.success("All results and cache cleared!")
            st.rerun() # Rerun to clear the display immediately

    all_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    # Session for enhanced methods (Specific Page, Site Crawl's internal scrape)
    general_purpose_session = None # Initialize to None
    
    try:
        # --- Input Method Logic ---
        if input_method == "Search and Scrape from Google":
            keyword_gs = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_keyword_input")
            if st.button("Search, Scrape, and Validate", use_container_width=True, key="gs_button"): # Key from user example
                if not keyword_gs: st.warning("Please enter a search query.")
                else:
                    # Use user's original google_search function
                    search_page_urls = google_search_user_original(keyword_gs, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                    if search_page_urls:
                        st.success(f"Found {len(search_page_urls)} webpages. Scraping WhatsApp links (user original method)...")
                        # Create a placeholder for scraping progress
                        prog_bar_gs = st.progress(0)
                        status_text_gs = st.empty()

                        for i, page_url in enumerate(search_page_urls):
                            status_text_gs.text(f"Scraping links from page {i+1}/{len(search_page_urls)}: {page_url[:50]}...")
                            # Use user's original scrape_whatsapp_links function (fixed UA, no session)
                            links_from_page = scrape_whatsapp_links_user_original(page_url)
                            all_scraped_links.update(links_from_page)
                            prog_bar_gs.progress((i+1)/len(search_page_urls))
                        status_text_gs.success(f"Google page scraping complete. Found {len(all_scraped_links)} unique links.")
                    else:
                         st.warning("No webpages found via Google search.")

        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file_bulk = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if excel_file_bulk and st.button("Process Excel & Scrape from Google", use_container_width=True, key="gs_bulk_button"):
                keywords_bulk = load_keywords_from_excel(excel_file_bulk)
                if not keywords_bulk: st.warning("No keywords found or processed from Excel.")
                else:
                    st.info(f"Processing {len(keywords_bulk)} keywords for Google search & scraping (user original methods)...")
                    prog_bulk, stat_txt_bulk = st.progress(0), st.empty()
                    total_links_found_bulk = 0
                    for i, kw_bulk in enumerate(keywords_bulk):
                        stat_txt_bulk.text(f"Keyword: **{kw_bulk}** ({i+1}/{len(keywords_bulk)}). Found {total_links_found_bulk} links so far.")
                        search_page_urls_bulk = google_search_user_original(kw_bulk, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                        if search_page_urls_bulk:
                            # Scrape pages found for this keyword
                             st.text(f"-> Scraping {len(search_page_urls_bulk)} pages for '{kw_bulk}'...")
                             for page_url_bulk in search_page_urls_bulk:
                                links_from_page_bulk = scrape_whatsapp_links_user_original(page_url_bulk)
                                all_scraped_links.update(links_from_page_bulk)
                                total_links_found_bulk = len(all_scraped_links) # Update total count
                        prog_bulk.progress((i + 1) / len(keywords_bulk))
                    stat_txt_bulk.success(f"Bulk Google processing complete. Found a total of {len(all_scraped_links)} unique links.")

        elif input_method == "Scrape from Specific Webpage URL":
            page_url_specific = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
            if st.button("Scrape Page (Enhanced Method) & Validate", use_container_width=True, key="specific_url_button"):
                if not page_url_specific or not (page_url_specific.startswith("http://") or page_url_specific.startswith("https://")):
                    st.warning("Please enter a valid URL starting with http:// or https://")
                else:
                    # Create a session for this specific scrape
                    general_purpose_session = requests.Session()
                    with st.spinner(f"Scraping {page_url_specific} (enhanced method)..."):
                        links_from_page_spec = scrape_whatsapp_links_enhanced(page_url_specific, general_purpose_session)
                        all_scraped_links.update(links_from_page_spec)
                    st.success(f"Scraping of {page_url_specific} complete. Found {len(all_scraped_links)} unique links.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url_crawl = st.text_input("Enter Base Domain URL:", placeholder="example.com or https://example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape (Enhanced Method)", use_container_width=True, key="crawl_button"):
                if not domain_url_crawl: st.warning("Please enter a domain URL.")
                else:
                    pages_to_scrape_crawl, crawl_session_obj = crawl_website(domain_url_crawl, max_depth=crawl_depth_val, max_pages=max_crawl_pages_val)
                    general_purpose_session = crawl_session_obj # Use the session created by the crawler

                    if pages_to_scrape_crawl:
                        st.info(f"Crawled. Now scraping {len(pages_to_scrape_crawl)} pages for WhatsApp links (enhanced method)...")
                        prog_crawl, stat_txt_crawl = st.progress(0), st.empty()
                        total_links_found_crawl = 0
                        for i, p_url_crawl in enumerate(pages_to_scrape_crawl):
                            stat_txt_crawl.text(f"Scraping: {p_url_crawl[:60]}... ({i+1}/{len(pages_to_scrape_crawl)}). Found {total_links_found_crawl} links so far.")
                            # Use the session from the crawler
                            links_from_page_crawl = scrape_whatsapp_links_enhanced(p_url_crawl, general_purpose_session)
                            all_scraped_links.update(links_from_page_crawl)
                            total_links_found_crawl = len(all_scraped_links) # Update total count
                            prog_crawl.progress((i + 1) / len(pages_to_scrape_crawl))
                        stat_txt_crawl.success(f"Website scraping complete. Found a total of {len(all_scraped_links)} unique links.")
                    else:
                        st.warning("No valid pages found/scraped from the provided domain within the specified depth and page limits.")

        elif input_method == "Enter Links Manually (for Validation)": # From user example
            links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123", key="manual_links_text_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"): # Key from user example
                links_manual = [line.strip() for line in links_text_manual.split('\n') if line.strip()]
                if not links_manual: st.warning("Please enter at least one link.")
                else:
                    valid_format_links = [l for l in links_manual if l.startswith(WHATSAPP_DOMAIN)]
                    if len(valid_format_links) < len(links_manual):
                         st.warning(f"Skipped {len(links_manual) - len(valid_format_links)} lines that didn't start with '{WHATSAPP_DOMAIN}'")
                    all_scraped_links.update(valid_format_links)


        elif input_method == "Upload Link File (TXT/CSV for Validation)": # From user example
            uploaded_file_val = st.file_uploader("Upload TXT or CSV", type=["txt", "csv"], key="upload_file_links")
            if uploaded_file_val and st.button("Validate File Links", use_container_width=True, key="upload_validate_button"): # Key from user example
                links_from_file = load_links_from_text_file(uploaded_file_val) # User's load_links
                if not links_from_file: st.warning("No links found or processed from the uploaded file.")
                else:
                    valid_format_links_file = [l for l in links_from_file if l.startswith(WHATSAPP_DOMAIN)]
                    if len(valid_format_links_file) < len(links_from_file):
                        st.warning(f"Skipped {len(links_from_file) - len(valid_format_links_file)} lines that didn't start with '{WHATSAPP_DOMAIN}'")
                    all_scraped_links.update(valid_format_links_file)

    except Exception as e:
        st.error(f"An unexpected error occurred during the input/scraping phase: {e}")
    finally:
        # Close the session if it was created
        if general_purpose_session:
            general_purpose_session.close()


    # --- Unified Validation Step (uses fake UA via validate_link) ---
    links_to_validate_now = list(all_scraped_links - st.session_state.processed_links_in_session)

    if links_to_validate_now: # Only run validation if there are new links
        st.success(f"Found {len(all_scraped_links)} total unique links from input(s). Validating {len(links_to_validate_now)} new links...")
        prog_val, stat_val = st.progress(0), st.empty() # From user example (prog bar / status text)
        new_results_validation = []

        # Use a thread pool for parallel validation
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor: # Max_workers from user's example
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
            for i, future in enumerate(as_completed(future_to_link)):
                link_validated = future_to_link[future]
                try:
                    result_validated = future.result()
                    new_results_validation.append(result_validated)
                except Exception as exc:
                    # Handle exceptions during validation thread execution
                    new_results_validation.append({"Group Name": "Error", "Group Link": link_validated, "Logo URL": "", "Status": f"Validation Exception: {exc}"})
                    st.error(f"Error validating {link_validated}: {exc}") # Log the error

                st.session_state.processed_links_in_session.add(link_validated) # Mark as processed regardless of outcome
                prog_val.progress((i + 1) / len(links_to_validate_now))
                stat_val.text(f"Validated {i + 1}/{len(links_to_validate_now)} links") # Text from user example

        # Append new results to session state
        st.session_state.results.extend(new_results_validation)
        stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!") # Or similar success message

    elif all_scraped_links and not links_to_validate_now:
         st.info("No *new* WhatsApp links found. All links previously found have been processed.")
    else:
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")


    # --- Display Results ---
    if 'results' in st.session_state and st.session_state.results:
        # Create DataFrame from session state results, drop duplicates based on link
        df_results_display = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first').reset_index(drop=True)
        # Update session state results to the cleaned DataFrame records
        st.session_state.results = df_results_display.to_dict('records')

        # Filter by status for summary and markdown/html generation
        active_df_display = df_results_display[df_results_display['Status'].str.contains('Active', na=False)].copy() # Includes 'Active' and 'Active (No Logo Scraped)'
        expired_df_display = df_results_display[df_results_display['Status'] == 'Expired'] # Specific 'Expired' status
        error_df_display = df_results_display[df_results_display['Status'].str.contains('Error|HTTP Error|Redirected|Timeout|Network|Parsing', na=False)] # Filter for various error states


        st.subheader("📊 Results Summary") # From user example
        col1_disp, col2_disp, col3_disp, col4_disp = st.columns(4) # Added fourth column for Errors
        with col1_disp:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Links", len(df_results_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col2_disp:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Active Links", len(active_df_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col3_disp:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Expired Links", len(expired_df_display)) # Matching user's display
            st.markdown('</div>', unsafe_allow_html=True)
        with col4_disp:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Error Links", len(error_df_display))
            st.markdown('</div>', unsafe_allow_html=True)


        with st.expander("🔎 View and Filter Results Table", expanded=True): # From user example
            # Collect all unique statuses for the filter
            all_statuses = df_results_display['Status'].unique().tolist()
            # Default filter to show Active and potentially Active (No Logo Scraped) if they exist
            default_statuses = [s for s in all_statuses if 'Active' in s] or all_statuses[:1]
            status_filter_val = st.multiselect("Filter by Status", options=all_statuses, default=default_statuses)

            filtered_df_for_display = df_results_display[df_results_display['Status'].isin(status_filter_val)] if status_filter_val else df_results_display

            # Hide the index column for cleaner display
            st.dataframe(
                filtered_df_for_display,
                column_config={ # From user example
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Group", width="medium"), # Added width hint
                    "Group Name": st.column_config.TextColumn("Group Name", width="large"), # Added width hint
                    "Logo URL": st.column_config.TextColumn("Logo URL", help="Direct URL of the group logo scraped.", width="small"), # Added width hint & help
                    "Status": st.column_config.TextColumn("Status", width="small") # Added width hint
                },
                hide_index=True, # Hide the DataFrame index
                height=400,
                use_container_width=True
            )

        # --- Styled HTML Table Output Section ---
        st.subheader("✨ Styled Output (Active Groups)")
        if not active_df_display.empty:
            # Generate the HTML string using the new function
            styled_html_output = generate_styled_html_table(active_df_display)

            with st.expander("View and Copy Styled HTML / Download", expanded=True):
                st.markdown(
                    "Below is a preview of the styled output. You can copy the raw HTML code or download it.",
                    unsafe_allow_html=True
                )
                # Render the styled HTML table using unsafe_allow_html
                st.markdown(styled_html_output, unsafe_allow_html=True)

                st.text_area("Raw HTML Code (Copy this):", value=styled_html_output, height=300, key="styled_html_export_area", help="Ctrl+A then Ctrl+C to copy the HTML source code.")

                # Provide download button for the generated HTML
                st.download_button(
                    "📥 Download Styled HTML Table (.html)",
                    styled_html_output.encode('utf-8'),
                    "styled_active_groups.html",
                    "text/html",
                    use_container_width=True,
                    key="styled_html_export_download"
                )
        else:
            st.info("No active groups found to generate a styled output.")

        # Download buttons for the original DataFrame data (CSV)
        st.subheader("Downloads")
        col_dl1_orig, col_dl2_orig = st.columns(2)
        with col_dl1_orig:
            csv_active_orig = active_df_display.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Active Groups (CSV)", csv_active_orig, "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_csv_orig")
        with col_dl2_orig:
            csv_all_orig = df_results_display.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download All Results (CSV)", csv_all_orig, "all_groups.csv", "text/csv", use_container_width=True, key="dl_all_csv_orig")


    else: # From user example
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")


if __name__ == "__main__":
    # Ensure necessary libraries are available (optional check, good for UX)
    try: import openpyxl
    except ImportError: st.error("Library 'openpyxl' for Excel is missing. Please install: `pip install openpyxl`"); st.stop()
    try: from fake_useragent import UserAgent; UserAgent() # Test initialization
    except ImportError: st.warning("Library 'fake-useragent' is missing. General scraping might be less effective. Install: `pip install fake-useragent`", icon="⚠️")
    except Exception: st.warning("Fake-useragent initialized with issues. General scraping might use a default User-Agent.", icon="⚠️")

    main()
