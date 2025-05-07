import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
import io # Needed for handling file uploads with pandas
from urllib.parse import urljoin, urlparse, urlencode, parse_qs # Needed for crawl and url manipulation

# --- Import Google Search Library ---
try:
    from googlesearch import search as google_search_library # Use the direct import name
except ImportError:
    st.error("The `googlesearch-python` library is not installed. Please install it: `pip install googlesearch-python`")
    # Define a dummy function to avoid NameError if import fails
    def google_search_library(query, num_results, lang, pause):
        st.error("`googlesearch-python` library not found. Cannot perform Google searches.")
        return []

# --- Import Fake User Agent Library ---
try:
    from fake_useragent import UserAgent
    ua_general = UserAgent()
    def get_random_headers_general():
        """Returns headers with a random User-Agent for general scraping/validation."""
        try:
            return {
                "User-Agent": ua_general.random,
                "Accept-Language": "en-US,en;q=0.9"
            }
        except Exception:
             # Fallback if fake-useragent fails during lookup
             return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9"
            }
except ImportError:
    st.warning("`fake-useragent` library not found. Install with `pip install fake-useragent`. Using default User-Agent for scraping/validation.", icon="⚠️")
    # Fallback if fake-useragent is not installed
    def get_random_headers_general():
         return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
except Exception as e:
     st.warning(f"Error initializing fake-useragent: {e}. Using default User-Agent for scraping/validation.", icon="⚠️")
     def get_random_headers_general():
         return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

# --- Threading for Validation ---
# Import ThreadPoolExecutor here, ensure it's available
from concurrent.futures import ThreadPoolExecutor, as_completed


# --- Streamlit Configuration & Constants ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
# Pattern for WhatsApp profile images on pps.whatsapp.net
IMAGE_PATTERN_PPS = re.compile(r'https:\/\/pps\.whatsapp\.net\/v\/t\d+\/[-\w]+\/\d+\.jpg\?')
# Pattern for Open Graph images, more general
OG_IMAGE_PATTERN = re.compile(r'https?:\/\/[^\/\s]+\/[^\/\s]+\.(jpg|jpeg|png)(\?[^\s]*)?') # Added png and query string

MAX_VALIDATION_WORKERS = 8 # Set a reasonable number of threads for validation

# --- Custom CSS (Combined and Enhanced) ---
st.markdown("""
<style>
/* General Streamlit overrides */
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
/* Adjusted Streamlit button style for consistency */
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; margin: 5px 0; } /* Added margin for spacing */
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }

/* Streamlit Progress Bar Fix */
.stProgress > div > div > div > div {
    background-color: #25D366;
}

/* Metric cards */
.metric-card {
    background-color: #F5F6F5;
    padding: 12px;
    border-radius: 8px;
    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    color: #333333;
    text-align: center;
    margin-bottom: 10px;
}
.metric-card div[data-testid="stMetricValue"] { font-size: 1.5em; }
.metric-card div[data-testid="stMetricLabel"] { font-size: 0.9em; color: #555; }

/* Input fields */
.stTextInput, .stTextArea {
    border: 1px solid #25D366;
    border-radius: 5px;
    padding: 8px;
}

/* Sidebar background */
/* Targeting the correct Streamlit class for the sidebar content area */
.st-emotion-cache-1v3rj08 { /* This class name is highly variable and might change */
    background-color: #F5F6F5;
}

/* Expander Styling */
.stExpander {
    border: 1px solid #E0E0E0;
    border-radius: 5px;
    padding: 10px;
    margin-top: 10px;
    margin-bottom: 10px; /* Added bottom margin */
}
.stExpander div[data-testid="stExpanderToggleIcon"] {
     color: #25D366;
}
 .stExpander div[data-testid="stExpanderLabel"] strong {
     color: #25D366;
 }

/* Optional: Style the dataframe header */
.stDataFrame table th {
    background-color: #25D366;
    color: white;
}


/* --- CSS for the Styled HTML Table Output --- */
.whatsapp-groups-table {
    border-collapse: collapse;
    width: 100%;
    margin-top: 20px;
    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid #eee; /* Add a light border around the table */
}

.whatsapp-groups-table tr {
    border-bottom: 1px solid #eee;
}

.whatsapp-groups-table tr:last-child {
    border-bottom: none;
}

.whatsapp-groups-table td {
    padding: 12px; /* Increased padding slightly */
    vertical-align: middle;
    text-align: left;
}

/* Column widths (optional, adjust as needed) */
.whatsapp-groups-table td:nth-child(1) { /* Logo column */
    width: 60px; /* Increased width for logo cell */
    padding-right: 8px; /* Increased space */
    text-align: center;
}

.whatsapp-groups-table td:nth-child(2) { /* Name column */
    flex-grow: 1;
    padding-left: 8px; /* Increased space */
    padding-right: 12px; /* Increased space */
    word-break: break-word;
    font-weight: 500; /* Slightly bolder name */
    color: #333;
}

.whatsapp-groups-table td:nth-child(3) { /* Button column */
     width: 140px; /* Increased width for button cell */
     text-align: right;
     padding-left: 12px; /* Increased space */
}

/* Image styling */
.group-logo-img {
    width: 40px; /* Increased image size */
    height: 40px; /* Increased image size */
    border-radius: 50%;
    object-fit: cover;
    display: block;
    margin: 0 auto;
    border: 1px solid #eee; /* Subtle border around logo */
}

/* Join Button styling */
.join-button {
    display: inline-block;
    background-color: #25D366; /* WhatsApp Green */
    color: #FFFFFF !important; /* White text */
    padding: 8px 16px;
    border-radius: 8px;
    text-decoration: none;
    font-weight: bold;
    text-align: center;
    white-space: nowrap;
    font-size: 0.9em;
    transition: background-color 0.2s ease; /* Smooth hover transition */
}

.join-button:hover {
    background-color: #1EBE5A; /* Darker green on hover */
    color: #FFFFFF !important;
    text-decoration: none;
}
/* --- End of Styled HTML Table CSS --- */

</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---

def append_query_param(url, param_name, param_value):
    """Appends or updates a query parameter in a URL, preserving fragment."""
    if not url: return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value] # Set the parameter value
    new_query_string = urlencode(query_params, doseq=True)
    # Reconstruct URL without fragment, then add fragment back if it existed
    url_without_fragment = parsed_url._replace(query=new_query_string, fragment='').geturl()
    if parsed_url.fragment:
         return f"{url_without_fragment}#{parsed_url.fragment}"
    return url_without_fragment

def load_keywords_from_excel(uploaded_file):
    """Load keywords from the first column of an Excel file."""
    if uploaded_file is None: return []
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        # Get the first column, drop NaNs, convert to string, then to list
        keywords = df.iloc[:, 0].dropna().astype(str).tolist()
        # Optional: Add a check for minimum keyword length or filter empty strings
        keywords = [kw.strip() for kw in keywords if len(kw.strip()) > 1]
        if not keywords: st.warning("No valid keywords found in the first column of the Excel file.")
        return keywords
    except Exception as e:
        st.error(f"Error reading Excel file {uploaded_file.name}: {e}. Ensure it's a valid .xlsx file and 'openpyxl' installed.", icon="❌")
        return []

def load_links_from_file(uploaded_file):
    """Load WhatsApp group links from an uploaded TXT or CSV file, handling encoding."""
    if uploaded_file is None: return []
    try:
        # Read the file content as bytes
        content = uploaded_file.getvalue()
        text_content = None
        # Attempt decoding with common encodings
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252']
        for encoding in encodings_to_try:
            try:
                text_content = content.decode(encoding)
                st.sidebar.info(f"Successfully decoded file with {encoding}.")
                break
            except UnicodeDecodeError:
                continue # Try the next encoding

        if text_content is None:
             st.error(f"Could not decode file {uploaded_file.name} with common encodings.", icon="❌")
             return []

        # If CSV, use pandas
        if uploaded_file.name.endswith('.csv'):
            try:
                 df = pd.read_csv(io.StringIO(text_content))
                 if df.empty: st.warning("CSV file is empty."); return []
                 # Get the first column, drop NaNs, convert to string, then to list
                 links = df.iloc[:, 0].dropna().astype(str).tolist()
                 # Filter for lines that look like links to avoid junk data from CSV
                 links = [link.strip() for link in links if link.strip().startswith(('http://', 'https://'))]
                 return links
            except Exception as e:
                 st.error(f"Error reading CSV file {uploaded_file.name}: {e}. Ensure it's a valid CSV.", icon="❌")
                 return []

        # If TXT, split lines
        else: # Assume TXT
             links = [line.strip() for line in text_content.splitlines() if line.strip()]
             return links

    except Exception as e:
        st.error(f"Error processing file {uploaded_file.name}: {e}", icon="❌")
        return []

# --- Core Logic Functions (Enhanced from v2, combined with v1 needs) ---

def validate_link(link):
    """Validate a WhatsApp group link and return details if active, with enhanced logic."""
    result = {
        "Group Name": "Unknown",
        "Group Link": link,
        "Logo URL": "",
        "Status": "Error" # Default to error, update based on findings
    }
    try:
        # Use random headers for validation request
        # Set a slightly longer timeout as validation hits WhatsApp servers
        response = requests.get(link, headers=get_random_headers_general(), timeout=20, allow_redirects=True)
        response.encoding = 'utf-8' # Ensure correct decoding

        # Check HTTP status code first
        if response.status_code != 200:
            # Treat 404 as Expired, other errors as HTTP Error
            if response.status_code == 404:
                 result["Status"] = "Expired (404 Not Found)"
            else:
                result["Status"] = f"HTTP Error {response.status_code}"
            return result

        # Check if the final URL is still a WhatsApp invite link after redirects
        # This handles cases where links might redirect to a different site (e.g., landing page)
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = f"Redirected Away ({response.urlparse(response.url).netloc or 'Unknown Site'})"
            return result

        # Parse HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Check for specific text indicating an expired/invalid link (case-insensitive)
        page_text_lower = soup.get_text().lower()
        expired_phrases = [
            "invite link is invalid",
            "invite link was reset",
            "group doesn't exist",
            "this group is no longer available" # Another common phrase
        ]
        if any(phrase in page_text_lower for phrase in expired_phrases):
            result["Status"] = "Expired"
            # Even if expired, still try to get name/logo as sometimes available

        # Extract Group Name
        group_name_found = False
        # 1. From og:title (most reliable)
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = unescape(meta_title['content']).strip()
            if group_name: # Ensure it's not an empty string
                 result["Group Name"] = group_name
                 group_name_found = True

        # 2. Fallback: try to find text on the page if og:title is missing or empty
        if not group_name_found:
             # Look for text within specific tags or generally prominent text
             potential_name_tags = soup.find_all(['h2', 'strong', 'span'], class_=re.compile('group-name', re.IGNORECASE)) # Look for common classes too
             for tag in potential_name_tags:
                 text = tag.get_text().strip()
                 # Basic checks: not too short, not generic WhatsApp text
                 if text and len(text) > 2 and text.lower() != "whatsapp group invite":
                     result["Group Name"] = text
                     group_name_found = True
                     break # Found a potential name

        # Default to "Unnamed Group" if no name was found through any method
        if not group_name_found:
             result["Group Name"] = "Unnamed Group"


        # Extract Logo URL
        logo_found = False
        # 1. Check og:image first (often high-res)
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
             src = unescape(meta_image['content'])
             # Basic validation that it looks like an image URL (ends with common image extensions)
             if OG_IMAGE_PATTERN.match(src) or src.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                 result["Logo URL"] = src
                 logo_found = True

        # 2. Check img tags with the specific pps.whatsapp.net pattern if og:image wasn't found or wasn't suitable
        if not logo_found:
            img_tags = soup.find_all('img', src=True)
            for img in img_tags:
                src = unescape(img['src'])
                # Use a more flexible regex or just check the start
                if src.startswith('https://pps.whatsapp.net/'): # Simpler check
                    result["Logo URL"] = src
                    logo_found = True
                    break

        # Determine final status if not already set to Error, HTTP Error, Redirected, or Expired by content
        if result["Status"] == "Error": # If status is still the default "Error"
             # If we reached here, HTTP was 200, not redirected, no explicit expired text found.
             # It's very likely an active group.
             result["Status"] = "Active"
             # Optional: Indicate if logo wasn't scraped successfully for 'Active' links
             # if not logo_found:
             #     result["Status"] = "Active (No Logo Scraped)"


    except requests.exceptions.Timeout:
        result["Status"] = "Timeout Error"
    except requests.exceptions.ConnectionError:
        result["Status"] = "Connection Error"
    except requests.exceptions.RequestException as e:
        result["Status"] = f"Network Error: {type(e).__name__}" # More specific error type
    except Exception as e:
        result["Status"] = f"Parsing Error: {type(e).__name__}" # More specific error type

    return result


def scrape_whatsapp_links_from_page(url, session=None):
    """Scrape WhatsApp group links from a webpage using random headers and optional session."""
    links = set() # Use a set to automatically handle duplicates
    try:
        headers = get_random_headers_general()
        # Use the provided session if available, otherwise use requests directly
        if session:
            response = session.get(url, headers=headers, timeout=15)
        else:
            response = requests.get(url, headers=headers, timeout=15)

        response.encoding = 'utf-8'
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find links in href attributes
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                # Normalize the link by removing query parameters and fragments
                parsed_url = urlparse(href)
                links.add(parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path)

        # Find links directly in text content
        # Process text nodes or stripped strings for potential links not in <a> tags
        text_content = soup.get_text() # Use get_text() which often includes more text than stripped_strings
        if WHATSAPP_DOMAIN in text_content:
             # Find potential links in the text, robustly handling surrounding punctuation
            # Regex: Looks for http/https, chat.whatsapp.com, followed by non-whitespace/quote/bracket chars
            found_in_chunk = re.findall(r'(https?://chat\.whatsapp\.com/[^\s"\'<>()\[\]{}]+)', text_content)
            for link_url in found_in_chunk:
                 # Basic cleanup for trailing punctuation like periods or commas right after the link
                clean_link = re.sub(r'[.,;!?"\'<>)]+$', '', link_url)
                parsed_url = urlparse(clean_link)
                links.add(parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path)

    except requests.exceptions.Timeout: st.sidebar.warning(f"Scrape Timeout: {url[:50]}...", icon="⏱️")
    except requests.exceptions.HTTPError as e: st.sidebar.warning(f"Scrape HTTP Error {e.response.status_code}: {url[:50]}...", icon="⚠️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape Network Error ({type(e).__name__}): {url[:50]}...", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Scrape Parsing Error ({type(e).__name__}): {url[:50]}...", icon="💣")

    return list(links) # Convert back to list for processing


def google_search_and_scrape(query, top_n=5, pause_duration=2.0):
    """Fetch URLs from Google, then scrape WhatsApp links from those pages."""
    st.info(f"Searching Google for '{query}' (top {top_n} results, pause: {pause_duration}s)...")
    all_links = set() # Use set for unique links
    try:
        # Use the imported search function
        search_page_urls = list(google_search_library(query, num_results=top_n, lang="en", pause=pause_duration))
        if not search_page_urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms.")
            return []

        st.success(f"Google Search found {len(search_page_urls)} results. Scraping WhatsApp links from pages...")

        # Create a placeholder for scraping progress and status
        scrape_prog_bar = st.progress(0)
        scrape_status_text = st.empty()

        for idx, url in enumerate(search_page_urls):
            scrape_status_text.text(f"Scraping links from page {idx + 1}/{len(search_page_urls)}: {url[:60]}...")
            # Use the general scrape function without a session here, as we process page by page
            links_from_page = scrape_whatsapp_links_from_page(url)
            # Filter for only WhatsApp domain links immediately
            whatsapp_only_links = {link for link in links_from_page if link.startswith(WHATSAPP_DOMAIN)}
            all_links.update(whatsapp_only_links)
            scrape_prog_bar.progress((idx + 1) / len(search_page_urls))

        scrape_status_text.success(f"Finished scraping webpages. Found {len(all_links)} unique WhatsApp links.")
        return list(all_links)

    except Exception as e:
        st.error(f"An error occurred during Google search or scraping: {type(e).__name__} - {str(e)}. The googlesearch library can be unreliable.", icon="❌")
        return list(all_links) # Return any links found before the error


def crawl_website(start_url, max_depth=2, max_pages=50):
    """Crawls a website to find pages, then scrapes links from those pages."""
    if not start_url.strip(): return set() # Handle empty input

    # Ensure URL has scheme, default to https
    if not start_url.startswith(('http://', 'https://')):
         start_url = 'https://' + start_url
         st.sidebar.warning(f"Prepending 'https://' to URL: {start_url}", icon="🔗")

    parsed_start_url = urlparse(start_url)
    # Basic validation for the start URL
    if not parsed_start_url.netloc:
        st.sidebar.error(f"Invalid start URL provided: {start_url}", icon="🚫")
        return set() # Return empty set if URL is invalid

    base_domain = parsed_start_url.netloc.replace('www.', '') # Use root domain for comparison
    # Use sets for efficiency in checking visited/to_visit
    urls_to_visit = {(start_url, 0)} # Store as (url, depth) tuples
    visited_urls = set() # Stores normalized URLs that have been requested
    scraped_whatsapp_links = set() # Stores unique WhatsApp links found

    # Add normalized start URL path to visited to avoid immediate re-crawl
    normalized_start_path = urljoin(start_url, parsed_start_url.path or '/')
    visited_urls.add(normalized_start_path)

    session = requests.Session() # Use a session for potentially better performance/connection reuse during crawl
    st.sidebar.info(f"Starting crawl on `{base_domain}` (Max Depth: {max_depth}, Max Pages: {max_pages})...")

    page_count = 0
    queue_list = [(start_url, 0)] # Use a list to maintain FIFO order for BFS

    with st.spinner(f"Crawling {base_domain}..."):
        while queue_list and page_count < max_pages:
            current_url, depth = queue_list.pop(0) # Get next URL from the queue

            # Normalize current URL for consistent visited check
            normalized_current_url = urljoin(current_url, urlparse(current_url).path or '/')

            # Check visited status again after popping (might have been added while in queue)
            if normalized_current_url in visited_urls or depth > max_depth:
                # st.sidebar.markdown(f"<span style='color:#888; font-size:0.8em;'>Skipping queue: {normalized_current_url[:50]}... (Visited/Depth)</span>", unsafe_allow_html=True)
                continue # Skip if already visited or beyond max depth

            visited_urls.add(normalized_current_url) # Mark as visited before requesting

            if page_count >= max_pages: break # Check limit before request

            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}): {current_url[:60]}...")

            try:
                response = session.get(current_url, headers=get_random_headers_general(), timeout=10)
                response.raise_for_status() # Raise HTTPError for bad responses

                # Check Content-Type
                if 'text/html' not in response.headers.get('Content-Type', '').lower():
                     st.sidebar.markdown(f"<span style='color:#888; font-size:0.8em;'>Skipping crawl: {current_url[:50]}... (Not HTML)</span>", unsafe_allow_html=True)
                     continue # Skip non-HTML content

                page_count += 1 # Only increment page count for successful HTML requests

                # Scrape WhatsApp links from this page using the enhanced scraper
                links_from_page = scrape_whatsapp_links_from_page(current_url, session=session)
                whatsapp_only_links = {link for link in links_from_page if link.startswith(WHATSAPP_DOMAIN)}
                scraped_whatsapp_links.update(whatsapp_only_links)


                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag.get('href')
                        if href:
                            abs_url = urljoin(current_url, href)
                            parsed_abs_url = urlparse(abs_url)

                            # Basic checks for valid scheme, same base domain, and no fragment
                            if parsed_abs_url.scheme in ['http', 'https'] and \
                               parsed_abs_url.netloc.replace('www.', '') == base_domain and \
                               not parsed_abs_url.fragment:

                                normalized_abs_url = urljoin(abs_url, parsed_abs_url.path or '/')
                                # Check if the normalized URL has been visited or is already in the queue
                                if normalized_abs_url not in visited_urls and (abs_url, depth + 1) not in urls_to_visit:
                                    queue_list.append((abs_url, depth + 1))
                                    urls_to_visit.add((abs_url, depth + 1)) # Keep track of URLs added to queue


            except requests.exceptions.Timeout: st.sidebar.markdown(f"<span style='color:orange; font-size:0.8em;'>Timeout: {current_url[:50]}...</span>", unsafe_allow_html=True)
            except requests.exceptions.HTTPError as e: st.sidebar.markdown(f"<span style='color:orange; font-size:0.8em;'>HTTP Error {e.response.status_code}: {current_url[:50]}...</span>", unsafe_allow_html=True)
            except requests.exceptions.ConnectionError: st.sidebar.markdown(f"<span style='color:red; font-size:0.8em;'>Conn Error: {current_url[:50]}...</span>", unsafe_allow_html=True)
            except requests.exceptions.RequestException as e: st.sidebar.markdown(f"<span style='color:orange; font-size:0.8em;'>Req Error ({type(e).__name__}): {current_url[:50]}...</span>", unsafe_allow_html=True)
            except Exception as e: st.sidebar.markdown(f"<span style='color:red; font-size:0.8em;'>Parse/Other Error ({type(e).__name__}): {current_url[:50]}...</span>", unsafe_allow_html=True)

            # Optional: Add a small delay between requests to be polite during crawl
            # time.sleep(0.05) # Small delay

    session.close() # Close the session when done
    st.sidebar.success(f"Crawl finished. Scraped {page_count} pages and found {len(scraped_whatsapp_links)} unique WhatsApp links.")
    if page_count >= max_pages:
         st.sidebar.warning(f"Stopped crawl after reaching maximum pages ({max_pages}).", icon="❗️")

    return scraped_whatsapp_links # Return the set of unique WhatsApp links found


# --- Function to generate the styled HTML table ---
def generate_styled_html_table(active_results_df):
    """Generates an HTML string for a styled table of active groups."""
    if active_results_df.empty:
        return "<p>No active groups found to generate a styled table.</p>"

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
            # Use a width param if it's a pps.whatsapp.net URL, otherwise use original
            if logo_url.startswith('https://pps.whatsapp.net/'):
                display_logo_url = append_query_param(logo_url, 'w', '96') # Request 96px width
            else:
                 display_logo_url = logo_url # Use original for non-PPS URLs

            # Use the custom image class for styling (size, circle, etc.)
            html_string += f'<img src="{display_logo_url}" alt="Group Logo" class="group-logo-img">'
        else:
             # Add a placeholder div to maintain cell structure and look like a grey circle
             html_string += '<div class="group-logo-img" style="background-color:#e0e0e0; display: flex; align-items: center; justify-content: center; font-size: 0.8em; color: #888;">?</div>' # Placeholder grey circle with a question mark
        html_string += '</td>'

        # Cell 2: Group Name
        # Sanitize group name for HTML display (basic entity escaping)
        safe_group_name = group_name.replace('&', '&').replace('<', '<').replace('>', '>').replace('"', '"').replace("'", ''')
        html_string += f'<td class="group-name-cell">{safe_group_name}</td>'

        # Cell 3: Join Button
        html_string += '<td class="join-button-cell">'
        # Use the custom button class for styling the link
        if group_link and group_link.startswith(WHATSAPP_DOMAIN):
             html_string += f'<a href="{group_link}" class="join-button" target="_blank">Join Group</a>'
        else:
             # Handle case where link is missing or invalid (shouldn't happen for active groups normally)
             html_string += '<span style="color:#888; font-size:0.9em;">Link N/A</span>'
        html_string += '</td>'

        # End the table row
        html_string += '</tr>'

    # End the table body and table
    html_string += '</tbody></table>'

    return html_string


# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Find, scrape, and validate WhatsApp group links from various sources.</p>', unsafe_allow_html=True)

    # Initialize session state variables safely
    # 'results': List of dictionaries, each representing a validated link result
    # 'processed_links_in_session': Set of normalized WhatsApp links that have been added to results
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()

    # Ensure processed_links_in_session is a set if it somehow got changed
    if not isinstance(st.session_state.processed_links_in_session, set):
         st.session_state.processed_links_in_session = set()
         # Re-populate set from existing results if necessary (handles potential state migration)
         if isinstance(st.session_state.results, list):
              for res in st.session_state.results:
                   if 'Group Link' in res and res['Group Link'].startswith(WHATSAPP_DOMAIN):
                        try:
                             parsed_url = urlparse(res['Group Link'])
                             normalized_link = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
                             st.session_state.processed_links_in_session.add(normalized_link)
                        except Exception:
                             pass # Ignore malformed links

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google",
            "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL",
            "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)",
            "Upload Link File (TXT/CSV for Validation)"
        ], key="input_method_main_select") # Unique key

        # Settings for Google Search Methods
        google_results_slider_top_n = 5
        google_search_pause = 2.0
        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)"]:
            google_results_slider_top_n = st.slider(
                "Number of top Google results to scrape from",
                min_value=1, max_value=20, value=5, key="google_top_n_slider",
                 help="How many top web results from Google Search to visit and scrape links from."
            )
            google_search_pause = st.slider(
                "Google Search Pause (seconds):", min_value=0.5, max_value=10.0, value=2.0, step=0.5,
                help="Pause between Google search queries. Increase if encountering rate limits.", key="google_pause_slider"
            )

        # Settings for Extensive Crawl
        crawl_depth_val, max_crawl_pages_val = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive website crawling can be slow and resource-intensive. Use with caution.", icon="🚨")
            st.info("Crawler attempts to stay within the base domain. Depth 0 = only the starting page.", icon="ℹ️")
            crawl_depth_val = st.slider("Max Crawl Depth:", min_value=0, max_value=5, value=2, key="crawl_depth_slider")
            max_crawl_pages_val = st.slider("Max Pages to Crawl:", min_value=1, max_value=300, value=50, key="crawl_pages_slider")

        st.markdown("---") # Separator

        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all_button"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.cache_data.clear() # Clear Streamlit's data cache
            st.success("All results and cache cleared!")
            st.rerun() # Rerun to clear the display immediately

    # Set to collect all *new* unique WhatsApp links found from the current input action
    # This set will be used to determine which links need validation
    current_action_scraped_links = set()

    st.subheader(f"🚀 Action Zone: {input_method}")

    # --- Input Method Logic ---
    try:
        if input_method == "Search and Scrape from Google":
            keyword_gs = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_keyword_input")
            if st.button("Search, Scrape, and Validate", use_container_width=True, key="gs_button"):
                if not keyword_gs: st.warning("Please enter a search query.")
                else:
                    links_from_google = google_search_and_scrape(keyword_gs, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                    current_action_scraped_links.update(links_from_google)


        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file_bulk = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if excel_file_bulk and st.button("Process Excel & Scrape from Google", use_container_width=True, key="gs_bulk_button"):
                keywords_bulk = load_keywords_from_excel(excel_file_bulk)
                if not keywords_bulk: st.warning("No keywords found or processed from Excel.")
                else:
                    st.info(f"Processing {len(keywords_bulk)} keywords for Google search & scraping...")
                    prog_bulk, stat_txt_bulk = st.progress(0), st.empty()
                    total_links_found_bulk = 0
                    for i, kw_bulk in enumerate(keywords_bulk):
                        stat_txt_bulk.text(f"Processing Keyword: **{kw_bulk}** ({i+1}/{len(keywords_bulk)}). Found {total_links_found_bulk} links so far.")
                        links_for_keyword = google_search_and_scrape(kw_bulk, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                        current_action_scraped_links.update(links_for_keyword)
                        total_links_found_bulk = len(current_action_scraped_links)
                        prog_bulk.progress((i + 1) / len(keywords_bulk))
                    stat_txt_bulk.success(f"Bulk Google processing complete. Found a total of {len(current_action_scraped_links)} unique links.")


        elif input_method == "Scrape from Specific Webpage URL":
            page_url_specific = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
            if st.button("Scrape Page & Validate", use_container_width=True, key="specific_url_button"):
                if not page_url_specific or not (page_url_specific.startswith("http://") or page_url_specific.startswith("https://")):
                    st.warning("Please enter a valid URL starting with http:// or https://")
                else:
                    with st.spinner(f"Scraping {page_url_specific}..."):
                        # Use scrape_whatsapp_links_from_page without session for a single page
                        links_from_page_spec = scrape_whatsapp_links_from_page(page_url_specific)
                        current_action_scraped_links.update(links_from_page_spec)
                    st.success(f"Scraping of {page_url_specific} complete. Found {len(current_action_scraped_links)} unique links.")


        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url_crawl = st.text_input("Enter Base Domain URL:", placeholder="example.com or https://example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape", use_container_width=True, key="crawl_button"):
                if not domain_url_crawl: st.warning("Please enter a domain URL.")
                else:
                    st.info("Starting extensive crawl. Progress will be shown in the sidebar.")
                    # crawl_website returns a set of whatsapp links
                    links_from_crawl = crawl_website(domain_url_crawl, max_depth=crawl_depth_val, max_pages=max_crawl_pages_val)
                    current_action_scraped_links.update(links_from_crawl)
                    st.success(f"Website crawl and scraping complete. Found {len(current_action_scraped_links)} unique links.")


        elif input_method == "Enter Links Manually (for Validation)":
            links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123", key="manual_links_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                links_manual = [line.strip() for line in links_text_manual.split('\n') if line.strip()]
                if not links_manual:
                    st.warning("Please enter at least one link.")
                else:
                    # Filter for valid WhatsApp links right away
                    whatsapp_only_links = {link for link in links_manual if link.strip().startswith(WHATSAPP_DOMAIN)}
                    if len(whatsapp_only_links) < len(links_manual):
                         st.warning(f"Skipped {len(links_manual) - len(whatsapp_only_links)} lines that did not start with '{WHATSAPP_DOMAIN}'")
                    current_action_scraped_links.update(whatsapp_only_links)


        elif input_method == "Upload Link File (TXT/CSV for Validation)":
            uploaded_file_val = st.file_uploader("Upload TXT or CSV", type=["txt", "csv", "xlsx"], key="file_upload_input") # Added xlsx type
            if uploaded_file_val and st.button("Validate File Links", use_container_width=True, key="upload_validate_button"):
                 # Determine file type and load accordingly
                 if uploaded_file_val.name.endswith('.xlsx'):
                      st.info("Loading keywords/links from Excel...")
                      # For Excel, treat as keywords for Google search by default
                      keywords_from_file = load_keywords_from_excel(uploaded_file_val)
                      if keywords_from_file:
                           st.info(f"Loaded {len(keywords_from_file)} keywords from Excel. Starting Google searches...")
                           # Perform Google search for each keyword, scrape pages, collect links
                           prog_file_bulk, stat_txt_file_bulk = st.progress(0), st.empty()
                           total_links_found_file_bulk = 0
                           for i, kw_file in enumerate(keywords_from_file):
                               stat_txt_file_bulk.text(f"Processing Keyword: **{kw_file}** ({i+1}/{len(keywords_from_file)}). Found {total_links_found_file_bulk} links so far.")
                               links_for_keyword_file = google_search_and_scrape(kw_file, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                               current_action_scraped_links.update(links_for_keyword_file)
                               total_links_found_file_bulk = len(current_action_scraped_links)
                               prog_file_bulk.progress((i + 1) / len(keywords_from_file))
                           stat_txt_file_bulk.success(f"File processing complete. Found a total of {len(current_action_scraped_links)} unique links.")
                      else:
                           st.warning("No keywords found in the Excel file.")

                 else: # Assume TXT or CSV
                    st.info("Loading links from TXT/CSV file...")
                    links_from_file = load_links_from_file(uploaded_file_val)
                    if not links_from_file:
                        st.warning("No links found or processed from the uploaded file.")
                    else:
                        # Filter for valid WhatsApp links right away
                        whatsapp_only_links_file = {link for link in links_from_file if link.strip().startswith(WHATSAPP_DOMAIN)}
                        if len(whatsapp_only_links_file) < len(links_from_file):
                             st.warning(f"Skipped {len(links_from_file) - len(whatsapp_only_links_file)} lines that did not start with '{WHATSAPP_DOMAIN}'")
                        current_action_scraped_links.update(whatsapp_only_links_file)


    except Exception as e:
        st.error(f"An unexpected error occurred during the input or scraping phase: {e}", icon="💥")


    # --- Unified Validation Step ---
    # Determine which links are new and need validation
    links_to_validate_now = list(current_action_scraped_links - st.session_state.processed_links_in_session)

    if links_to_validate_now: # Only proceed if there are new links to validate
        st.success(f"Found {len(current_action_scraped_links)} unique links from the last input(s). Validating {len(links_to_validate_now)} new links...")

        # Create placeholders for the progress bar and status text during validation
        validation_prog_bar = st.progress(0)
        validation_status_text = st.empty()

        new_validation_results = [] # List to hold results from this validation run

        # Use ThreadPoolExecutor for concurrent validation
        with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
            # Submit validation tasks for the new links
            future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}

            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_link)):
                link = future_to_link[future]
                try:
                    result = future.result()
                    new_validation_results.append(result)
                except Exception as exc:
                    # Catch exceptions from the validation thread itself
                    new_validation_results.append({
                        "Group Name": "Validation Failed",
                        "Group Link": link,
                        "Logo URL": "",
                        "Status": f"Validation Exception: {type(exc).__name__}"
                    })
                    # Optional: Log error to sidebar or console for debugging
                    st.sidebar.error(f"Validation error for {link[:50]}...: {exc}", icon="❗")

                # Mark the link as processed regardless of validation outcome
                # Use the normalized link for consistent tracking
                try:
                    parsed_link = urlparse(link)
                    normalized_link = parsed_link.scheme + "://" + parsed_link.netloc + parsed_link.path
                    st.session_state.processed_links_in_session.add(normalized_link)
                except Exception:
                     st.sidebar.warning(f"Could not normalize link for tracking: {link[:50]}...", icon="⚠️")
                     st.session_state.processed_links_in_session.add(link) # Add original if normalization fails


                # Update progress and status text
                validation_prog_bar.progress((i + 1) / len(links_to_validate_now))
                validation_status_text.text(f"Validated {i + 1}/{len(links_to_validate_now)} links")

        # Append the new validation results to the session state's main results list
        st.session_state.results.extend(new_validation_results)

        validation_status_text.success(f"Validation complete for {len(links_to_validate_now)} new links!")

    elif current_action_scraped_links and not links_to_validate_now:
         st.info("No *new* WhatsApp links found from the last input. All links were previously processed in this session.")
    # If current_action_scraped_links is empty, the initial message "Start by..." will be shown below.


    # --- Display Results ---
    # Check if there are any results in session state before displaying
    if 'results' in st.session_state and st.session_state.results:
        # Convert list of dicts to DataFrame for easy manipulation and display
        # Drop duplicates based on Group Link, keeping the first occurrence encountered historically
        df = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first').reset_index(drop=True)
        # Update session state with the cleaned DataFrame records - this ensures df is always consistent
        st.session_state.results = df.to_dict('records')

        # Filter data for the summary metrics based on the cleaned df
        active_df = df[df['Status'].str.contains('Active', na=False)].copy()
        # Filter for 'Expired' status explicitly
        expired_df = df[df['Status'] == 'Expired'].copy()
        # Filter for any status indicating an error or failure
        error_statuses = ['HTTP Error', 'Redirected Away', 'Timeout Error', 'Connection Error', 'Network Error', 'Parsing Error', 'Validation Failed', 'Unknown']
        # Use str.contains for more flexibility, covering cases like 'HTTP Error 404', 'Network Error: Timeout', etc.
        error_df = df[df['Status'].str.contains('Error|Redirected|Timeout|Connection|Failed|Unknown', na=False)].copy()


        st.subheader("📊 Results Summary")
        # Updated columns to include Error count
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Links", len(df))
            st.markdown('</div>', unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Active Links", len(active_df))
            st.markdown('</div>', unsafe_allow_html=True)
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Expired Links", len(expired_df))
            st.markdown('</div>', unsafe_allow_html=True)
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Error Links", len(error_df))
            st.markdown('</div>', unsafe_allow_html=True)


        with st.expander("🔎 View and Filter Results Table", expanded=True):
            # Get all unique statuses for the multiselect filter options from the full DataFrame
            all_statuses = df['Status'].unique().tolist()
            # Default selection: Active statuses if they exist, otherwise the first status found
            default_statuses = [s for s in all_statuses if 'Active' in s]
            # Add 'Expired' to default if it exists and Active wasn't selected
            if 'Expired' in all_statuses and not default_statuses: default_statuses = ['Expired']
            # If no statuses selected by default rules, pick the first one if any exist
            if not default_statuses and all_statuses: default_statuses = [all_statuses[0]]
            # Ensure default statuses are actually in the available options
            default_statuses = [s for s in default_statuses if s in all_statuses]


            status_filter = st.multiselect(
                "Filter by Status",
                options=all_statuses,
                default=default_statuses,
                key="status_filter_multiselect"
            )

            # Apply the filter
            filtered_df_for_display = df[df['Status'].isin(status_filter)] if status_filter else df.copy() # Use .copy() to avoid SettingWithCopyWarning


            # Display the filtered DataFrame
            st.dataframe(
                filtered_df_for_display,
                column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Group", width="medium"),
                    "Group Name": st.column_config.TextColumn("Group Name", width="large"),
                    "Logo URL": st.column_config.LinkColumn("Logo URL", display_text="View Logo", width="small", help="Direct URL of the group logo scraped."),
                    "Status": st.column_config.TextColumn("Status", width="small")
                },
                hide_index=True,
                height=400,
                use_container_width=True
            )

        # --- Styled HTML Table Output Section ---
        st.subheader("✨ Styled Output (Active Groups)")
        if not active_df.empty:
            # Generate the HTML string using the styled function
            styled_html_output = generate_styled_html_table(active_df)

            with st.expander("View and Copy Styled HTML / Download", expanded=True):
                st.markdown(
                    "Below is a preview of the styled output. You can copy the raw HTML code or download it.",
                    unsafe_allow_html=True
                )
                # Render the styled HTML table
                st.markdown(styled_html_output, unsafe_allow_html=True)

                st.text_area("Raw HTML Code (Copy this):", value=styled_html_output, height=300, key="styled_html_export_area", help="Ctrl+A then Ctrl+C to copy the HTML source code.")

                # Provide download button for the generated HTML
                st.download_button(
                    "📥 Download Styled HTML Table (.html)",
                    styled_html_output.encode('utf-8'), # Encode to UTF-8
                    "styled_active_groups.html",
                    "text/html",
                    use_container_width=True,
                    key="styled_html_export_download"
                )
        else:
            st.info("No active groups found to generate a styled output.")


        # Download buttons for the original DataFrame data (CSV)
        st.subheader("Downloads")
        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            # Encode CSV to UTF-8 before downloading
            csv_active = active_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Active Groups (CSV)",
                csv_active,
                "active_groups.csv",
                "text/csv",
                use_container_width=True,
                key="dl_active_csv"
            )
        with col_dl2:
            # Encode CSV to UTF-8 before downloading
            csv_all = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download All Results (CSV)",
                csv_all,
                "all_groups.csv",
                "text/csv",
                use_container_width=True,
                key="dl_all_csv"
            )

    else:
        # This message is shown initially when session state results is empty
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")

# Boilerplate to run the app
if __name__ == "__main__":
    # Check for essential libraries
    missing_libs = []
    try: import requests
    except ImportError: missing_libs.append('requests')
    try: import bs4 # BeautifulSoup is part of bs4
    except ImportError: missing_libs.append('bs4')
    try: import pandas
    except ImportError: missing_libs.append('pandas')
    try: import openpyxl # Needed for Excel
    except ImportError: missing_libs.append('openpyxl (for Excel)')
    # googlesearch and fake-useragent are handled by their import blocks

    if missing_libs:
        st.error(f"The following required libraries are not installed: {', '.join(missing_libs)}. Please install them using pip (e.g., `pip install requests bs4 pandas openpyxl`).")
        st.stop() # Stop the app if essential libraries are missing

    # Run the main function
    main()
