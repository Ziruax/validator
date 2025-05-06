import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_lib # Renamed to avoid conflict
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io # For excel download in new pandas versions

# --- Streamlit Configuration & Constants ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
# More specific image pattern to catch various WhatsApp CDN formats
IMAGE_PATTERN = re.compile(r'https://pps\.whatsapp\.net/[^"\s]+')
# User agent for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}
MAX_VALIDATION_WORKERS = 10 # Increased for potentially more links

# --- Custom CSS ---
st.markdown("""
<style>
/* ... (keeping existing CSS, can be expanded if needed) ... */
.main-title {
    font-size: 2.5em;
    color: #25D366;
    text-align: center;
    margin-bottom: 0;
    font-weight: bold;
}
.subtitle {
    font-size: 1.2em;
    color: #4A4A4A;
    text-align: center;
    margin-top: 0;
}
.stButton>button {
    background-color: #25D366;
    color: #FFFFFF;
    border-radius: 8px;
    font-weight: bold;
    border: none;
    padding: 8px 16px;
}
.stButton>button:hover {
    background-color: #1EBE5A;
    color: #FFFFFF;
}
.stProgress .st-bo { /* Progress bar color */
    background-color: #25D366;
}
.metric-card {
    background-color: #F5F6F5;
    padding: 12px;
    border-radius: 8px;
    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    color: #333333;
    text-align: center;
}
.stTextInput input, .stTextArea textarea, .stFileUploader div[data-testid="stFileUploadDropzone"] {
    border: 1px solid #25D366 !important;
    border-radius: 5px !important;
}
.sidebar .sidebar-content {
    background-color: #F5F6F5;
}
.stExpander {
    border: 1px solid #E0E0E0;
    border-radius: 5px;
}
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---

def append_query_param(url, param_name, param_value):
    """Appends a query parameter to a URL, handling existing params."""
    if not url:
        return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return parsed_url._replace(query=new_query_string).geturl()

def validate_link(link):
    """Validate a WhatsApp group link and return details if active."""
    result = {
        "Group Name": "Unknown",
        "Group Link": link,
        "Logo URL": "",
        "Status": "Error" # Default status
    }
    try:
        response = requests.get(link, headers=HEADERS, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'

        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result

        # Check if the final URL is a WhatsApp invite link
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid Link (Redirected)"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Group Name from <meta property="og:title">
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = unescape(meta_title['content']).strip()
            result["Group Name"] = group_name or "Unnamed Group"
        else: # Fallback: Try to find h3 with class '_9vd5' or similar, more fragile
            title_tag = soup.find('h3') # Simplified fallback
            if title_tag:
                 result["Group Name"] = unescape(title_tag.get_text(strip=True)) or "Unnamed Group"
            else:
                result["Group Name"] = "Unnamed Group"


        # Group Logo from <meta property="og:image"> or <img> tag
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            img_src = unescape(meta_image['content'])
            if IMAGE_PATTERN.match(img_src):
                 result["Logo URL"] = img_src
        
        if not result["Logo URL"]: # Fallback to img tags if meta tag not found or not matching
            img_tags = soup.find_all('img', src=True)
            for img in img_tags:
                src = unescape(img['src'])
                if IMAGE_PATTERN.match(src):
                    result["Logo URL"] = src
                    break
        
        # Determine status based on content (presence of join button or specific text)
        # This is a heuristic. WhatsApp might change its page structure.
        join_button_texts = ["Join Chat", "Join Group", "View Group"] # Add more if known
        page_text_lower = soup.get_text().lower()
        
        if any(btn_text.lower() in page_text_lower for btn_text in join_button_texts) or \
           "you can join this group" in page_text_lower or \
           result["Group Name"] != "Unnamed Group": # If we got a name, likely active
            result["Status"] = "Active"
        elif "link is invalid" in page_text_lower or "link has been revoked" in page_text_lower or "link expired" in page_text_lower:
            result["Status"] = "Expired/Invalid"
        else:
            # If we found a logo but no clear join/error message, assume active for now
            # Or if we found a group name, assume active
            if result["Logo URL"] or result["Group Name"] not in ["Unknown", "Unnamed Group"]:
                 result["Status"] = "Active" # Potentially active
            else:
                 result["Status"] = "Expired/Invalid" # Default if no positive indicators

    except requests.exceptions.Timeout:
        result["Status"] = "Timeout Error"
    except requests.exceptions.RequestException as e:
        result["Status"] = f"Network Error" # Simplified: {str(e)[:50]}...
    except Exception as e:
        result["Status"] = f"Parsing Error" # Simplified: {str(e)[:50]}...
    
    return result

def scrape_whatsapp_links_from_url(url, session):
    """Scrape WhatsApp group links from a single webpage."""
    links = set()
    try:
        response = session.get(url, headers=HEADERS, timeout=10)
        response.encoding = 'utf-8' # Ensure correct encoding
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find links in <a> tags
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith(WHATSAPP_DOMAIN):
                clean_link = href.split('?')[0] # Basic normalization
                links.add(clean_link)

        # Find links in plain text (more robust regex)
        text_content = soup.get_text()
        found_in_text = re.findall(r'https?://chat\.whatsapp\.com/([a-zA-Z0-9_-]+)', text_content)
        for code in found_in_text:
            links.add(f"{WHATSAPP_DOMAIN}{code}")
            
    except requests.exceptions.RequestException as e:
        st.sidebar.warning(f"Failed to scrape {url}: {type(e).__name__}", icon="⚠️")
    except Exception as e:
        st.sidebar.warning(f"Error processing {url}: {type(e).__name__}", icon="💣")
    return list(links)

def perform_google_search(query, num_results):
    """Fetch URLs from Google's search results."""
    try:
        # The 'num' parameter in googlesearch library is actually num_results
        urls = list(google_search_lib(query, num_results=num_results, lang="en", sleep_interval=1))
        if not urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms.")
        return urls
    except Exception as e:
        st.error(f"Google search error for '{query}': {e}")
        return []

def crawl_website(start_url, max_depth=2, max_pages=20):
    """Crawl a website to find internal pages for scraping."""
    if not start_url.startswith(('http://', 'https://')):
        start_url = 'http://' + start_url # Default to http if no scheme

    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    
    urls_to_visit = [(start_url, 0)]
    visited_urls = set()
    scraped_content_urls = set() # URLs from which content will be scraped
    
    session = requests.Session()

    with st.spinner(f"Crawling {base_domain}..."):
        while urls_to_visit and len(scraped_content_urls) < max_pages:
            current_url, depth = urls_to_visit.pop(0)

            if current_url in visited_urls or depth > max_depth:
                continue
            
            visited_urls.add(current_url)
            st.sidebar.text(f"Crawling (D:{depth}): {current_url[:70]}...")

            try:
                response = session.get(current_url, headers=HEADERS, timeout=7)
                response.encoding = response.apparent_encoding # Try to guess encoding
                scraped_content_urls.add(current_url) # Add for scraping later

                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag['href']
                        abs_url = urljoin(current_url, href)
                        parsed_abs_url = urlparse(abs_url)

                        # Check if it's an HTTP/HTTPS URL and on the same domain
                        if parsed_abs_url.scheme in ['http', 'https'] and parsed_abs_url.netloc == base_domain:
                            if abs_url not in visited_urls and (abs_url, depth + 1) not in urls_to_visit:
                                urls_to_visit.append((abs_url, depth + 1))
            except requests.exceptions.RequestException as e:
                st.sidebar.warning(f"Crawler error on {current_url}: {type(e).__name__}", icon="🕸️")
            except Exception as e:
                st.sidebar.error(f"Unexpected crawler error: {e}", icon="💥")

    st.sidebar.success(f"Crawler finished. Found {len(scraped_content_urls)} pages to scrape from.")
    return list(scraped_content_urls), session


def load_links_from_text_file(uploaded_file):
    """Load links from an uploaded TXT or CSV file."""
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
            if df.empty: return []
            # Try to find a column with 'whatsapp.com' links, otherwise take first column
            for col in df.columns:
                if df[col].astype(str).str.contains(WHATSAPP_DOMAIN, case=False, na=False).any():
                    return df[col].dropna().astype(str).tolist()
            return df.iloc[:, 0].dropna().astype(str).tolist() # Fallback to first column
        else: # TXT file
            return [line.decode().strip() for line in uploaded_file.readlines() if line.strip()]
    except Exception as e:
        st.error(f"Error reading file {uploaded_file.name}: {e}")
        return []

def load_keywords_from_excel(uploaded_file):
    """Load keywords from the first column of an uploaded Excel file."""
    try:
        # For newer pandas, BytesIO might be needed for Streamlit's UploadedFile
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty:
            st.warning("Excel file is empty.")
            return []
        # Assume keywords are in the first column
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        st.error(f"Error reading Excel file {uploaded_file.name}: {e}. Make sure 'openpyxl' is installed.")
        return []

def generate_markdown_output(active_results_df):
    """Generates Markdown table for active groups."""
    if active_results_df.empty:
        return "No active groups found to generate Markdown."

    markdown_lines = []
    markdown_lines.append("| Group Logo | Group Name | Group Link |")
    markdown_lines.append("|---|---|---|")

    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", "N/A")
        group_link = row.get("Group Link", "")

        if logo_url:
            # Append &w=200 for resizing, handling existing query parameters
            logo_md = f"![Logo]({append_query_param(logo_url, 'w', '200')})"
        else:
            logo_md = " " # Empty for no logo, or use placeholder text like "No Logo"

        link_md = f"[Join Group]({group_link})"
        
        # Sanitize group name for Markdown table (pipes are problematic)
        safe_group_name = group_name.replace("|", "|")

        markdown_lines.append(f"| {logo_md} | {safe_group_name} | {link_md} |")
    
    return "\n".join(markdown_lines)

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Enhanced tool to find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

    # Initialize session state
    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: # Stores links for which validation has been ATTEMPTED
        st.session_state.processed_links_in_session = set()

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox(
            "Choose Input Method:",
            [
                "Search & Scrape from Google (Single Keyword)",
                "Search & Scrape from Google (Bulk via Excel)",
                "Scrape from Specific Webpage URL",
                "Scrape from Entire Website (Limited Crawl)",
                "Enter Links Manually (for Validation)",
                "Upload Link File (TXT/CSV for Validation)"
            ],
            index=0
        )

        # --- Method-specific settings ---
        google_results_to_scrape = 10 # Default
        crawl_depth = 1 # Default
        max_crawl_pages = 10 # Default

        if "Google" in input_method:
            google_results_to_scrape = st.slider(
                "Google search results to process (per keyword):",
                min_value=1, max_value=100, value=10, step=1,
                help="Number of Google search result pages to analyze for WhatsApp links."
            )
        if "Entire Website" in input_method:
            crawl_depth = st.slider(
                "Max Crawl Depth:", min_value=0, max_value=5, value=1,
                help="How many levels deep to crawl from the start URL (0 means only the start URL)."
            )
            max_crawl_pages = st.slider(
                "Max Pages to Crawl:", min_value=1, max_value=50, value=10,
                help="Maximum number of pages to fetch during website crawl."
            )
        
        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_results_sidebar"):
            st.session_state.results = []
            st.session_state.processed_links_in_session = set()
            st.success("All results and processed link cache cleared!")
            # st.rerun() # Use with caution, can lead to loops if not handled well

    # --- Processing Logic ---
    all_scraped_links = set() # Collects raw chat.whatsapp.com links from scraping
    
    st.subheader(f"🚀 Action Zone: {input_method}")

    if input_method == "Search & Scrape from Google (Single Keyword)":
        keyword = st.text_input("Enter Google Search Query:", placeholder="e.g., Technology WhatsApp groups")
        if st.button("🔍 Search, Scrape, and Validate", use_container_width=True):
            if not keyword:
                st.warning("Please enter a search query.")
            else:
                with st.spinner(f"Searching Google for '{keyword}'..."):
                    search_result_urls = perform_google_search(keyword, num_results=google_results_to_scrape)
                if search_result_urls:
                    st.info(f"Found {len(search_result_urls)} webpages. Now scraping for WhatsApp links...")
                    # Scrape links from these URLs
                    session = requests.Session()
                    for url in search_result_urls:
                        links_from_page = scrape_whatsapp_links_from_url(url, session)
                        all_scraped_links.update(links_from_page)
                    session.close()

    elif input_method == "Search & Scrape from Google (Bulk via Excel)":
        excel_file = st.file_uploader("Upload Excel file with keywords (one per row in first column)", type=["xlsx"])
        if excel_file and st.button("📄 Process Excel & Validate", use_container_width=True):
            keywords = load_keywords_from_excel(excel_file)
            if not keywords:
                st.warning("No keywords found in the Excel file or file could not be read.")
            else:
                st.info(f"Found {len(keywords)} keywords. Starting Google searches and scraping...")
                session = requests.Session()
                overall_progress = st.progress(0)
                for i, keyword in enumerate(keywords):
                    st.write(f"Processing keyword: {keyword} ({i+1}/{len(keywords)})")
                    search_result_urls = perform_google_search(keyword, num_results=google_results_to_scrape)
                    if search_result_urls:
                        for url in search_result_urls:
                            links_from_page = scrape_whatsapp_links_from_url(url, session)
                            all_scraped_links.update(links_from_page)
                    overall_progress.progress((i + 1) / len(keywords))
                session.close()
                st.success("Bulk processing complete.")
    
    elif input_method == "Scrape from Specific Webpage URL":
        page_url = st.text_input("Enter Webpage URL to Scrape:", placeholder="https://example.com/some-page")
        if st.button("🔗 Scrape Page & Validate", use_container_width=True):
            if not page_url:
                st.warning("Please enter a webpage URL.")
            elif not (page_url.startswith("http://") or page_url.startswith("https://")):
                 st.warning("Please enter a valid URL starting with http:// or https://")
            else:
                with st.spinner(f"Scraping {page_url}..."):
                    session = requests.Session()
                    links_from_page = scrape_whatsapp_links_from_url(page_url, session)
                    all_scraped_links.update(links_from_page)
                    session.close()

    elif input_method == "Scrape from Entire Website (Limited Crawl)":
        domain_url = st.text_input("Enter Base Domain URL to Crawl:", placeholder="example.com or https://example.com")
        if st.button("🌐 Crawl Website & Validate", use_container_width=True):
            if not domain_url:
                st.warning("Please enter a domain URL.")
            else:
                pages_to_scrape, session = crawl_website(domain_url, max_depth=crawl_depth, max_pages=max_crawl_pages)
                if pages_to_scrape:
                    st.info(f"Now scraping {len(pages_to_scrape)} found pages for WhatsApp links...")
                    crawl_progress = st.progress(0)
                    for i, page_url in enumerate(pages_to_scrape):
                        st.sidebar.text(f"Scraping: {page_url[:70]}...")
                        links_from_page = scrape_whatsapp_links_from_url(page_url, session)
                        all_scraped_links.update(links_from_page)
                        crawl_progress.progress((i + 1) / len(pages_to_scrape))
                    st.sidebar.success("Website scraping complete.")
                else:
                    st.warning("No pages were found or crawled from the provided domain.")
                session.close()


    elif input_method == "Enter Links Manually (for Validation)":
        links_text = st.text_area("Enter WhatsApp Links (one per line):", height=150, placeholder=f"{WHATSAPP_DOMAIN}ABC123XYZ\n{WHATSAPP_DOMAIN}DEF456UVW")
        if st.button("✍️ Validate Manual Links", use_container_width=True):
            raw_links = [line.strip() for line in links_text.split('\n') if line.strip().startswith(WHATSAPP_DOMAIN)]
            if not raw_links:
                st.warning("Please enter at least one valid WhatsApp link.")
            else:
                all_scraped_links.update(raw_links)

    elif input_method == "Upload Link File (TXT/CSV for Validation)":
        uploaded_file = st.file_uploader("Upload TXT or CSV file with WhatsApp links", type=["txt", "csv"])
        if uploaded_file and st.button("📤 Validate File Links", use_container_width=True):
            raw_links = load_links_from_text_file(uploaded_file)
            valid_whatsapp_links = [link for link in raw_links if link.startswith(WHATSAPP_DOMAIN)]
            if not valid_whatsapp_links:
                st.warning("No valid WhatsApp links found in the file.")
            else:
                all_scraped_links.update(valid_whatsapp_links)

    # --- Unified Validation Step for all scraped/inputted links ---
    if all_scraped_links:
        # Filter out links already processed in this session
        links_to_validate_now = list(all_scraped_links - st.session_state.processed_links_in_session)

        if not links_to_validate_now:
            st.info("All found WhatsApp links have already been processed in this session.")
        else:
            st.success(f"Found {len(all_scraped_links)} total WhatsApp links. Validating {len(links_to_validate_now)} new unique links...")
            
            validation_progress = st.progress(0)
            status_text = st.empty()
            new_results_this_run = []

            with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
                for i, future in enumerate(as_completed(future_to_link)):
                    link = future_to_link[future]
                    try:
                        result = future.result()
                        new_results_this_run.append(result)
                        st.session_state.processed_links_in_session.add(link) # Mark as processed
                    except Exception as exc:
                        st.error(f"Error validating link {link}: {exc}")
                        new_results_this_run.append({"Group Name": "Error", "Group Link": link, "Logo URL": "", "Status": f"Validation Error: {exc}"})
                        st.session_state.processed_links_in_session.add(link) # Also mark as processed to avoid retrying error links

                    validation_progress.progress((i + 1) / len(links_to_validate_now))
                    status_text.text(f"Validating: {i + 1}/{len(links_to_validate_now)} links. Last: {link.split('/')[-1]}")
            
            st.session_state.results.extend(new_results_this_run)
            status_text.success(f"Validation complete for {len(links_to_validate_now)} new links!")

    # --- Display Results ---
    if st.session_state.results:
        df_results = pd.DataFrame(st.session_state.results)
        # Deduplicate based on 'Group Link' keeping the first occurrence (in case of re-runs with slight changes)
        df_results.drop_duplicates(subset=['Group Link'], keep='first', inplace=True)
        st.session_state.results = df_results.to_dict('records') # Update session state with deduplicated list

        active_df = df_results[df_results['Status'] == 'Active'].copy() # Use .copy() to avoid SettingWithCopyWarning
        expired_df = df_results[~df_results['Status'].isin(['Active', 'Error', 'Timeout Error', 'Network Error', 'Parsing Error', 'HTTP Error'])] # More robust filtering for expired/invalid
        error_df_count = len(df_results[df_results['Status'].str.contains("Error", case=False, na=False)])


        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Processed", len(df_results))
            st.markdown('</div>', unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Active Links", len(active_df))
            st.markdown('</div>', unsafe_allow_html=True)
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Expired/Invalid", len(expired_df))
            st.markdown('</div>', unsafe_allow_html=True)
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Errors", error_df_count)
            st.markdown('</div>', unsafe_allow_html=True)


        with st.expander("🔎 View, Filter & Download All Results", expanded=False):
            status_options = ["All"] + sorted(df_results['Status'].unique().tolist())
            selected_status = st.multiselect("Filter by Status:", options=status_options, default=["Active"])

            if "All" in selected_status or not selected_status:
                filtered_df_display = df_results
            else:
                filtered_df_display = df_results[df_results['Status'].isin(selected_status)]
            
            st.dataframe(
                filtered_df_display,
                column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Open Link"),
                    "Logo URL": st.column_config.ImageColumn("Logo Preview", width="small") # Use ImageColumn for preview
                },
                height=400,
                use_container_width=True
            )
            csv_all = filtered_df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Filtered Results (CSV)",
                csv_all,
                "filtered_whatsapp_groups.csv",
                "text/csv",
                use_container_width=True,
                key="download_filtered_csv"
            )
        
        st.subheader("📋 Markdown Export (Active Groups)")
        if not active_df.empty:
            markdown_table_data = generate_markdown_output(active_df)
            st.text_area("Copy Markdown Table:", value=markdown_table_data, height=200, key="markdown_output_area")
            
            col_md1, col_md2 = st.columns(2)
            with col_md1:
                # Copy to clipboard button - Streamlit doesn't have a native one.
                # The text_area itself is the easiest way for users to copy.
                # For a true "copy" button, you'd need streamlit-copy-to-clipboard or custom JS.
                # Let's keep it simple with text_area for now.
                st.caption("Select the text above and press Ctrl+C or Cmd+C to copy.")
            with col_md2:
                 st.download_button(
                    label="📥 Download Markdown (.md)",
                    data=markdown_table_data,
                    file_name="active_whatsapp_groups.md",
                    mime="text/markdown",
                    use_container_width=True,
                    key="download_markdown"
                )
            
            with st.expander("📋 Markdown Preview (WordPress-like)", expanded=False):
                 st.markdown(markdown_table_data, unsafe_allow_html=True) # unsafe_allow_html for img tags

        else:
            st.info("No active groups found to generate Markdown output.")

    else:
        st.info("🏁 Start by choosing an input method and providing data to find WhatsApp group links.", icon="ℹ️")

if __name__ == "__main__":
    # Ensure `openpyxl` is available for Excel operations.
    try:
        import openpyxl
    except ImportError:
        st.error("The 'openpyxl' library is required for Excel file processing. Please install it (`pip install openpyxl`).")
        st.stop()
    main()
