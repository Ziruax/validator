import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time # Keep for potential future use, though googlesearch handles its own pausing
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_lib # Renamed to avoid conflict
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io

# --- Streamlit Configuration & Constants ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN = re.compile(r'https://pps\.whatsapp\.net/[^"\s]+')
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}
MAX_VALIDATION_WORKERS = 10

# --- Custom CSS ---
st.markdown("""
<style>
/* ... (your existing CSS) ... */
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
.stProgress .st-bo {
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
    if not url:
        return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return parsed_url._replace(query=new_query_string).geturl()

def validate_link(link):
    result = {
        "Group Name": "Unknown",
        "Group Link": link,
        "Logo URL": "",
        "Status": "Error"
    }
    try:
        response = requests.get(link, headers=HEADERS, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid Link (Redirected)"
            return result
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = unescape(meta_title['content']).strip()
            result["Group Name"] = group_name or "Unnamed Group"
        else:
            title_tag = soup.find('h3')
            if title_tag:
                 result["Group Name"] = unescape(title_tag.get_text(strip=True)) or "Unnamed Group"
            else:
                result["Group Name"] = "Unnamed Group"
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            img_src = unescape(meta_image['content'])
            if IMAGE_PATTERN.match(img_src):
                 result["Logo URL"] = img_src
        if not result["Logo URL"]:
            img_tags = soup.find_all('img', src=True)
            for img in img_tags:
                src = unescape(img['src'])
                if IMAGE_PATTERN.match(src):
                    result["Logo URL"] = src
                    break
        join_button_texts = ["Join Chat", "Join Group", "View Group"]
        page_text_lower = soup.get_text().lower()
        if any(btn_text.lower() in page_text_lower for btn_text in join_button_texts) or \
           "you can join this group" in page_text_lower or \
           result["Group Name"] not in ["Unknown", "Unnamed Group"]:
            result["Status"] = "Active"
        elif "link is invalid" in page_text_lower or "link has been revoked" in page_text_lower or "link expired" in page_text_lower:
            result["Status"] = "Expired/Invalid"
        else:
            if result["Logo URL"] or result["Group Name"] not in ["Unknown", "Unnamed Group"]:
                 result["Status"] = "Active"
            else:
                 result["Status"] = "Expired/Invalid"
    except requests.exceptions.Timeout:
        result["Status"] = "Timeout Error"
    except requests.exceptions.RequestException:
        result["Status"] = f"Network Error"
    except Exception:
        result["Status"] = f"Parsing Error"
    return result

def scrape_whatsapp_links_from_url(url, session):
    links = set()
    try:
        response = session.get(url, headers=HEADERS, timeout=10)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith(WHATSAPP_DOMAIN):
                clean_link = href.split('?')[0]
                links.add(clean_link)
        text_content = soup.get_text()
        found_in_text = re.findall(r'https?://chat\.whatsapp\.com/([a-zA-Z0-9_-]+)', text_content)
        for code in found_in_text:
            links.add(f"{WHATSAPP_DOMAIN}{code}")
    except requests.exceptions.RequestException as e:
        st.sidebar.warning(f"Scrape fail {url_parse.netloc}: {type(e).__name__}", icon="⚠️")
    except Exception as e:
        st.sidebar.warning(f"Parse fail {url_parse.netloc}: {type(e).__name__}", icon="💣")
    return list(links)

def perform_google_search(query, num_results_to_fetch, pause_duration): # Renamed parameters for clarity
    """Fetch URLs from Google's search results with configurable pause."""
    try:
        # num_results parameter of google_search_lib is equivalent to 'stop' in some contexts,
        # it's the total number of results to retrieve.
        # 'pause' is the delay between HTTP requests made by the library.
        st.sidebar.info(f"Googling '{query}' (want {num_results_to_fetch} results, pause: {pause_duration}s)...")
        urls = list(google_search_lib(query, num=num_results_to_fetch, stop=num_results_to_fetch, lang="en", pause=pause_duration))
        
        if not urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms or check Google's website directly.")
        return urls
    except Exception as e:
        error_str = str(e).lower()
        error_message_base = f"🚫 Google Search Error for '{query}'"
        if "http error 429" in error_str or "too many requests" in error_str:
            st.error(f"{error_message_base}: Likely rate-limited by Google (Too Many Requests). "
                     f"Try increasing the 'Google Search Pause' in settings, reducing 'Google search results to process', "
                     f"or waiting a while before trying again.")
        elif "http error 503" in error_str or "service unavailable" in error_str:
            st.error(f"{error_message_base}: Google service temporarily unavailable. Please try again later.")
        else:
            st.error(f"{error_message_base}: {e}. "
                     f"This could be due to network issues, changes in Google's search page structure, or aggressive blocking. "
                     f"Consider increasing the 'Google Search Pause' in settings or reducing the number of results.")
        return []


def crawl_website(start_url, max_depth=2, max_pages=20):
    if not start_url.startswith(('http://', 'https://')):
        start_url = 'http://' + start_url
    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    urls_to_visit = [(start_url, 0)]
    visited_urls = set()
    scraped_content_urls = set()
    session = requests.Session()
    with st.spinner(f"Crawling {base_domain}..."):
        while urls_to_visit and len(scraped_content_urls) < max_pages:
            current_url, depth = urls_to_visit.pop(0)
            if current_url in visited_urls or depth > max_depth:
                continue
            visited_urls.add(current_url)
            st.sidebar.text(f"Crawl (D:{depth}): {current_url[:70]}...")
            try:
                response = session.get(current_url, headers=HEADERS, timeout=7)
                response.encoding = response.apparent_encoding
                scraped_content_urls.add(current_url)
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        href = link_tag['href']
                        abs_url = urljoin(current_url, href)
                        parsed_abs_url = urlparse(abs_url)
                        if parsed_abs_url.scheme in ['http', 'https'] and parsed_abs_url.netloc == base_domain:
                            if abs_url not in visited_urls and (abs_url, depth + 1) not in urls_to_visit:
                                urls_to_visit.append((abs_url, depth + 1))
            except requests.exceptions.RequestException as e:
                st.sidebar.warning(f"Crawl skip {current_url[:30]}: {type(e).__name__}", icon="🕸️")
            except Exception as e:
                st.sidebar.error(f"Crawl err: {type(e).__name__}", icon="💥")
    st.sidebar.success(f"Crawler found {len(scraped_content_urls)} pages.")
    return list(scraped_content_urls), session

def load_links_from_text_file(uploaded_file):
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
            if df.empty: return []
            for col in df.columns:
                if df[col].astype(str).str.contains(WHATSAPP_DOMAIN, case=False, na=False).any():
                    return df[col].dropna().astype(str).tolist()
            return df.iloc[:, 0].dropna().astype(str).tolist()
        else:
            return [line.decode().strip() for line in uploaded_file.readlines() if line.strip()]
    except Exception as e:
        st.error(f"Error reading file {uploaded_file.name}: {e}")
        return []

def load_keywords_from_excel(uploaded_file):
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty:
            st.warning("Excel file is empty.")
            return []
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        st.error(f"Error reading Excel {uploaded_file.name}: {e}. Ensure 'openpyxl' installed.")
        return []

def generate_markdown_output(active_results_df):
    if active_results_df.empty:
        return "No active groups found to generate Markdown."
    markdown_lines = ["| Group Logo | Group Name | Group Link |", "|---|---|---|"]
    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", "N/A")
        group_link = row.get("Group Link", "")
        logo_md = f"![Logo]({append_query_param(logo_url, 'w', '200')})" if logo_url else " "
        link_md = f"[Join Group]({group_link})"
        safe_group_name = group_name.replace("|", "|")
        markdown_lines.append(f"| {logo_md} | {safe_group_name} | {link_md} |")
    return "\n".join(markdown_lines)

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Enhanced tool to find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state:
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

        google_results_to_scrape = 10
        google_search_pause_seconds = 2.0 # Default pause
        crawl_depth = 1
        max_crawl_pages = 10

        if "Google" in input_method:
            google_results_to_scrape = st.slider(
                "Google search results to process (per keyword):",
                min_value=1, max_value=100, value=10, step=1,
                help="Total number of Google search result URLs to fetch per keyword."
            )
            google_search_pause_seconds = st.slider(
                "Google Search Pause (seconds):",
                min_value=1.0, max_value=10.0, value=2.0, step=0.5,
                help="Seconds to pause between Google search requests to avoid rate-limiting. Increase if you see search errors."
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

    all_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")

    if input_method == "Search & Scrape from Google (Single Keyword)":
        keyword = st.text_input("Enter Google Search Query:", placeholder="e.g., Technology WhatsApp groups")
        if st.button("🔍 Search, Scrape, and Validate", use_container_width=True):
            if not keyword: st.warning("Please enter a search query.")
            else:
                with st.spinner(f"Searching Google for '{keyword}'..."):
                    search_result_urls = perform_google_search(
                        keyword, 
                        num_results_to_fetch=google_results_to_scrape, 
                        pause_duration=google_search_pause_seconds
                    )
                if search_result_urls:
                    st.info(f"Found {len(search_result_urls)} webpages. Scraping for WhatsApp links...")
                    session = requests.Session()
                    for url in search_result_urls:
                        links_from_page = scrape_whatsapp_links_from_url(url, session)
                        all_scraped_links.update(links_from_page)
                    session.close()

    elif input_method == "Search & Scrape from Google (Bulk via Excel)":
        excel_file = st.file_uploader("Upload Excel file with keywords (one per row in first column)", type=["xlsx"])
        if excel_file and st.button("📄 Process Excel & Validate", use_container_width=True):
            keywords = load_keywords_from_excel(excel_file)
            if not keywords: st.warning("No keywords found in Excel or file error.")
            else:
                st.info(f"Found {len(keywords)} keywords. Starting Google searches & scraping...")
                session = requests.Session()
                overall_progress = st.progress(0)
                for i, keyword in enumerate(keywords):
                    st.write(f"Processing keyword: {keyword} ({i+1}/{len(keywords)})")
                    search_result_urls = perform_google_search(
                        keyword, 
                        num_results_to_fetch=google_results_to_scrape, 
                        pause_duration=google_search_pause_seconds
                    )
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
            if not page_url: st.warning("Please enter a webpage URL.")
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
            if not domain_url: st.warning("Please enter a domain URL.")
            else:
                pages_to_scrape, session = crawl_website(domain_url, max_depth=crawl_depth, max_pages=max_crawl_pages)
                if pages_to_scrape:
                    st.info(f"Scraping {len(pages_to_scrape)} found pages for WhatsApp links...")
                    crawl_progress = st.progress(0)
                    for i, page_url in enumerate(pages_to_scrape):
                        st.sidebar.text(f"Scraping: {page_url[:70]}...")
                        links_from_page = scrape_whatsapp_links_from_url(page_url, session)
                        all_scraped_links.update(links_from_page)
                        crawl_progress.progress((i + 1) / len(pages_to_scrape))
                    st.sidebar.success("Website scraping complete.")
                else: st.warning("No pages were found or crawled.")
                session.close()

    elif input_method == "Enter Links Manually (for Validation)":
        links_text = st.text_area("Enter WhatsApp Links (one per line):", height=150, placeholder=f"{WHATSAPP_DOMAIN}ABC123XYZ\n{WHATSAPP_DOMAIN}DEF456UVW")
        if st.button("✍️ Validate Manual Links", use_container_width=True):
            raw_links = [line.strip() for line in links_text.split('\n') if line.strip().startswith(WHATSAPP_DOMAIN)]
            if not raw_links: st.warning("Please enter at least one valid WhatsApp link.")
            else: all_scraped_links.update(raw_links)

    elif input_method == "Upload Link File (TXT/CSV for Validation)":
        uploaded_file = st.file_uploader("Upload TXT or CSV file with WhatsApp links", type=["txt", "csv"])
        if uploaded_file and st.button("📤 Validate File Links", use_container_width=True):
            raw_links = load_links_from_text_file(uploaded_file)
            valid_whatsapp_links = [link for link in raw_links if link.startswith(WHATSAPP_DOMAIN)]
            if not valid_whatsapp_links: st.warning("No valid WhatsApp links found in the file.")
            else: all_scraped_links.update(valid_whatsapp_links)

    # --- Unified Validation Step ---
    if all_scraped_links:
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
                    except Exception as exc:
                        new_results_this_run.append({"Group Name": "Error", "Group Link": link, "Logo URL": "", "Status": f"Validation Error: {exc}"})
                    st.session_state.processed_links_in_session.add(link)
                    validation_progress.progress((i + 1) / len(links_to_validate_now))
                    status_text.text(f"Validating: {i + 1}/{len(links_to_validate_now)} links. Last: {link.split('/')[-1]}")
            st.session_state.results.extend(new_results_this_run)
            status_text.success(f"Validation complete for {len(links_to_validate_now)} new links!")

    # --- Display Results ---
    if st.session_state.results:
        df_results = pd.DataFrame(st.session_state.results)
        df_results.drop_duplicates(subset=['Group Link'], keep='first', inplace=True)
        st.session_state.results = df_results.to_dict('records')
        active_df = df_results[df_results['Status'] == 'Active'].copy()
        expired_df_count = len(df_results[~df_results['Status'].isin(['Active', 'Error', 'Timeout Error', 'Network Error', 'Parsing Error']) & ~df_results['Status'].str.startswith('HTTP Error', na=False)])
        error_df_count = len(df_results[df_results['Status'].str.contains("Error", case=False, na=False) | df_results['Status'].str.startswith('HTTP Error', na=False)])
        
        st.subheader("📊 Results Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total Processed", len(df_results), help="Total unique WhatsApp links processed.")
        with col2: st.metric("Active Links", len(active_df), help="Links confirmed as active.")
        with col3: st.metric("Expired/Invalid", expired_df_count, help="Links confirmed as expired or invalid (not including network/parsing errors).")
        with col4: st.metric("Errors", error_df_count, help="Links that resulted in network, parsing, or other validation errors.")

        with st.expander("🔎 View, Filter & Download All Results", expanded=False):
            status_options = ["All"] + sorted(df_results['Status'].unique().tolist())
            selected_status = st.multiselect("Filter by Status:", options=status_options, default=["Active"])
            filtered_df_display = df_results[df_results['Status'].isin(selected_status)] if selected_status and "All" not in selected_status else df_results
            st.dataframe(filtered_df_display, column_config={"Group Link": st.column_config.LinkColumn("Invite Link", display_text="Open Link"), "Logo URL": st.column_config.ImageColumn("Logo Preview", width="small")}, height=400, use_container_width=True)
            csv_all = filtered_df_display.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Filtered Results (CSV)", csv_all, "filtered_whatsapp_groups.csv", "text/csv", use_container_width=True, key="download_filtered_csv")
        
        st.subheader("📋 Markdown Export (Active Groups)")
        if not active_df.empty:
            markdown_table_data = generate_markdown_output(active_df)
            st.text_area("Copy Markdown Table:", value=markdown_table_data, height=200, key="markdown_output_area")
            col_md1, col_md2 = st.columns(2)
            with col_md1: st.caption("Select text & Ctrl+C/Cmd+C to copy.")
            with col_md2: st.download_button("📥 Download Markdown (.md)", markdown_table_data, "active_whatsapp_groups.md", "text/markdown", use_container_width=True, key="download_markdown")
            with st.expander("📋 Markdown Preview (WordPress-like)", expanded=False):
                 st.markdown(markdown_table_data, unsafe_allow_html=True)
        else: st.info("No active groups found for Markdown output.")
    else: st.info("🏁 Start by choosing an input method and providing data.", icon="ℹ️")

if __name__ == "__main__":
    try: import openpyxl
    except ImportError:
        st.error("The 'openpyxl' library is required for Excel. Install with `pip install openpyxl`.")
        st.stop()
    main()
