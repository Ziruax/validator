import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_library
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io
from fake_useragent import UserAgent

# --- Initialize UserAgent ---
try:
    ua_general = UserAgent()
except Exception as e:
    st.error(f"Could not initialize Fake UserAgent, using a default. Error: {e}")
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
IMAGE_PATTERN_SHARED = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
MAX_VALIDATION_WORKERS = 10

# --- Custom CSS for Streamlit Display (Minimal, since main CSS is in WordPress) ---
st.markdown("""
<style>
/* General Styles for Streamlit */
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
    return parsed_url._replace(query=new_query_string).geturl()

# --- Google Search Function ---
def google_search_user_original(query, top_n=5, pause_duration=5.0):
    """Fetch URLs from Google's top N search results with random User-Agent."""
    try:
        headers = {
            "User-Agent": ua_general.random,
            "Accept-Language": "en-US,en;q=0.9"
        }
        st.sidebar.info(f"Googling '{query}' (top {top_n}, pause: {pause_duration}s)...")
        urls = list(google_search_library(query, num_results=top_n, lang="en", pause=pause_duration, headers=headers))
        if not urls:
            st.warning(f"No search results found for '{query}'. Try refining your search terms.")
        return urls
    except Exception as e:
        st.error(f"Google Search error: {str(e)}")
        return []

def scrape_whatsapp_links_user_original(url):
    """Scrape WhatsApp group links from a webpage."""
    try:
        headers = {
            "User-Agent": ua_general.random
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links.append(a['href'].split('?')[0])
        for text in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text:
                found_links = re.findall(r'https?://chat\.whatsapp\.com/[^\s]+', text)
                for flink in found_links:
                    links.append(flink.split('?')[0])
        return list(set(links))
    except Exception:
        return []

# --- Enhanced Scraping Function ---
def scrape_whatsapp_links_enhanced(url, session):
    links = set()
    try:
        netloc_for_error = urlparse(url).netloc or url[:30]
        response = session.get(url, headers=get_random_headers_for_general_use(), timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0])
        for text_chunk in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text_chunk:
                found_in_chunk = re.findall(r'https?://chat\.whatsapp\.com/[^\s"\'<>()]+', text_chunk)
                for link_url in found_in_chunk: links.add(link_url.split('?')[0])
    except requests.exceptions.Timeout: st.sidebar.warning(f"Timeout (enh) {netloc_for_error}", icon="⏱️")
    except requests.exceptions.RequestException as e: st.sidebar.warning(f"Scrape err (enh) {netloc_for_error}: {type(e).__name__}", icon="⚠️")
    except Exception as e: st.sidebar.warning(f"Parse err (enh) {netloc_for_error}: {type(e).__name__}", icon="💣")
    return list(links)

# --- Validation Function ---
def validate_link(link):
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        response = requests.get(link, headers=get_random_headers_for_general_use(), timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200: result["Status"] = f"HTTP Error {response.status_code}"; return result
        if WHATSAPP_DOMAIN not in response.url: result["Status"] = "Invalid Link (Redirected)"; return result
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')
        result["Group Name"] = unescape(meta_title['content']).strip() or "Unnamed Group" if meta_title and meta_title.get('content') else "Unnamed Group"
        
        img_tags = soup.find_all('img', src=True)
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN_SHARED.match(src):
                result["Logo URL"] = src
                result["Status"] = "Active"
                break
        if result["Status"] != "Active":
            result["Status"] = "Expired"
    except requests.exceptions.RequestException: result["Status"] = "Network Error"
    except Exception: result["Status"] = "Parsing Error"
    return result

def crawl_website(start_url, max_depth=3, max_pages=100):
    if not start_url.startswith(('http://', 'https://')): start_url = 'https://' + start_url
    parsed_start_url = urlparse(start_url)
    base_domain = parsed_start_url.netloc
    urls_to_visit, visited_urls, scraped_content_urls = [(start_url, 0)], set(), set()
    session = requests.Session()
    with st.spinner(f"Crawling {base_domain} (D:{max_depth}, P:{max_pages})..."):
        page_count = 0
        while urls_to_visit and page_count < max_pages:
            current_url, depth = urls_to_visit.pop(0)
            if current_url in visited_urls or depth > max_depth: continue
            visited_urls.add(current_url)
            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}): {current_url[:60]}...")
            try:
                response = session.get(current_url, headers=get_random_headers_for_general_use(), timeout=7)
                response.raise_for_status()
                scraped_content_urls.add(current_url); page_count += 1
                if depth < max_depth:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for link_tag in soup.find_all('a', href=True):
                        abs_url = urljoin(current_url, link_tag['href'])
                        parsed_abs_url = urlparse(abs_url)
                        if parsed_abs_url.scheme in ['http', 'https'] and parsed_abs_url.netloc == base_domain and \
                           parsed_abs_url.path and abs_url not in visited_urls and (abs_url, depth + 1) not in urls_to_visit:
                            urls_to_visit.append((abs_url, depth + 1))
            except requests.exceptions.RequestException as e: st.sidebar.warning(f"Crawl skip: {type(e).__name__}", icon="🕸️")
            except Exception as e: st.sidebar.error(f"Crawl err: {type(e).__name__}", icon="💥")
    st.sidebar.success(f"Crawler found {len(scraped_content_urls)} pages.")
    return list(scraped_content_urls), session

def load_links_from_text_file(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file).iloc[:, 0].tolist()
    else:
        return [line.decode().strip() for line in uploaded_file.readlines()]

def load_keywords_from_excel(uploaded_file):
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        st.error(f"Error reading Excel {uploaded_file.name}: {e}. Ensure 'openpyxl' installed.")
        return []

# --- MODIFIED generate_markdown_output function ---
def generate_markdown_output(active_results_df, negative_keywords=[]):
    if active_results_df.empty:
        return "No active groups found to generate HTML table."

    # These are the labels that will correspond to your table headers
    # and will be used in the data-label attributes.
    # Ensure they match the order and meaning of your <th> elements.
    header_labels = {
        "Logo": "Logo",
        "Group Name": "Group Name",
        "Group Link": "Group Link"
    }

    markdown_lines = [
        '<table class="whatsapp-groups-table" aria-label="List of Active WhatsApp Groups">',
        '<caption>Filtered Active WhatsApp Groups</caption>',
        # Ensure your <th> text matches the keys/values in header_labels if you want them to be identical
        '<thead><tr><th scope="col">Logo</th><th scope="col">Group Name</th><th scope="col">Group Link</th></tr></thead>',
        '<tbody>'
    ]

    filtered_rows = 0
    for _, row in active_results_df.iterrows():
        group_name = row.get("Group Name", "N/A")
        if any(keyword.strip().lower() in group_name.lower() for keyword in negative_keywords if keyword.strip()):
            continue

        logo_url = row.get("Logo URL", "")
        group_link = row.get("Group Link", "")

        if logo_url:
            # Consider adding error handling for append_query_param if logo_url might be invalid
            try:
                resized_logo_url_server = append_query_param(logo_url, 'w', '96') # Appends w=96 for image width
                logo_md = f'<img src="{resized_logo_url_server}" alt="{group_name} Group Logo" class="group-logo-img" loading="lazy" onerror="this.src=\'https://placehold.co/96x96/cccccc/FFFFFF?text=Error\'; this.alt=\'Fallback Logo\';">'
            except Exception: # Catch potential errors if logo_url is malformed for append_query_param
                 logo_md = f'<img src="https://placehold.co/96x96/cccccc/FFFFFF?text=Logo" alt="{group_name} Group Logo (Error)" class="group-logo-img" loading="lazy">'
        else:
            # Provide a placeholder if no logo URL is present
            logo_md = f'<img src="https://placehold.co/96x96/eeeeee/999999?text=No+Logo" alt="{group_name} No Logo" class="group-logo-img" loading="lazy">'


        link_md = f'<a href="{group_link}" class="join-button" target="_blank" rel="noopener noreferrer">Join Group</a>'
        
        # Sanitize group name for HTML display
        safe_group_name = unescape(group_name) # Use html.unescape

        # *** MODIFICATION HERE: Add data-label attributes ***
        markdown_lines.append(
            f'<tr>'
            f'<td data-label="{header_labels["Logo"]}" class="group-logo-cell">{logo_md}</td>'
            f'<td data-label="{header_labels["Group Name"]}" class="group-name-cell">{safe_group_name}</td>'
            f'<td data-label="{header_labels["Group Link"]}" class="join-button-cell">{link_md}</td>'
            f'</tr>'
        )
        filtered_rows += 1

    markdown_lines.append('</tbody></table>')

    if filtered_rows == 0:
        return "No groups match the filter criteria."
    return "\n".join(markdown_lines)

# --- Main Application Logic ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Enhanced tool to find, scrape, and validate WhatsApp group links.</p>', unsafe_allow_html=True)

    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()

    with st.sidebar:
        st.header("⚙️ Input & Settings")
        input_method = st.selectbox("Choose Input Method:", [
            "Search and Scrape from Google",
            "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL",
            "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)",
            "Upload Link File (TXT/CSV for Validation)"
        ], key="input_method_main_select")

        google_results_slider_top_n = 5
        google_search_pause = 5.0

        if input_method in ["Search and Scrape from Google", "Search & Scrape from Google (Bulk via Excel)"]:
            google_results_slider_top_n = st.slider(
                "Google Results to Scrape (per keyword)",
                min_value=1, max_value=20, value=5, key="google_top_n_slider"
            )
            google_search_pause = st.slider(
                "Google Search Pause (seconds):", min_value=1.0, max_value=10.0, value=5.0, step=0.5,
                help="Pause between Google search API calls to avoid rate-limiting.", key="google_pause_slider"
            )
        
        crawl_depth_val, max_crawl_pages_val = 2, 50
        if input_method == "Scrape from Entire Website (Extensive Crawl)":
            st.warning("⚠️ Extensive website crawling can be very slow. Use with caution.", icon="🚨")
            crawl_depth_val = st.slider("Max Crawl Depth:", min_value=0, max_value=10, value=2, key="crawl_depth_slider")
            max_crawl_pages_val = st.slider("Max Pages to Crawl:", min_value=1, max_value=1000, value=50, key="crawl_pages_slider")
        
        if st.button("🗑️ Clear All Results & Reset Filters", use_container_width=True, key="clear_all_button"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.success("All results and cache cleared!")

    all_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")
    
    general_purpose_session = requests.Session()
    try:
        if input_method == "Search and Scrape from Google":
            keyword_gs = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="gs_keyword_input")
            if st.button("Search, Scrape, and Validate", use_container_width=True, key="gs_button"):
                if not keyword_gs: st.warning("Please enter a search query.")
                else:
                    with st.spinner("Searching Google..."):
                        search_page_urls = google_search_user_original(keyword_gs, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                    if search_page_urls:
                        st.success(f"Found {len(search_page_urls)} webpages. Scraping WhatsApp links...")
                        prog_bar_gs = st.progress(0)
                        for i, page_url in enumerate(search_page_urls):
                            links_from_page = scrape_whatsapp_links_user_original(page_url)
                            all_scraped_links.update(links_from_page)
                            prog_bar_gs.progress((i+1)/len(search_page_urls))
                        st.success("Google page scraping complete.")
        
        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file_bulk = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"], key="gs_bulk_excel_upload")
            if excel_file_bulk and st.button("Process Excel & Scrape from Google", use_container_width=True, key="gs_bulk_button"):
                keywords_bulk = load_keywords_from_excel(excel_file_bulk)
                if not keywords_bulk: st.warning("No keywords in Excel.")
                else:
                    st.info(f"{len(keywords_bulk)} keywords. Starting Google searches & scraping...")
                    prog_bulk, stat_txt_bulk = st.progress(0), st.empty()
                    for i, kw_bulk in enumerate(keywords_bulk):
                        stat_txt_bulk.write(f"Keyword: **{kw_bulk}** ({i+1}/{len(keywords_bulk)})")
                        search_page_urls_bulk = google_search_user_original(kw_bulk, top_n=google_results_slider_top_n, pause_duration=google_search_pause)
                        if search_page_urls_bulk:
                            for page_url_bulk in search_page_urls_bulk:
                                links_from_page_bulk = scrape_whatsapp_links_user_original(page_url_bulk)
                                all_scraped_links.update(links_from_page_bulk)
                        prog_bulk.progress((i + 1) / len(keywords_bulk))
                    stat_txt_bulk.success("Bulk Google processing complete.")

        elif input_method == "Scrape from Specific Webpage URL":
            page_url_specific = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page", key="specific_url_input")
            if st.button("Scrape Page (Enhanced Method) & Validate", use_container_width=True, key="specific_url_button"):
                if not page_url_specific or not (page_url_specific.startswith("http://") or page_url_specific.startswith("https://")):
                    st.warning("Please enter a valid URL.")
                else:
                    with st.spinner(f"Scraping {page_url_specific}..."):
                        links_from_page_spec = scrape_whatsapp_links_enhanced(page_url_specific, general_purpose_session)
                        all_scraped_links.update(links_from_page_spec)
                    st.success(f"Scraping of {page_url_specific} complete.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url_crawl = st.text_input("Enter Base Domain URL:", placeholder="example.com", key="crawl_domain_input")
            if st.button("Crawl & Scrape (Enhanced Method)", use_container_width=True, key="crawl_button"):
                if not domain_url_crawl: st.warning("Please enter a domain URL.")
                else:
                    pages_to_scrape_crawl, crawl_session_obj = crawl_website(domain_url_crawl, max_depth=crawl_depth_val, max_pages=max_crawl_pages_val)
                    try:
                        if pages_to_scrape_crawl:
                            st.info(f"Crawled. Now scraping {len(pages_to_scrape_crawl)} pages...")
                            prog_crawl, stat_txt_crawl = st.progress(0), st.empty()
                            for i, p_url_crawl in enumerate(pages_to_scrape_crawl):
                                stat_txt_crawl.text(f"Scraping: {p_url_crawl[:60]}... ({i+1}/{len(pages_to_scrape_crawl)})")
                                links_from_page_crawl = scrape_whatsapp_links_enhanced(p_url_crawl, crawl_session_obj)
                                all_scraped_links.update(links_from_page_crawl)
                                prog_crawl.progress((i + 1) / len(pages_to_scrape_crawl))
                            stat_txt_crawl.success("Website scraping complete.")
                        else: st.warning("No pages found/scraped from domain.")
                    finally:
                        if 'crawl_session_obj' in locals() and crawl_session_obj: crawl_session_obj.close()
        
        elif input_method == "Enter Links Manually (for Validation)":
            links_text_manual = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123", key="manual_links_text_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button"):
                links_manual = [line.strip() for line in links_text_manual.split('\n') if line.strip()]
                if not links_manual: st.warning("Please enter at least one link.")
                else: all_scraped_links.update([l for l in links_manual if l.startswith(WHATSAPP_DOMAIN)])

        elif input_method == "Upload Link File (TXT/CSV for Validation)":
            uploaded_file_val = st.file_uploader("Upload TXT or CSV", type=["txt", "csv"], key="upload_file_links")
            if uploaded_file_val and st.button("Validate File Links", use_container_width=True, key="upload_validate_button"):
                links_from_file = load_links_from_text_file(uploaded_file_val)
                if not links_from_file: st.warning("No links found in the uploaded file.")
                else: all_scraped_links.update([l for l in links_from_file if l.startswith(WHATSAPP_DOMAIN)])
    finally:
        if 'general_purpose_session' in locals() and general_purpose_session: general_purpose_session.close()

    # --- Unified Validation Step ---
    if all_scraped_links:
        links_to_validate_now = list(all_scraped_links - st.session_state.processed_links_in_session)
        if not links_to_validate_now:
            st.info("No new WhatsApp links found or all previously found links processed.")
        else:
            st.success(f"Found {len(all_scraped_links)} total unique links. Validating {len(links_to_validate_now)} new links...")
            prog_val, stat_val = st.progress(0), st.empty()
            new_results_validation = []
            with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
                for i, future in enumerate(as_completed(future_to_link)):
                    link_validated, result_validated = future_to_link[future], future.result()
                    new_results_validation.append(result_validated)
                    st.session_state.processed_links_in_session.add(link_validated)
                    prog_val.progress((i + 1) / len(links_to_validate_now))
                    stat_val.text(f"Validated {i + 1}/{len(links_to_validate_now)} links")
            st.session_state.results.extend(new_results_validation)
            stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")

    # --- Display Results ---
    if 'results' in st.session_state and st.session_state.results:
        df_results_display = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = df_results_display.to_dict('records')
        
        active_df_display = df_results_display[df_results_display['Status'] == 'Active'].copy()
        expired_df_display = df_results_display[df_results_display['Status'] == 'Expired']
        
        st.subheader("📊 Results Summary")
        col1_disp, col2_disp, col3_disp = st.columns(3)
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
            st.metric("Expired Links", len(expired_df_display))
            st.markdown('</div>', unsafe_allow_html=True)

        with st.expander("🔎 View and Filter Results", expanded=True):
            status_filter_options = df_results_display['Status'].unique()
            status_filter_val = st.multiselect("Filter by Status", options=status_filter_options, default=["Active"] if "Active" in status_filter_options else list(status_filter_options[:1]))
            
            filtered_df_for_display = df_results_display[df_results_display['Status'].isin(status_filter_val)] if status_filter_val else df_results_display
            
            st.dataframe(
                filtered_df_for_display,
                column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Group"),
                    "Logo URL": st.column_config.TextColumn("Logo URL")
                },
                height=400,
                use_container_width=True
            )
        
        st.subheader("📋 HTML Table Export (Active Groups)")
        if not active_df_display.empty:
            negative_keywords_input = st.text_input(
                "Enter negative keywords (comma-separated):",
                "",
                help="e.g., Hub, Flix to exclude groups containing these words"
            )
            negative_keywords = [kw.strip() for kw in negative_keywords_input.split(",") if kw.strip()]
            md_data_export = generate_markdown_output(active_df_display, negative_keywords)
            st.markdown(f"Showing {len(active_df_display) - sum(1 for _, row in active_df_display.iterrows() if any(kw.lower() in row.get('Group Name', '').lower() for kw in negative_keywords))} out of {len(active_df_display)} matching active groups.")
            with st.expander("Copy or Download HTML Table", expanded=True):
                st.text_area("Copy Raw HTML Code (above table):", value=md_data_export, height=250, key="md_export_area")
                st.download_button(
                    "📥 Download HTML (.html)",
                    md_data_export,
                    "active_groups.html",
                    "text/html",
                    use_container_width=True,
                    key="md_export_download"
                )
            with st.expander("📋 HTML Table Preview", expanded=False):
                st.markdown(md_data_export, unsafe_allow_html=True)
        else:
            st.info("No active groups found to generate HTML table.")
        
        col_dl1_orig, col_dl2_orig = st.columns(2)
        with col_dl1_orig:
            csv_active_orig = active_df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Active Groups (CSV)",
                csv_active_orig,
                "active_groups.csv",
                "text/csv",
                use_container_width=True,
                key="dl_active_csv_orig"
            )
        with col_dl2_orig:
            csv_all_orig = df_results_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download All Results (CSV)",
                csv_all_orig,
                "all_groups.csv",
                "text/csv",
                use_container_width=True,
                key="dl_all_csv_orig"
            )

    else:
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")

if __name__ == "__main__":
    try: import openpyxl
    except ImportError: st.error("Library 'openpyxl' for Excel is missing. Please install: `pip install openpyxl`"); st.stop()
    try: from fake_useragent import UserAgent; UserAgent()
    except ImportError: st.warning("Library 'fake-useragent' is missing. General scraping might be less effective. Install: `pip install fake-useragent`", icon="⚠️")
    except Exception: st.warning("Fake-useragent initialized with issues. General scraping might use a default User-Agent.", icon="⚠️")
    
    main()
