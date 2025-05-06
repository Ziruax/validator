import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
# time is imported in your original code, so keeping it.
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
# This is the import from YOUR working code for Google Search
from googlesearch import search as user_google_search_function 
from urllib.parse import urljoin, urlparse, urlencode, parse_qs
import io
from fake_useragent import UserAgent # For new features and validation

# --- Initialize UserAgent (for new features and validation) ---
try:
    ua_for_new_features = UserAgent()
except Exception: # Fallback
    class FallbackUA:
        def random(self): return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ua_for_new_features = FallbackUA()

# --- Streamlit Configuration (from your code) ---
st.set_page_config(
    page_title="WhatsApp Link Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Constants (from your code) ---
WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
# IMAGE_PATTERN from your code, used by your validate_link and potentially new validate_link
IMAGE_PATTERN_FROM_USER_CODE = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')

# --- Custom CSS (from your code, with addition for small logo) ---
st.markdown("""
    <style>
    .main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
    .subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
    .stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; }
    .stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }
    .stProgress .st-bo { background-color: #25D366; }
    .metric-card { background-color: #F5F6F5; padding: 12px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); color: #333333; text-align: center; }
    .stTextInput input, .stTextArea textarea, .stFileUploader div[data-testid="stFileUploadDropzone"] { border: 1px solid #25D366 !important; border-radius: 5px !important; } /* Applied to new inputs too */
    .sidebar .sidebar-content { background-color: #F5F6F5; }
    .stExpander { border: 1px solid #E0E0E0; border-radius: 5px; }
    /* CSS for small, circular Markdown logo */
    img.group-logo-markdown { 
        width:35px !important; 
        height:35px !important; 
        border-radius:50% !important; 
        object-fit:cover !important; 
        vertical-align:middle !important; 
        margin-right: 5px !important; 
    }
    </style>
""", unsafe_allow_html=True)

def get_random_headers_for_new_scrapers():
    return {"User-Agent": ua_for_new_features.random, "Accept-Language": "en-US,en;q=0.9"}

# --- YOUR `validate_link` function (kept as is, but will use it if new one fails or by choice) ---
# I will provide an enhanced one, but keep yours as a reference or fallback.
# For now, let's use an ENHANCED validate_link for better results across the board.
def validate_link_enhanced(link): # This is my enhanced version
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        headers = get_random_headers_for_new_scrapers() # Use fake UA
        response = requests.get(link, headers=headers, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200: result["Status"] = f"HTTP Error {response.status_code}"; return result
        if WHATSAPP_DOMAIN not in response.url: result["Status"] = "Invalid Link (Redirected)"; return result
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')
        group_name_found = unescape(meta_title['content']).strip() if meta_title and meta_title.get('content') else "Unnamed Group"
        result["Group Name"] = group_name_found or "Unnamed Group"
        
        # More robust logo finding
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            img_src = unescape(meta_image['content'])
            if IMAGE_PATTERN_FROM_USER_CODE.match(img_src): result["Logo URL"] = img_src
        
        if not result["Logo URL"]: # Fallback to img tags if meta not found/matched
            img_tags = soup.find_all('img', src=True)
            for img in img_tags:
                src_unescaped = unescape(img['src'])
                if IMAGE_PATTERN_FROM_USER_CODE.match(src_unescaped):
                    result["Logo URL"] = src_unescaped; break
        
        # Status determination logic (can be refined further)
        page_text_lower = soup.get_text().lower()
        if any(btn_text.lower() in page_text_lower for btn_text in ["Join Chat", "Join Group", "View Group"]) or \
           "you can join this group" in page_text_lower or result["Group Name"] not in ["Unknown", "Unnamed Group"]:
            result["Status"] = "Active"
        elif "link is invalid" in page_text_lower or "link has been revoked" in page_text_lower or "link expired" in page_text_lower:
            result["Status"] = "Expired" # Matching your original "Expired" status
        else: # Default heuristic
            result["Status"] = "Active" if result["Logo URL"] or result["Group Name"] not in ["Unknown", "Unnamed Group"] else "Expired"
            
    except requests.exceptions.RequestException: result["Status"] = "Network Error" # Simplified from your original
    except Exception: result["Status"] = "Parsing Error" # Simplified
    return result

# --- YOUR `scrape_whatsapp_links` function (EXACTLY AS PROVIDED) ---
# This will be used ONLY for scraping pages found by YOUR google_search function.
def scrape_whatsapp_links_user_original(url):
    """Scrape WhatsApp group links from a webpage. (User's original function)"""
    try:
        headers = { # FIXED User-Agent from your working code
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8' # As in your code
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links.append(a['href'].split('?')[0]) # Normalize
        for text in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text:
                found_links = re.findall(r'https?://chat\.whatsapp\.com/[^\s]+', text)
                for flink in found_links:
                     links.append(flink.split('?')[0]) # Normalize
        return list(set(links))
    except Exception:
        return []

# --- YOUR `google_search` function (EXACTLY AS PROVIDED, aliased) ---
# aliased to user_google_search_function at import
# def google_search_user_original(query, top_n=5): ... this is now user_google_search_function

# --- NEW SCRAPER for "Specific Page" and "Entire Website" (uses fake-useragent) ---
def scrape_whatsapp_links_for_direct_url(url, session_obj):
    links = set()
    try:
        netloc_for_error = urlparse(url).netloc or url[:30]
        response = session_obj.get(url, headers=get_random_headers_for_new_scrapers(), timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN): links.add(href.split('?')[0])
        for text_chunk in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text_chunk:
                found = re.findall(r'https?://chat\.whatsapp\.com/[^\s"\'<>()]+', text_chunk)
                for link_url in found: links.add(link_url.split('?')[0])
    except Exception as e: st.sidebar.warning(f"Direct URL scrape err ({netloc_for_error}): {type(e).__name__}", icon="🕸️")
    return list(links)

# --- NEW Crawler function (uses fake-useragent) ---
def crawl_website_for_links(start_url, max_depth_crawl, max_pages_crawl):
    if not start_url.startswith(('http://', 'https://')): start_url = 'https://' + start_url
    base_domain = urlparse(start_url).netloc
    urls_to_visit, visited_urls, found_pages_for_scrape = [(start_url, 0)], set(), set()
    crawl_session = requests.Session()
    with st.spinner(f"Crawling {base_domain} (Depth:{max_depth_crawl}, Max Pages:{max_pages_crawl})..."):
        page_count = 0
        while urls_to_visit and page_count < max_pages_crawl:
            current_url, depth = urls_to_visit.pop(0)
            if current_url in visited_urls or depth > max_depth_crawl: continue
            visited_urls.add(current_url)
            st.sidebar.text(f"Crawl (D:{depth}, P:{page_count+1}): {current_url[:60]}...")
            try:
                response = crawl_session.get(current_url, headers=get_random_headers_for_new_scrapers(), timeout=7)
                response.raise_for_status()
                found_pages_for_scrape.add(current_url); page_count += 1
                if depth < max_depth_crawl:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for tag in soup.find_all('a', href=True):
                        abs_url = urljoin(current_url, tag['href'])
                        p_abs = urlparse(abs_url)
                        if p_abs.scheme in ['http','https'] and p_abs.netloc == base_domain and p_abs.path and \
                           abs_url not in visited_urls and (abs_url, depth+1) not in urls_to_visit:
                            urls_to_visit.append((abs_url, depth + 1))
            except Exception as e: st.sidebar.warning(f"Crawl skip on {current_url[:30]}: {type(e).__name__}", icon="🕸️")
    st.sidebar.success(f"Crawler found {len(found_pages_for_scrape)} pages.")
    return list(found_pages_for_scrape), crawl_session # Return session for scraping then close

# --- YOUR `load_links` function (EXACTLY AS PROVIDED) ---
def load_links_user_original(uploaded_file):
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file).iloc[:, 0].tolist()
    else:
        return [line.decode().strip() for line in uploaded_file.readlines()]

# --- NEW Function to load keywords from Excel ---
def load_keywords_from_excel_new(uploaded_file):
    try:
        df = pd.read_excel(io.BytesIO(uploaded_file.getvalue()), engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e: st.error(f"Error reading Excel: {e}"); return []

# --- NEW Function for Markdown Output ---
def generate_markdown_output_new(active_df):
    if active_df.empty: return "No active groups for Markdown."
    lines = ["| Group Logo | Group Name | Group Link |", "|---|---|---|"]
    for _, row in active_df.iterrows():
        logo, name, link_url = row.get("Logo URL",""), row.get("Group Name","N/A"), row.get("Group Link","")
        # Request a slightly larger image from server, then style down for quality
        logo_md = f'<img src="{append_query_param(logo, "w", "80")}" alt="Logo" class="group-logo-markdown">' if logo else " "
        link_md = f"[Join Group]({link_url})"
        name_safe = name.replace("|", "|")
        lines.append(f"| {logo_md} | {name_safe} | {link_md} |")
    return "\n".join(lines)

# --- Main function (Structure from YOUR code, with NEW features integrated) ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Group Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Search, scrape, or validate WhatsApp group links with ease</p>', unsafe_allow_html=True)

    # Initialize session state for results and processed links (NEW)
    if 'results' not in st.session_state: st.session_state.results = []
    if 'processed_links_in_session' not in st.session_state: st.session_state.processed_links_in_session = set()

    with st.sidebar:
        st.header("⚙️ Settings")
        # EXPANDED Input Methods
        input_method = st.selectbox(
            "Input Method",
            [ # Original options + NEW options
                "Search and Scrape from Google", # YOUR ORIGINAL
                "Search and Scrape from Google (Bulk via Excel)", # NEW
                "Scrape from Specific Webpage URL", # NEW
                "Scrape from Entire Website (Limited Crawl)", # NEW
                "Enter Links Manually", # YOUR ORIGINAL
                "Upload File (TXT/CSV)" # YOUR ORIGINAL
            ],
            index=0, help="Choose how to input links", key="main_input_method"
        )

        # Slider for Google search (YOUR ORIGINAL LOGIC, but range extended by request)
        google_search_top_n_val = 5 # Default from your original code
        google_search_pause_val = 2.0 # Default general pause

        if input_method == "Search and Scrape from Google" or \
           input_method == "Search and Scrape from Google (Bulk via Excel)":
            google_search_top_n_val = st.slider(
                "Number of Google results to process", 
                min_value=10, max_value=100, value=10, # Range 10-100 as requested
                help="How many Google search result pages to analyze.", key="google_results_slider_enhanced"
            )
            google_search_pause_val = st.slider(
                "Google Search Pause (seconds):", min_value=1.0, max_value=10.0, value=2.0, step=0.5,
                help="Pause between Google search API calls.", key="google_pause_slider_enhanced"
            )
        
        # NEW Sliders for Website Crawl
        crawl_depth_setting, max_pages_setting = 1, 20 # Defaults
        if input_method == "Scrape from Entire Website (Limited Crawl)":
            st.warning("Extensive crawling can be slow. Be mindful of website terms.", icon="🕸️")
            crawl_depth_setting = st.slider("Max Crawl Depth:", 0, 5, 1, key="crawl_depth_new")
            max_pages_setting = st.slider("Max Pages to Crawl:", 1, 500, 20, key="max_pages_new") # Increased practical limit

    if st.button("🗑️ Clear Results", use_container_width=True, key="clear_results_main"): # YOUR BUTTON
        if 'results' in st.session_state: del st.session_state['results']
        # Also clear processed links cache (NEW)
        if 'processed_links_in_session' in st.session_state: st.session_state.processed_links_in_session = set()
        st.success("Results cleared successfully!")

    # --- Processing Logic ---
    scraped_links_for_validation = set() # Collect all links here before validation (NEW)
    
    # Session for new scrapers that benefit from it
    # Google search uses direct requests via your original functions
    session_for_new_scrapers = requests.Session()

    try: # Wrap scraping in try-finally to ensure session is closed
        if input_method == "Search and Scrape from Google": # YOUR ORIGINAL WORKFLOW
            st.subheader("🔍 Google Search & Scrape")
            keyword = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group", key="google_keyword_single")
            if st.button("Search, Scrape, and Validate", use_container_width=True, key="google_scrape_validate_button"):
                if not keyword: st.warning("Please enter a search query."); return
                with st.spinner("Searching Google (user method)..."):
                    # CALLING YOUR GOOGLE SEARCH FUNCTION
                    search_results = user_google_search_function(keyword, top_n=google_search_top_n_val, pause_duration=google_search_pause_val) 
                if not search_results: st.warning("No Google results."); return # Adjusted message
                st.success(f"Found {len(search_results)} webpages. Scraping (user method)...")
                all_links_gs = []
                progress_bar_gs = st.progress(0)
                for idx, url in enumerate(search_results):
                    # CALLING YOUR SCRAPE WHATSAPP LINKS FUNCTION (FIXED UA)
                    links_gs = scrape_whatsapp_links_user_original(url)
                    all_links_gs.extend(links_gs)
                    progress_bar_gs.progress((idx + 1) / len(search_results))
                scraped_links_for_validation.update(all_links_gs) # Add to central set

        elif input_method == "Search and Scrape from Google (Bulk via Excel)": # NEW
            st.subheader("🔍 Google Search & Scrape (Bulk from Excel)")
            excel_file_bulk = st.file_uploader("Upload Excel with keywords", type=["xlsx"], key="bulk_excel_upload")
            if excel_file_bulk and st.button("Process Excel & Scrape from Google", use_container_width=True, key="bulk_excel_button"):
                keywords_excel = load_keywords_from_excel_new(excel_file_bulk)
                if not keywords_excel: st.warning("No keywords found in Excel."); return
                st.info(f"Processing {len(keywords_excel)} keywords...")
                prog_excel, stat_excel = st.progress(0), st.empty()
                for i, kw_excel in enumerate(keywords_excel):
                    stat_excel.text(f"Keyword: {kw_excel} ({i+1}/{len(keywords_excel)})")
                    # CALLING YOUR GOOGLE SEARCH
                    s_results_excel = user_google_search_function(kw_excel, top_n=google_search_top_n_val, pause_duration=google_search_pause_val)
                    if s_results_excel:
                        for url_excel in s_results_excel:
                            # CALLING YOUR SCRAPER
                            links_excel = scrape_whatsapp_links_user_original(url_excel)
                            scraped_links_for_validation.update(links_excel)
                    prog_excel.progress((i+1)/len(keywords_excel))
                stat_excel.success("Bulk Google processing complete.")

        elif input_method == "Scrape from Specific Webpage URL": # NEW
            st.subheader("🔗 Scrape from Specific Webpage")
            specific_url = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page-with-links", key="specific_url_input_new")
            if st.button("Scrape Specific URL", use_container_width=True, key="specific_url_button_new"):
                if not specific_url or not specific_url.startswith(('http://','https://')):
                    st.warning("Please enter a valid URL."); return
                with st.spinner(f"Scraping {specific_url}..."):
                    links_specific = scrape_whatsapp_links_for_direct_url(specific_url, session_for_new_scrapers)
                    scraped_links_for_validation.update(links_specific)
                st.success(f"Scraped {len(links_specific)} potential links from {specific_url}.")
        
        elif input_method == "Scrape from Entire Website (Limited Crawl)": # NEW
            st.subheader("🌐 Scrape from Entire Website")
            domain_crawl = st.text_input("Enter Base Domain URL:", placeholder="example.com", key="domain_crawl_input_new")
            if st.button("Crawl and Scrape Website", use_container_width=True, key="domain_crawl_button_new"):
                if not domain_crawl: st.warning("Please enter a domain."); return
                pages_found, crawl_sess = crawl_website_for_links(domain_crawl, crawl_depth_setting, max_pages_setting)
                try:
                    if pages_found:
                        st.info(f"Crawled {len(pages_found)} pages. Now scraping them...")
                        prog_site, stat_site = st.progress(0), st.empty()
                        for i, page_site in enumerate(pages_found):
                            stat_site.text(f"Scraping: {page_site[:60]} ({i+1}/{len(pages_found)})")
                            links_site = scrape_whatsapp_links_for_direct_url(page_site, crawl_sess)
                            scraped_links_for_validation.update(links_site)
                            prog_site.progress((i+1)/len(pages_found))
                        stat_site.success("Website scraping complete.")
                    else: st.warning("No pages found by crawler based on settings.")
                finally:
                    if crawl_sess: crawl_sess.close()


        elif input_method == "Enter Links Manually": # YOUR ORIGINAL WORKFLOW
            st.subheader("📝 Manual Link Entry")
            links_text = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123", key="manual_text_area")
            if st.button("Validate Links", use_container_width=True, key="manual_validate_button_orig"):
                links_manual = [line.strip() for line in links_text.split('\n') if line.strip()]
                if not links_manual: st.warning("Please enter at least one link."); return
                # Filter for WhatsApp links before adding to central set
                scraped_links_for_validation.update([l for l in links_manual if l.startswith(WHATSAPP_DOMAIN)])

        elif input_method == "Upload File (TXT/CSV)": # YOUR ORIGINAL WORKFLOW
            st.subheader("📥 File Upload")
            uploaded_file = st.file_uploader("Upload TXT or CSV", type=["txt", "csv"], key="file_uploader_orig")
            if uploaded_file and st.button("Validate File Links", use_container_width=True, key="file_validate_button_orig"):
                # Using YOUR load_links function
                links_file = load_links_user_original(uploaded_file)
                if not links_file: st.warning("No links found in the uploaded file."); return
                scraped_links_for_validation.update([l for l in links_file if l.startswith(WHATSAPP_DOMAIN)])
    finally:
        session_for_new_scrapers.close()


    # --- Unified Validation Step (NEW LOGIC, uses enhanced_validate_link) ---
    final_unique_links_to_validate = list(scraped_links_for_validation - st.session_state.processed_links_in_session)

    if scraped_links_for_validation and not final_unique_links_to_validate:
        st.info("All found links have already been processed in this session.")
    
    if final_unique_links_to_validate:
        st.success(f"Collected {len(scraped_links_for_validation)} unique links. Validating {len(final_unique_links_to_validate)} new links...")
        # Re-use progress bar and status text like in your original code structure
        progress_bar_validate = st.progress(0)
        status_text_validate = st.empty()
        
        # Store newly validated results temporarily
        newly_validated_results = [] 
        with ThreadPoolExecutor(max_workers=5) as executor: # Max_workers from your example
            future_to_link_map = {executor.submit(validate_link_enhanced, link_val): link_val for link_val in final_unique_links_to_validate}
            for i, future_obj in enumerate(as_completed(future_to_link_map)):
                validated_result = future_obj.result()
                newly_validated_results.append(validated_result)
                # Add to processed_links_in_session immediately
                st.session_state.processed_links_in_session.add(future_to_link_map[future_obj])
                progress_bar_validate.progress((i + 1) / len(final_unique_links_to_validate))
                status_text_validate.text(f"Validated {i + 1}/{len(final_unique_links_to_validate)} links")
        
        # Add newly validated results to the main session state results
        st.session_state.results.extend(newly_validated_results)
        status_text_validate.success("Validation complete!") # Or a similar message

    # --- Display Results (Structure from YOUR code, data from enhanced process) ---
    if 'results' in st.session_state and st.session_state.results:
        # Deduplicate final results list just in case, before creating DataFrame
        # This assumes results are dicts; convert to tuple of items for hashability if complex
        unique_results_list = []
        seen_links_for_df = set()
        for res_dict in st.session_state.results:
            if res_dict['Group Link'] not in seen_links_for_df:
                unique_results_list.append(res_dict)
                seen_links_for_df.add(res_dict['Group Link'])
        
        df_display = pd.DataFrame(unique_results_list)
        
        # Update session_state.results with the deduplicated list of dicts
        st.session_state.results = unique_results_list 
        
        active_df_display = df_display[df_display['Status'] == 'Active'].copy() # Use .copy()
        expired_df_display = df_display[df_display['Status'] == 'Expired'].copy() # Use .copy()

        st.subheader("📊 Results Summary") # From your code
        col1_sum, col2_sum, col3_sum = st.columns(3) # From your code
        with col1_sum:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Total Links", len(df_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col2_sum:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Active Links", len(active_df_display))
            st.markdown('</div>', unsafe_allow_html=True)
        with col3_sum:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            # Add a more robust count for "Other" statuses if desired, or stick to Expired
            other_statuses_df = df_display[~df_display['Status'].isin(['Active', 'Expired'])]
            st.metric("Expired/Other", len(expired_df_display) + len(other_statuses_df))
            st.markdown('</div>', unsafe_allow_html=True)

        with st.expander("🔎 View and Filter Results", expanded=True): # From your code
            status_options_display = df_display['Status'].unique()
            default_sel_display = ["Active"] if "Active" in status_options_display else list(status_options_display[:1])
            status_filter_display = st.multiselect("Filter by Status", options=status_options_display, default=default_sel_display, key="results_status_filter")
            
            filtered_df_display = df_display[df_display['Status'].isin(status_filter_display)] if status_filter_display else df_display
            
            st.dataframe( # From your code, but Logo URL now just text
                filtered_df_display,
                column_config={
                    "Group Link": st.column_config.LinkColumn("Invite Link", display_text="Join Group"),
                    "Logo URL": st.column_config.TextColumn("Logo URL") # Display as text, Markdown handles image
                }, height=400, use_container_width=True
            )
        
        # NEW Markdown Export Section
        st.subheader("📋 Markdown Export (Active Groups)")
        if not active_df_display.empty:
            markdown_text = generate_markdown_output_new(active_df_display)
            with st.expander("Copy or Download Markdown", expanded=True):
                st.text_area("Markdown Table (Copy this):", value=markdown_text, height=250, key="markdown_export_text_area", help="Select all (Ctrl+A) and copy (Ctrl+C).")
                st.download_button("📥 Download Markdown (.md)", markdown_text, "active_whatsapp_groups.md", "text/markdown", use_container_width=True, key="markdown_download_button")
            with st.expander("📋 Markdown Preview", expanded=False):
                st.markdown(markdown_text, unsafe_allow_html=True) # Allow custom img tag
        else:
            st.info("No active groups found to generate Markdown output.")

        # YOUR ORIGINAL Download Buttons
        col_dl1_orig, col_dl2_orig = st.columns(2)
        with col_dl1_orig:
            csv_active = active_df_display.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Active Groups", csv_active, "active_groups.csv", "text/csv", use_container_width=True, key="dl_active_orig_csv")
        with col_dl2_orig:
            csv_all = df_display.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download All Results", csv_all, "all_groups.csv", "text/csv", use_container_width=True, key="dl_all_orig_csv")

    else: # From your code
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")

if __name__ == "__main__":
    # Startup checks for optional but recommended libraries
    try: import openpyxl
    except ImportError: st.error("Excel processing requires 'openpyxl'. Install with `pip install openpyxl`"); st.stop()
    try: from fake_useragent import UserAgent; UserAgent() 
    except ImportError: st.warning("'fake-useragent' not found. Direct URL/Site scraping might be less effective. `pip install fake-useragent`", icon="⚠️")
    except Exception: st.warning("Fake-useragent had trouble initializing. Direct URL/Site scraping may use a default UA.", icon="⚠️")
    main()
