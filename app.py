import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
# import time # Not directly used now, googlesearch handles its own pause
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search as google_search_lib # Alias from your working example
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
# Stricter image pattern from your working example
IMAGE_PATTERN_VALIDATE = re.compile(r'https://pps\.whatsapp\.net/.*\.jpg\?[^&]*&[^&]+')
# Emoji pattern from your working example (for validate_link_legacy if used, but we use improved validate_link)
EMOJI_PATTERN_LEGACY = re.compile(
    "["
    u"\U0001F600-\U0001F64F"  # emoticons
    # ... (rest of your emoji pattern if you want to ensure it's identical for some legacy function)
    "]+",
    flags=re.UNICODE
)


HEADERS_GLOBAL = { # Global headers for requests, can be overridden
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}
MAX_VALIDATION_WORKERS = 10

# --- Custom CSS ---
st.markdown("""
<style>
/* ... (your existing CSS) ... */
.main-title { font-size: 2.5em; color: #25D366; text-align: center; margin-bottom: 0; font-weight: bold; }
.subtitle { font-size: 1.2em; color: #4A4A4A; text-align: center; margin-top: 0; }
.stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 8px 16px; }
.stButton>button:hover { background-color: #1EBE5A; color: #FFFFFF; }
.stProgress .st-bo { background-color: #25D366; }
.metric-card { background-color: #F5F6F5; padding: 12px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); color: #333333; text-align: center; }
.stTextInput input, .stTextArea textarea, .stFileUploader div[data-testid="stFileUploadDropzone"] { border: 1px solid #25D366 !important; border-radius: 5px !important; }
.sidebar .sidebar-content { background-color: #F5F6F5; }
.stExpander { border: 1px solid #E0E0E0; border-radius: 5px; }
.stAlert p { font-size: 0.95rem; }
</style>
""", unsafe_allow_html=True)

# --- Helper Functions ---

def append_query_param(url, param_name, param_value):
    if not url: return ""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    query_params[param_name] = [param_value]
    new_query_string = urlencode(query_params, doseq=True)
    return parsed_url._replace(query=new_query_string).geturl()

# --- User's exact scrape_whatsapp_links function for Google Search results ---
def scrape_whatsapp_links_user_method(url):
    """Scrape WhatsApp group links from a webpage. (User's provided method)"""
    try:
        headers = { # Specific headers from user's example
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        # This function uses its own requests.get(), not a session
        response = requests.get(url, headers=headers, timeout=10)
        # No explicit encoding set, let requests/bs4 handle
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links_found = [] # Use a list then convert to set for order preservation (though not strictly needed here)
        # 1. From <a> tags
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links_found.append(a['href'].split('?')[0]) # Normalize
        
        # 2. From text content
        for text in soup.stripped_strings: # User's method
            if WHATSAPP_DOMAIN in text:
                # User's regex for text search
                found_in_text = re.findall(r'https?://chat\.whatsapp\.com/[^\s]+', text)
                for flink in found_in_text:
                    links_found.append(flink.split('?')[0]) # Normalize
        
        return list(set(links_found)) # Deduplicate
    except Exception: # Broad exception catch as in user's example
        # Optionally log this error or notify user in a non-blocking way
        # st.sidebar.warning(f"User method scrape fail for {url[:30]}", icon="🕸️")
        return []


# --- Enhanced scraping function for "Specific Page" and "Entire Website" ---
def scrape_whatsapp_links_enhanced(url, session):
    """Scrape WhatsApp group links from a webpage, using session and more detailed error handling."""
    links = set()
    try:
        url_parse_for_error = urlparse(url)
        netloc_for_error = url_parse_for_error.netloc if url_parse_for_error.netloc else url[:30]

        response = session.get(url, headers=HEADERS_GLOBAL, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser') # Let BS4 handle encoding
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            if href and href.startswith(WHATSAPP_DOMAIN):
                links.add(href.split('?')[0])
        
        for text_chunk in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text_chunk:
                found_in_chunk = re.findall(r'https?://chat\.whatsapp\.com/[^\s"\'<>()]+', text_chunk)
                for link_url in found_in_chunk:
                    links.add(link_url.split('?')[0])
    except requests.exceptions.Timeout:
        st.sidebar.warning(f"Timeout scraping (enhanced) {netloc_for_error}", icon="⏱️")
    except requests.exceptions.RequestException as e:
        st.sidebar.warning(f"Scrape error (enhanced) on {netloc_for_error}: {type(e).__name__}", icon="⚠️")
    except Exception as e:
        st.sidebar.warning(f"Content parsing error (enhanced) on {netloc_for_error}: {type(e).__name__}", icon="💣")
    return list(links)


def validate_link(link): # Using the robust validation from previous enhanced version
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error"}
    try:
        response = requests.get(link, headers=HEADERS_GLOBAL, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"; return result
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid Link (Redirected)"; return result
        
        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')
        if meta_title and meta_title.get('content'):
            group_name = unescape(meta_title['content']).strip()
            result["Group Name"] = group_name or "Unnamed Group"
        else:
            title_tag = soup.find('h3') # Fallback
            result["Group Name"] = unescape(title_tag.get_text(strip=True)) or "Unnamed Group" if title_tag else "Unnamed Group"
        
        meta_image = soup.find('meta', property='og:image')
        if meta_image and meta_image.get('content'):
            img_src = unescape(meta_image['content'])
            if IMAGE_PATTERN_VALIDATE.match(img_src): result["Logo URL"] = img_src
        
        if not result["Logo URL"]: # Fallback to img tags
            img_tags = soup.find_all('img', src=True)
            for img in img_tags:
                src = unescape(img['src'])
                if IMAGE_PATTERN_VALIDATE.match(src): result["Logo URL"] = src; break
        
        page_text_lower = soup.get_text().lower()
        if any(btn_text.lower() in page_text_lower for btn_text in ["Join Chat", "Join Group", "View Group"]) or \
           "you can join this group" in page_text_lower or result["Group Name"] not in ["Unknown", "Unnamed Group"]:
            result["Status"] = "Active"
        elif "link is invalid" in page_text_lower or "link has been revoked" in page_text_lower or "link expired" in page_text_lower:
            result["Status"] = "Expired/Invalid"
        else: # Default heuristic
            result["Status"] = "Active" if result["Logo URL"] or result["Group Name"] not in ["Unknown", "Unnamed Group"] else "Expired/Invalid"
            
    except requests.exceptions.Timeout: result["Status"] = "Timeout Error"
    except requests.exceptions.RequestException: result["Status"] = "Network Error"
    except Exception: result["Status"] = "Parsing Error"
    return result

# User's google_search function (from their working example, for num_results parameter)
def perform_google_search_user_method(query, top_n=5, pause_duration=2.0):
    """Fetch URLs from Google's top N search results using googlesearch-python. (User's style)"""
    try:
        st.sidebar.info(f"Googling (user method) '{query}' (top {top_n}, pause: {pause_duration}s)...")
        # The key is `num_results` as in user's code. `search` is already imported as `google_search_lib`
        urls = list(google_search_lib(query, num_results=top_n, lang="en", pause=pause_duration))
        if not urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms.")
        return urls
    except TypeError as te:
         st.error(f"🚫 Google Search Error (user method) for '{query}': Parameter issue ({te}). Library version mismatch?")
         return []
    except Exception as e:
        error_str = str(e).lower()
        error_message_base = f"🚫 Google Search Error (user method) for '{query}'"
        if "http error 429" in error_str :
            st.error(f"{error_message_base}: Rate-limited by Google. Increase 'Google Search Pause'.")
        else:
            st.error(f"{error_message_base}: {e}. Increase 'Google Search Pause'.")
        return []


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
                response = session.get(current_url, headers=HEADERS_GLOBAL, timeout=7)
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
    # ... (same as before)
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
            if df.empty: return []
            for col in df.columns:
                if df[col].astype(str).str.contains(WHATSAPP_DOMAIN, case=False, na=False).any():
                    return df[col].dropna().astype(str).tolist()
            return df.iloc[:, 0].dropna().astype(str).tolist() # Fallback
        else: # TXT file
            return [line.decode().strip() for line in uploaded_file.readlines() if line.strip()]
    except Exception as e:
        st.error(f"Error reading file {uploaded_file.name}: {e}")
        return []

def load_keywords_from_excel(uploaded_file):
    # ... (same as before)
    try:
        excel_data = io.BytesIO(uploaded_file.getvalue())
        df = pd.read_excel(excel_data, engine='openpyxl')
        if df.empty: st.warning("Excel file is empty."); return []
        return df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception as e:
        st.error(f"Error reading Excel {uploaded_file.name}: {e}. Ensure 'openpyxl' installed.")
        return []

def generate_markdown_output(active_results_df):
    if active_results_df.empty: return "No active groups found to generate Markdown."
    markdown_lines = ["| Group Logo | Group Name | Group Link |", "|---|---|---|"]
    for _, row in active_results_df.iterrows():
        logo_url = row.get("Logo URL", "")
        group_name = row.get("Group Name", "N/A")
        group_link = row.get("Group Link", "")
        # --- LOGO SIZE REDUCED TO 50px ---
        logo_md = f"![Logo]({append_query_param(logo_url, 'w', '50')})" if logo_url else " "
        link_md = f"[Join Group]({group_link})"
        safe_group_name = group_name.replace("|", "|")
        markdown_lines.append(f"| {logo_md} | {safe_group_name} | {link_md} |")
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
            "Search & Scrape from Google (Single Keyword)", "Search & Scrape from Google (Bulk via Excel)",
            "Scrape from Specific Webpage URL", "Scrape from Entire Website (Extensive Crawl)",
            "Enter Links Manually (for Validation)", "Upload Link File (TXT/CSV for Validation)"
        ], index=0)

        # --- GOOGLE RESULTS SLIDER 1-10 ---
        google_results_top_n = 5 # Default, matching user's original
        google_search_pause_seconds = 2.5
        crawl_depth, max_crawl_pages = 2, 50

        if "Google" in input_method:
            google_results_top_n = st.slider( # Renamed variable for clarity
                "Number of top Google results to process (per keyword):",
                min_value=1, max_value=10, value=5, step=1, # Range 1-10, default 5
                help="Number of Google search result pages to analyze."
            )
            google_search_pause_seconds = st.slider(
                "Google Search Pause (seconds):", min_value=1.0, max_value=10.0, value=2.5, step=0.5,
                help="Pause between Google requests to avoid rate-limiting."
            )
        if "Entire Website" in input_method:
            st.warning("⚠️ Extensive website crawling can be very slow. Use with caution.", icon="🚨")
            crawl_depth = st.slider("Max Crawl Depth:", min_value=0, max_value=10, value=2)
            max_crawl_pages = st.slider("Max Pages to Crawl:", min_value=1, max_value=1000, value=50) # High max for "unlimited" feel
        
        if st.button("🗑️ Clear All Results & Cache", use_container_width=True, key="clear_all"):
            st.session_state.results, st.session_state.processed_links_in_session = [], set()
            st.success("All results and cache cleared!")

    all_scraped_links = set()
    st.subheader(f"🚀 Action Zone: {input_method}")
    
    # Create one session for methods that benefit from it (Specific Page, Entire Website's internal scraping)
    # Google search parts will use direct requests via scrape_whatsapp_links_user_method
    enhanced_session = requests.Session() 
    try:
        if input_method == "Search & Scrape from Google (Single Keyword)":
            keyword = st.text_input("Enter Google Search Query:", placeholder="e.g., Tech WhatsApp groups")
            if st.button("🔍 Search, Scrape (User Method), Validate", use_container_width=True):
                if not keyword: st.warning("Please enter a search query.")
                else:
                    search_page_urls = perform_google_search_user_method(
                        keyword, 
                        top_n=google_results_top_n, # Use the 1-10 slider value
                        pause_duration=google_search_pause_seconds
                    )
                    if search_page_urls:
                        st.info(f"Found {len(search_page_urls)} pages. Scraping with user method...")
                        prog_bar = st.progress(0)
                        for i, page_url in enumerate(search_page_urls):
                            # --- CALLING USER'S SCRAPING METHOD ---
                            links_from_page = scrape_whatsapp_links_user_method(page_url)
                            all_scraped_links.update(links_from_page)
                            prog_bar.progress((i+1)/len(search_page_urls))
                        st.success("Google page scraping complete.")

        elif input_method == "Search & Scrape from Google (Bulk via Excel)":
            excel_file = st.file_uploader("Upload Excel (keywords in 1st col)", type=["xlsx"])
            if excel_file and st.button("📄 Process Excel, Scrape (User Method), Validate", use_container_width=True):
                keywords = load_keywords_from_excel(excel_file)
                if not keywords: st.warning("No keywords in Excel.")
                else:
                    st.info(f"{len(keywords)} keywords. Starting Google searches & user method scraping...")
                    prog, stat_txt = st.progress(0), st.empty()
                    for i, kw in enumerate(keywords):
                        stat_txt.write(f"Keyword: **{kw}** ({i+1}/{len(keywords)})")
                        search_page_urls = perform_google_search_user_method(
                            kw, top_n=google_results_top_n, pause_duration=google_search_pause_seconds)
                        if search_page_urls:
                            for page_url in search_page_urls:
                                # --- CALLING USER'S SCRAPING METHOD ---
                                links_from_page = scrape_whatsapp_links_user_method(page_url)
                                all_scraped_links.update(links_from_page)
                        prog.progress((i + 1) / len(keywords))
                    stat_txt.success("Bulk processing (user method) complete.")
        
        elif input_method == "Scrape from Specific Webpage URL":
            page_url = st.text_input("Enter Webpage URL:", placeholder="https://example.com/page")
            if st.button("🔗 Scrape Page (Enhanced Method) & Validate", use_container_width=True):
                if not page_url or not (page_url.startswith("http://") or page_url.startswith("https://")):
                    st.warning("Please enter a valid URL.")
                else:
                    with st.spinner(f"Scraping {page_url} (enhanced method)..."):
                        # --- CALLING ENHANCED SCRAPING METHOD ---
                        links_from_page = scrape_whatsapp_links_enhanced(page_url, enhanced_session)
                        all_scraped_links.update(links_from_page)
                    st.success(f"Scraping of {page_url} complete.")

        elif input_method == "Scrape from Entire Website (Extensive Crawl)":
            domain_url = st.text_input("Enter Base Domain URL:", placeholder="example.com")
            if st.button("🌐 Crawl & Scrape (Enhanced Method)", use_container_width=True):
                if not domain_url: st.warning("Please enter a domain URL.")
                else:
                    # crawl_website returns its own session, use that for scraping its found pages
                    pages_to_scrape, crawl_used_session = crawl_website(domain_url, max_depth=crawl_depth, max_pages=max_crawl_pages)
                    try:
                        if pages_to_scrape:
                            st.info(f"Crawled. Now scraping {len(pages_to_scrape)} pages (enhanced method)...")
                            prog, stat_txt = st.progress(0), st.empty()
                            for i, p_url in enumerate(pages_to_scrape):
                                stat_txt.text(f"Scraping: {p_url[:60]}... ({i+1}/{len(pages_to_scrape)})")
                                # --- CALLING ENHANCED SCRAPING METHOD with CRAWLER'S SESSION ---
                                links_from_page = scrape_whatsapp_links_enhanced(p_url, crawl_used_session)
                                all_scraped_links.update(links_from_page)
                                prog.progress((i + 1) / len(pages_to_scrape))
                            stat_txt.success("Website scraping complete.")
                        else: st.warning("No pages found/scraped from domain.")
                    finally:
                        crawl_used_session.close() # Close crawler's session

        elif input_method == "Enter Links Manually (for Validation)":
            # ... (same as before)
            links_text = st.text_area("Enter WhatsApp Links (one per line):", height=150, placeholder=f"{WHATSAPP_DOMAIN}LINK1\n{WHATSAPP_DOMAIN}LINK2")
            if st.button("✍️ Validate Manual Links", use_container_width=True):
                raw_links = [line.strip() for line in links_text.split('\n') if line.strip().startswith(WHATSAPP_DOMAIN)]
                if not raw_links: st.warning("Please enter at least one valid WhatsApp link.")
                else: all_scraped_links.update(raw_links)

        elif input_method == "Upload Link File (TXT/CSV for Validation)":
            # ... (same as before)
            uploaded_file = st.file_uploader("Upload TXT/CSV with WhatsApp links", type=["txt", "csv"])
            if uploaded_file and st.button("📤 Validate File Links", use_container_width=True):
                raw_links = load_links_from_text_file(uploaded_file)
                valid_whatsapp_links = [link for link in raw_links if link.startswith(WHATSAPP_DOMAIN)]
                if not valid_whatsapp_links: st.warning("No valid WhatsApp links in file.")
                else: all_scraped_links.update(valid_whatsapp_links)
    finally:
        enhanced_session.close() # Close the main enhanced_session

    # --- Unified Validation Step --- (same as before)
    if all_scraped_links:
        links_to_validate_now = list(all_scraped_links - st.session_state.processed_links_in_session)
        if not links_to_validate_now:
            st.info("No new WhatsApp links found or all previously found links processed.")
        else:
            st.success(f"Found {len(all_scraped_links)} total unique links. Validating {len(links_to_validate_now)} new links...")
            prog_val, stat_val = st.progress(0), st.empty()
            new_results = []
            with ThreadPoolExecutor(max_workers=MAX_VALIDATION_WORKERS) as executor:
                future_to_link = {executor.submit(validate_link, link): link for link in links_to_validate_now}
                for i, future in enumerate(as_completed(future_to_link)):
                    link, result = future_to_link[future], future.result()
                    new_results.append(result)
                    st.session_state.processed_links_in_session.add(link)
                    prog_val.progress((i + 1) / len(links_to_validate_now))
                    stat_val.text(f"Validating: {i + 1}/{len(links_to_validate_now)} - {link.split('/')[-1]}")
            st.session_state.results.extend(new_results)
            stat_val.success(f"Validation complete for {len(links_to_validate_now)} new links!")

    # --- Display Results --- (same as before, with markdown logo size change already handled in generate_markdown_output)
    if st.session_state.results:
        df_results = pd.DataFrame(st.session_state.results).drop_duplicates(subset=['Group Link'], keep='first')
        st.session_state.results = df_results.to_dict('records')
        active_df = df_results[df_results['Status'] == 'Active'].copy()
        expired_df_count = len(df_results[df_results['Status'].isin(['Expired/Invalid', 'Invalid Link (Redirected)'])])
        error_df_count = len(df_results[df_results['Status'].str.contains("Error", case=False, na=False) | df_results['Status'].str.startswith('HTTP Error', na=False)])
        
        st.subheader("📊 Results Summary")
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("Total Processed", len(df_results))
        c2.metric("Active Links", len(active_df))
        c3.metric("Expired/Invalid", expired_df_count)
        c4.metric("Errors", error_df_count)

        with st.expander("🔎 View, Filter & Download All Results", expanded=False):
            opts = ["All"] + sorted(df_results['Status'].unique().tolist())
            dflt = ["Active"] if "Active" in opts else opts[:1]
            sel_stat = st.multiselect("Filter by Status:", options=opts, default=dflt)
            filt_df = df_results[df_results['Status'].isin(sel_stat)] if sel_stat and "All" not in sel_stat else df_results
            st.dataframe(filt_df, column_config={"Group Link": st.column_config.LinkColumn("Invite Link", display_text="Open Link"), "Logo URL": st.column_config.ImageColumn("Logo", width="small")}, height=400, use_container_width=True)
            st.download_button("📥 Download Filtered (CSV)", filt_df.to_csv(index=False).encode('utf-8'), "filtered_groups.csv", "text/csv", use_container_width=True)
        
        st.subheader("📋 Markdown Export (Active Groups)")
        if not active_df.empty:
            md_data = generate_markdown_output(active_df) # Logo size 50px handled here
            with st.expander("Copy or Download Markdown", expanded=True):
                st.text_area("Markdown Table (Copy this):", value=md_data, height=250, key="md_area", help="Ctrl+A then Ctrl+C")
                st.download_button("📥 Download Markdown (.md)", md_data, "active_groups.md", "text/markdown", use_container_width=True, key="dl_md")
            with st.expander("📋 Markdown Preview", expanded=False): st.markdown(md_data, unsafe_allow_html=True)
        else: st.info("No active groups for Markdown output.")
    else: st.info("🏁 Start by choosing an input method and providing data.", icon="ℹ️")

if __name__ == "__main__":
    try: import openpyxl
    except ImportError: st.error("Lib 'openpyxl' for Excel missing. `pip install openpyxl`"); st.stop()
    main()
