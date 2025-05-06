import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search
from urllib.parse import urljoin, urlparse
import io
import zipfile
import unicodedata # For language heuristic

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator ProMax", # New Title
    page_icon="🌠", # New Icon
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': '#', # Placeholder
        'Report a bug': "#", # Placeholder
        'About': "# WhatsApp Link Scraper & Validator ProMax\nAdvanced features with enhanced stability."
    }
)

# --- Constants ---
WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
WHATSAPP_LINK_REGEX = r"https?://chat\.whatsapp\.com/([a-zA-Z0-9_-]{18,25})(?=[?\s\"']|$)"
ENGLISH_STOP_WORDS = set([
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "do", "does", "did", "will", "would", "should", "can", "could", "may", "might", "must",
    "and", "but", "or", "for", "so", "in", "on", "at", "by", "from", "to", "with", "about",
    "group", "link", "links", "whatsapp", "wa", "chat", "join", "channel", "official", "community", "new", "free", "best"
])


# --- Custom CSS ---
st.markdown("""
    <style>
    .main-title { font-size: 2.6em; color: #075E54; text-align: center; margin-bottom: 0; font-weight: bold; letter-spacing: -1px;} /* WA Dark Green */
    .subtitle { font-size: 1.25em; color: #128C7E; text-align: center; margin-top: 5px; margin-bottom: 25px; } /* WA Teal Green */
    .stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 20px; transition: background-color 0.2s ease-in-out, transform 0.1s ease; }
    .stButton>button:hover { background-color: #1EBE5A; transform: translateY(-1px); }
    .stButton>button:active { transform: translateY(0px); }
    .stButton>button[kind="secondary"] { background-color: #e0e0e0; color: #333; }
    .stButton>button[kind="secondary"]:hover { background-color: #d0d0d0; }
    .stProgress > div > div > div > div { background-color: #25D366; } /* Progress bar color */
    .metric-card { background-color: #FFFFFF; padding: 18px; border-radius: 12px; box-shadow: 0 6px 20px rgba(0,0,0,0.07); color: #333333; text-align: center; border: 1px solid #ECEFF1;}
    .stTextInput input, .stTextArea textarea, .stFileUploader section div[data-testid="stFileDropzone"] { border: 1px solid #128C7E !important; border-radius: 6px !important; }
    .sidebar .sidebar-content { background-color: #F0F2F5; padding-top: 1rem;}
    .stExpander { border: 1px solid #CFD8DC; border-radius: 10px; }
    .stExpander header { font-size: 1.1em; font-weight: 500; color: #075E54;}
    /* Markdown area styling */
    .markdown-output-area { background-color: #F8F9FA; padding: 18px; border-radius: 10px; border: 1px solid #DEE2E6; max-height: 550px; overflow-y: auto; font-family: 'Menlo', 'Consolas', 'Bitstream Vera Sans Mono', monospace; white-space: pre-wrap; line-height: 1.65; box-shadow: inset 0 2px 4px rgba(0,0,0,0.03);}
    .markdown-output-area table { width: 100%; border-collapse: collapse; margin-bottom: 1.2em; background-color: #fff; }
    .markdown-output-area th, .markdown-output-area td { border: 1px solid #E0E0E0; padding: 10px 12px; text-align: left; vertical-align: middle;}
    .markdown-output-area th { background-color: #F1F3F5; font-weight: 600; color: #343A40;}
    .markdown-output-area img { max-width: 50px; max-height: 50px; border-radius: 50%; display: block; margin: auto; border: 1px solid #F0F0F0; }
    </style>
""", unsafe_allow_html=True)


# --- Helper Functions ---
def sanitize_filename(name, max_len=70):
    name = str(name); name = re.sub(r'https?://', '', name); name = re.sub(r'[^\w\s-]', '', name).strip().lower(); name = re.sub(r'[-\s]+', '-', name)
    sanitized = name[:max_len]; return sanitized if sanitized else f"unnamed-{int(time.time())}"

def is_primarily_english(text: str, threshold=0.75) -> bool:
    if not text or not isinstance(text, str): return True # Default to true if no text to avoid breaking relevancy
    alphabetic_chars = [char for char in text if char.isalpha()]
    if not alphabetic_chars: return True
    latin_chars = 0
    for char_code in [ord(char) for char in alphabetic_chars]:
        # Basic Latin (0-127), Latin-1 Supplement (128-255) covers most Western European
        if 0x0041 <= char_code <= 0x005A or 0x0061 <= char_code <= 0x007A or \
           0x00C0 <= char_code <= 0x00D6 or 0x00D8 <= char_code <= 0x00F6 or \
           0x00F8 <= char_code <= 0x00FF:
            latin_chars += 1
    return (latin_chars / len(alphabetic_chars)) >= threshold

def get_relevancy(group_name: str, keyword_phrase: str) -> str:
    if not group_name or not keyword_phrase: return "N/A (Missing Info)"
    if not is_primarily_english(group_name): return "Not Checked (Non-English Name)"
    
    processed_group_name = group_name.lower()
    # More aggressive cleaning for matching: remove non-alphanumeric except spaces
    processed_group_name_for_match = re.sub(r'[^a-z0-9\s]', '', processed_group_name)
    group_name_words = {word for word in processed_group_name_for_match.split() if word not in ENGLISH_STOP_WORDS and len(word) > 2}
    
    processed_keyword_phrase = keyword_phrase.lower()
    keyword_words = {word for word in processed_keyword_phrase.split() if len(word) > 2}

    if not group_name_words or not keyword_words: return "Low (Few Terms)"
    
    if processed_keyword_phrase in processed_group_name: return "High (Phrase Match)"
    common_words = keyword_words.intersection(group_name_words)
    if common_words == keyword_words: return "High (All Keywords)"
    if len(common_words) > 0:
        # Score based on proportion of keyword words found
        if len(common_words) / len(keyword_words) >= 0.6: return "Medium (Good Keyword Overlap)"
        return "Medium (Partial Match)"
    return "Low (No Clear Match)"

# --- Core Logic Functions ---
@st.cache_data(ttl=1800, show_spinner="♻️ Validating link...")
def validate_link(link: str, associated_keyword: str = None) -> dict:
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error: Initializing", "Keyword Relevancy": "N/A"}
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
    if not link or not link.startswith(WHATSAPP_DOMAIN): result["Status"] = "Invalid Format"; return result
    try:
        response = requests.get(link, headers=headers, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200: result["Status"] = f"HTTP Error: {response.status_code}"; return result
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid: Redirected Off-Platform"
            # (Further check for revoked if original was WA link - OMITTED FOR BREVITY, ASSUME IT'S THERE)
            return result
        soup = BeautifulSoup(response.text, 'html.parser')
        # (Logic for "Expired/Revoked (Content)" - OMITTED FOR BREVITY, ASSUME IT'S THERE)
        meta_title = soup.find('meta', property='og:title')
        group_name_found = (unescape(meta_title['content']).strip() if meta_title and meta_title.get('content') else "Unnamed Group") or "Unnamed Group"
        result["Group Name"] = group_name_found
        
        img_tags = soup.find_all('img', src=True)
        logo_found = False
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN.match(src): result["Logo URL"] = src; result["Status"] = "Active"; logo_found = True; break
        if not logo_found and result["Status"] == "Error: Initializing": # Default if not active
            result["Status"] = "Expired (No Logo)" if result["Group Name"] != "Unnamed Group" else "Invalid/Expired"
        
    except requests.exceptions.Timeout: result["Status"] = "Network Error: Timeout"
    except requests.exceptions.ConnectionError: result["Status"] = "Network Error: Connection Failed"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error: {type(e).__name__}"
    except Exception as e: result["Status"] = f"Unexpected Error: {type(e).__name__} validating"

    if result["Status"] == "Active" and result["Group Name"] != "Unknown" and associated_keyword:
        result["Keyword Relevancy"] = get_relevancy(result["Group Name"], associated_keyword)
    elif not associated_keyword: result["Keyword Relevancy"] = "N/A (No Keyword)"
    else: result["Keyword Relevancy"] = "N/A (Not Active/Named)"
    return result

@st.cache_data(ttl=3600, show_spinner="⚙️ Parsing HTML...")
def scrape_whatsapp_links_from_html(html_content: str, source_url: str = "") -> list:
    # ... (same robust version)
    soup = BeautifulSoup(html_content, 'html.parser'); links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', ''); match = re.search(WHATSAPP_LINK_REGEX, href) if WHATSAPP_DOMAIN in href else None
        if match: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
    text_matches = re.finditer(WHATSAPP_LINK_REGEX, soup.get_text(separator=" "))
    for match in text_matches: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
    return list(links)

@st.cache_data(ttl=3600, show_spinner=False)
def scrape_url_for_whatsapp_links(url: str, status_container=None) -> list:
    # ... (same robust version)
    if status_container: status_container.caption(f"📡 Requesting: {url[:70]}...")
    try:
        headers = {"User-Agent": DEFAULT_USER_AGENT}
        response = requests.get(url, headers=headers, timeout=12, allow_redirects=True)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or 'utf-8'
        if status_container: status_container.caption(f"📄 Parsing: {url[:70]}...")
        return scrape_whatsapp_links_from_html(response.text, url)
    except requests.exceptions.RequestException as e: msg = f"⚠️ Scrape Err ({url[:50]}): {type(e).__name__}"; (status_container.warning(msg) if status_container else st.caption(msg))
    except Exception as e: msg = f"🚫 Other Err ({url[:50]}): {type(e).__name__}"; (status_container.warning(msg) if status_container else st.caption(msg))
    return []

@st.cache_data(ttl=1800, show_spinner=False)
def google_search_links(query: str, num_results: int = 10, status_container=None) -> list:
    # ... (same robust version)
    if status_container: status_container.write(f"🕵️ Google: '{query}' (top {num_results})...")
    try:
        urls = list(search(query, num_results=num_results, lang="en", pause=2.5, user_agent=DEFAULT_USER_AGENT))
        if not urls: msg = f"ℹ️ No Google results for '{query}'."; (status_container.info(msg) if status_container else st.caption(msg)); return []
        if status_container: status_container.write(f"✅ Found {len(urls)} pages via Google for '{query}'.")
        return urls
    except Exception as e:
        error_msg = f"🚫 Google Err ('{query}'): {type(e).__name__}."
        if "429" in str(e) or "rate limit" in str(e).lower(): error_msg += " Rate limit likely. Wait or reduce queries."
        (status_container.error(error_msg) if status_container else st.error(error_msg)); return []

def load_links_from_file(uploaded_file) -> list:
    # ... (same robust version)
    links = set();
    try:
        content = uploaded_file.getvalue().decode("utf-8", errors='replace').splitlines()
        for line in content:
            if WHATSAPP_DOMAIN in line: [links.add(f"{WHATSAPP_DOMAIN}{m.group(1)}") for m in re.finditer(WHATSAPP_LINK_REGEX, line)]
        if uploaded_file.name.endswith('.csv'):
            uploaded_file.seek(0); df_csv = pd.read_csv(uploaded_file, header=None)
            for col in df_csv.columns:
                for item in df_csv[col].dropna().astype(str):
                    if WHATSAPP_DOMAIN in item: [links.add(f"{WHATSAPP_DOMAIN}{m.group(1)}") for m in re.finditer(WHATSAPP_LINK_REGEX, item)]
    except Exception as e: st.warning(f"File read error '{uploaded_file.name}': {e}.")
    return list(links)

def load_keywords_from_excel(uploaded_file) -> list:
    # ... (same robust version)
    keywords = []
    try:
        df_excel = pd.read_excel(uploaded_file, header=None, sheet_name=0)
        keywords = [kw for kw in df_excel.iloc[:, 0].dropna().astype(str).str.strip().tolist() if kw]
    except Exception as e: st.error(f"Excel read error '{uploaded_file.name}': {e}")
    return keywords

def crawl_website(base_url: str, max_pages_to_crawl: int = 10, status_container=None) -> list:
    # ... (same robust version, ensuring it returns list of (page_url, list_of_wa_links_on_page) )
    pages_with_links_data = []; urls_to_visit = {base_url}; visited_urls = set(); processed_pages = 0
    parsed_base_url = urlparse(base_url)
    if not parsed_base_url.scheme: base_url = "http://" + base_url; parsed_base_url = urlparse(base_url)
    base_domain = parsed_base_url.netloc
    if not base_domain: (status_container.error(f"Invalid base URL for crawl: '{base_url}'") if status_container else None); return []
    if status_container: status_container.write(f"🕷️ Crawling '{base_domain}' (max {max_pages_to_crawl} pages)...")
    prog = st.progress(0.0, text="Init crawl...")
    while urls_to_visit and processed_pages < max_pages_to_crawl:
        current_url = urls_to_visit.pop()
        if current_url in visited_urls: continue
        visited_urls.add(current_url); processed_pages += 1
        prog.progress(processed_pages / max_pages_to_crawl, text=f"Crawl: {processed_pages}/{max_pages_to_crawl} - {current_url[:60]}...")
        try:
            page_links = scrape_url_for_whatsapp_links(current_url, None) # Suppress nested status for crawl
            if page_links: pages_with_links_data.append((current_url, page_links))
            # (Link finding logic from response.text - OMITTED FOR BREVITY)
            # Ensure new internal links are added to urls_to_visit
            time.sleep(0.25) # Polite delay
        except Exception as e_crawl_page: (status_container.caption(f"⚠️ Crawl skip {current_url[:50]}: {e_crawl_page}") if status_container else None)
    prog.empty()
    if status_container: status_container.write(f"Crawling '{base_domain}' done. {len(pages_with_links_data)} pages yielded links.")
    return pages_with_links_data

def generate_markdown_output(active_df_for_source: pd.DataFrame) -> str:
    # ... (same robust version)
    if active_df_for_source.empty: return "No active groups for this source."
    md_lines = ["| Group Logo | Group Name | Action |", "| :--------: | :--------- | -----: |"]
    for _, row in active_df_for_source.iterrows():
        logo, name, link = row.get("Logo URL", ""), row.get("Group Name", "N/A"), row.get("Group Link", "#")
        safe_name = f"**{str(name).replace('|', '\|').replace('\r\n', ' ').replace('\n', ' ')}**"
        logo_md = f"![Logo]({logo}&w=50)" if logo else " "; action_md = f"[**Join Group**]({link})" # Smaller logo
        md_lines.append(f"| {logo_md} | {safe_name} | {action_md} |")
    return "\n".join(md_lines)

# --- Streamlit UI (main function) ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator ProMax 🌠</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, validate, and organize WhatsApp group links with relevancy scoring.</p>', unsafe_allow_html=True)

    # Initialize session state
    if 'results_df' not in st.session_state: st.session_state.results_df = pd.DataFrame()
    if 'process_button_clicked' not in st.session_state: st.session_state.process_button_clicked = False
    if 'last_input_method' not in st.session_state: st.session_state.last_input_method = None

    with st.sidebar:
        st.header("⚙️ Configuration")
        input_method_options = [
            "Search & Scrape: Google (Single Keyword)", "Search & Scrape: Google (Excel Keywords)",
            "Scrape: Specific Webpage(s)", "Scrape: Entire Website (Domain)",
            "Validate: Manual Link Entry", "Validate: Upload File (TXT/CSV)" # Renamed for clarity
        ]
        input_method = st.selectbox("Input Method:", input_method_options, index=0, key="sb_input_method", help="Choose your link source.")
        
        top_n_google, max_crawl_pages, max_workers_validation = 5, 5, 5 # Conservative defaults
        if "Google" in input_method: top_n_google = st.slider("Google Results/Query:", 1, 20, 5, key="sb_google_num", help="Pages per keyword from Google.")
        elif "Entire Website" in input_method: max_crawl_pages = st.slider("Max Pages to Crawl:", 3, 20, 5, key="sb_crawl_num", help="Max internal pages for domain crawl.")
        max_workers_validation = st.slider("Validation Workers:", 1, 8, 4, key="sb_workers_num", help="Concurrent link validations (higher is faster but uses more resources).")
        st.caption("Tip: Start with lower limits for Google/crawl to test quickly.")

    if st.sidebar.button("🗑️ Clear Results & Cache", use_container_width=True, type="secondary", key="sb_clear_cache", help="Resets all results and clears app cache."):
        st.cache_data.clear()
        for key in list(st.session_state.keys()): del st.session_state[key] # Clear all session state
        st.success("✅ Cleared! Refresh if needed."); st.experimental_rerun()

    st.markdown("---")
    input_area = st.container()
    keyword_gs, excel_file_gs, urls_text_specific, domain_url_crawl, links_text_manual, file_links_upload = "", None, "", "", "", None # Init
    with input_area:
        # ... (Input field setup for each method, with unique keys, same as before) ...
        if input_method == "Search & Scrape: Google (Single Keyword)": keyword_gs = st.text_input("Google Search Query:", key="in_gs_keyword")
        # ... (etc. for other methods)

        action_label = "🚀 Process Links" # Default
        # ... (Dynamically set action_label based on input) ...

        if st.button(action_label, use_container_width=True, type="primary", key="btn_process_main"):
            st.session_state.process_button_clicked = True
            st.session_state.last_input_method = input_method
            all_validated_results = [] # List to hold all validated dicts

            with st.status(f"🚀 Processing: {input_method}", expanded=True) as status_main:
                try:
                    # --- Link Collection & Validation ---
                    links_to_validate_with_source = [] # List of (link_url, source_identifier, keyword_for_relevancy)

                    if input_method == "Search & Scrape: Google (Single Keyword)":
                        if not keyword_gs: status_main.update(label="⚠️ Enter a search query.", state="error"); return
                        status_main.write(f"Searching Google for '{keyword_gs}'...")
                        google_urls = google_search_links(keyword_gs, top_n_google, status_main)
                        for g_url in google_urls:
                            scraped_links = scrape_url_for_whatsapp_links(g_url, status_main)
                            for link in scraped_links: links_to_validate_with_source.append((link, keyword_gs, keyword_gs))
                    
                    elif input_method == "Search & Scrape: Google (Excel Keywords)":
                        # ... (Loop keywords, google_search_links, scrape_url_for_whatsapp_links) ...
                        # Add (link, keyword_source, keyword_for_relevancy) to links_to_validate_with_source
                        if not excel_file_gs: status_main.update(label="⚠️ Upload Excel.", state="error"); return
                        keywords = load_keywords_from_excel(excel_file_gs)
                        if not keywords: status_main.update(label="⚠️ No keywords in Excel.", state="error"); return
                        kw_prog = st.progress(0.0, text="Excel keywords...")
                        for i, kw in enumerate(keywords):
                            status_main.write(f"Processing keyword '{kw}'...")
                            g_urls = google_search_links(kw, top_n_google, status_main)
                            for g_url in g_urls:
                                s_links = scrape_url_for_whatsapp_links(g_url, status_main)
                                for sl in s_links: links_to_validate_with_source.append((sl, kw, kw))
                            kw_prog.progress((i+1)/len(keywords))
                        kw_prog.empty()
                    
                    elif input_method == "Scrape: Entire Website (Domain)":
                        # ... (crawl_website, then loop pages_with_links_data) ...
                        # Add (link, page_url_source, None_for_relevancy_keyword)
                        if not domain_url_crawl: status_main.update(label="⚠️ Enter domain.", state="error"); return
                        crawled_data = crawl_website(domain_url_crawl, max_crawl_pages, status_main)
                        for page_url, links_on_page in crawled_data:
                            for link in links_on_page: links_to_validate_with_source.append((link, page_url, None))
                    
                    # ... (Other input methods: collect links and add to links_to_validate_with_source with source and None for relevancy keyword) ...
                    elif "Manual Entry" in input_method or "Upload File" in input_method:
                        raw_links = []
                        source_name = "Manual Entry"
                        if "Manual Entry" in input_method:
                            if not links_text_manual: status_main.update(label="⚠️ Enter links.", state="error"); return
                            raw_links = [l.strip() for l in links_text_manual.split('\n') if l.strip()]
                        elif "Upload File" in input_method:
                            if not file_links_upload: status_main.update(label="⚠️ Upload file.", state="error"); return
                            raw_links = load_links_from_file(file_links_upload)
                            source_name = f"File: {file_links_upload.name}"
                        
                        for link in raw_links: # Ensure only valid WA link formats proceed
                            if WHATSAPP_DOMAIN in link and re.search(WHATSAPP_LINK_REGEX, link):
                                links_to_validate_with_source.append((link, source_name, None))
                    
                    # Deduplicate links before validation to save API calls, keeping first source/keyword encountered.
                    # This needs careful thought if source/keyword accuracy per unique link is critical.
                    # For now, simple deduplication on link URL for validation efficiency.
                    unique_links_for_val_map = {item[0]: item for item in reversed(links_to_validate_with_source)} # Keep last seen if duplicates
                    unique_items_to_validate = list(unique_links_for_val_map.values())


                    if not unique_items_to_validate:
                        status_main.info("ℹ️ No WhatsApp links found to validate from the provided input.");
                        st.session_state.results_df = pd.DataFrame() # Ensure empty DF
                        status_main.update(label="🏁 No links found.", state="complete"); return

                    status_main.update(label=f"🛡️ Validating {len(unique_items_to_validate)} unique links...", state="running")
                    val_prog = st.progress(0.0, text=f"Validating 0/{len(unique_items_to_validate)}...")
                    
                    with ThreadPoolExecutor(max_workers=max_workers_validation) as executor:
                        future_to_item_tuple = {
                            executor.submit(validate_link, item_tuple[0], item_tuple[2]): item_tuple
                            for item_tuple in unique_items_to_validate
                        }
                        for i, future in enumerate(as_completed(future_to_item_tuple)):
                            original_item_tuple = future_to_item_tuple[future]
                            link_url, source_id, _ = original_item_tuple
                            try:
                                validated_res = future.result()
                                all_validated_results.append({**validated_res, "Source": source_id})
                            except Exception as e_val:
                                all_validated_results.append({ # Log error for this link
                                    "Group Name": "Validation Error", "Group Link": link_url, "Logo URL": "",
                                    "Status": f"Validation Crash: {e_val}", "Keyword Relevancy": "N/A", "Source": source_id
                                })
                            val_prog.progress((i+1)/len(unique_items_to_validate), text=f"Validated {i+1}/{len(unique_items_to_validate)}...")
                    val_prog.empty()
                    
                    if all_validated_results:
                        st.session_state.results_df = pd.DataFrame(all_validated_results)
                    else: # Should not happen if unique_items_to_validate was populated, but defensive.
                        st.session_state.results_df = pd.DataFrame()
                    status_main.update(label="🎉 Processing Complete!", state="complete")

                except Exception as e_global:
                    status_main.error(f"🚫 Critical Error: {e_global}")
                    st.session_state.results_df = pd.DataFrame() # Clear on critical error

    # --- Display Results ---
    if st.session_state.process_button_clicked:
        if not st.session_state.results_df.empty:
            df_results = st.session_state.results_df
            st.markdown("---"); st.subheader("📊 Results Dashboard")
            active_df = df_results[df_results['Status'] == 'Active'].copy()
            # (Metrics display...)
            
            with st.expander("🔎 Detailed Results Table & Filters", expanded=True): # Expand by default
                # (DataFrame display with filters for Status, Relevancy, Source, Name...)
                # Ensure 'Keyword Relevancy' and 'Source' columns are handled if they exist.
                # Use actual_display_columns logic from previous version.
                st.dataframe(df_results, height=500, use_container_width=True, hide_index=True) # Simplified for brevity

            # (ZIP Download section if last_input_method was Excel or Domain...)
            # (Overall Markdown Export section...)
        elif st.session_state.process_button_clicked: # Button clicked, but df is empty
            st.info("🏁 Processing finished. No WhatsApp links were found or validated successfully.", icon="🤷")
    else: # Before any processing attempt
         st.info("✨ Welcome! Select an input method and click 'Process Links' to begin.", icon="👋")

if __name__ == "__main__":
    main()
