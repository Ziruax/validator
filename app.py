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
import unicodedata

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="WhatsApp Link Scraper & Validator ProMax",
    page_icon="🌠",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.example.com/help', # Replace with actual URL
        'Report a bug': "https://www.example.com/bug", # Replace with actual URL
        'About': "# WhatsApp Link Scraper & Validator ProMax\nAdvanced features with enhanced stability and relevancy scoring."
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
    "group", "link", "links", "whatsapp", "wa", "chat", "join", "channel", "official", "community", "new", "free", "best", "video", "audio", "all", "only", "just"
])


# --- Custom CSS ---
st.markdown("""
    <style>
    .main-title { font-size: 2.6em; color: #075E54; text-align: center; margin-bottom: 0; font-weight: bold; letter-spacing: -1px;}
    .subtitle { font-size: 1.25em; color: #128C7E; text-align: center; margin-top: 5px; margin-bottom: 25px; }
    .stButton>button { background-color: #25D366; color: #FFFFFF; border-radius: 8px; font-weight: bold; border: none; padding: 10px 20px; transition: background-color 0.2s ease-in-out, transform 0.1s ease; }
    .stButton>button:hover { background-color: #1EBE5A; transform: translateY(-1px); }
    .stButton>button:active { transform: translateY(0px); }
    .stButton>button[kind="secondary"] { background-color: #e0e0e0; color: #333; }
    .stButton>button[kind="secondary"]:hover { background-color: #d0d0d0; }
    .stProgress > div > div > div > div { background-color: #25D366; }
    .metric-card { background-color: #FFFFFF; padding: 18px; border-radius: 12px; box-shadow: 0 6px 20px rgba(0,0,0,0.07); color: #333333; text-align: center; border: 1px solid #ECEFF1;}
    .stTextInput input, .stTextArea textarea, .stFileUploader section div[data-testid="stFileDropzone"] { border: 1px solid #128C7E !important; border-radius: 6px !important; }
    .sidebar .sidebar-content { background-color: #F0F2F5; padding-top: 1rem;}
    .stExpander { border: 1px solid #CFD8DC; border-radius: 10px; }
    .stExpander header { font-size: 1.1em; font-weight: 500; color: #075E54;}
    .markdown-output-area { background-color: #F8F9FA; padding: 18px; border-radius: 10px; border: 1px solid #DEE2E6; max-height: 550px; overflow-y: auto; font-family: 'Menlo', 'Consolas', monospace; white-space: pre-wrap; line-height: 1.65; box-shadow: inset 0 2px 4px rgba(0,0,0,0.03);}
    .markdown-output-area table { width: 100%; border-collapse: collapse; margin-bottom: 1.2em; background-color: #fff; }
    .markdown-output-area th, .markdown-output-area td { border: 1px solid #E0E0E0; padding: 10px 12px; text-align: left; vertical-align: middle;}
    .markdown-output-area th { background-color: #F1F3F5; font-weight: 600; color: #343A40;}
    .markdown-output-area img { max-width: 50px; max-height: 50px; border-radius: 50%; display: block; margin: auto; border: 1px solid #F0F0F0; }
    </style>
""", unsafe_allow_html=True)


# --- Helper Functions ---
def sanitize_filename(name, max_len=70):
    name = str(name); name = re.sub(r'https?://', '', name); name = re.sub(r'[^\w\s-]', '', name).strip().lower(); name = re.sub(r'[-\s]+', '-', name)
    sanitized = name[:max_len]; return sanitized if sanitized else f"unnamed-file-{int(time.time())}"

def is_primarily_english(text: str, threshold=0.75) -> bool:
    if not text or not isinstance(text, str): return True
    alphabetic_chars = [char for char in text if char.isalpha()]
    if not alphabetic_chars: return True
    latin_chars = 0
    for char_ord in [ord(char) for char in alphabetic_chars]:
        if (0x0041 <= char_ord <= 0x005A) or \
           (0x0061 <= char_ord <= 0x007A) or \
           (0x00C0 <= char_ord <= 0x00D6) or \
           (0x00D8 <= char_ord <= 0x00F6) or \
           (0x00F8 <= char_ord <= 0x017F): # Extended Latin-A
            latin_chars += 1
    return (latin_chars / len(alphabetic_chars)) >= threshold

def get_relevancy(group_name: str, keyword_phrase: str) -> str:
    if not group_name or not keyword_phrase: return "N/A (Missing Info)"
    if not is_primarily_english(group_name): return "Not Checked (Non-English Name)"
    
    proc_group_name = group_name.lower()
    proc_group_name_match = re.sub(r'[^a-z0-9\s]', '', proc_group_name)
    group_name_words = {word for word in proc_group_name_match.split() if word not in ENGLISH_STOP_WORDS and len(word) > 2}
    
    proc_keyword_phrase = keyword_phrase.lower()
    keyword_words = {word for word in proc_keyword_phrase.split() if len(word) > 2}

    if not group_name_words or not keyword_words: return "Low (Few Terms)"
    if proc_keyword_phrase in proc_group_name: return "High (Phrase Match)" # Check in less cleaned name
    
    common_words = keyword_words.intersection(group_name_words)
    if common_words == keyword_words and len(keyword_words)>0 : return "High (All Keywords)" # Ensure not empty set match
    
    # Check for significant overlap if not all keywords match
    if len(keyword_words) > 0 and len(common_words) / len(keyword_words) >= 0.5: return "Medium (Good Overlap)"
    if len(common_words) > 0: return "Medium (Partial Match)"
    return "Low (No Clear Match)"

# --- Core Logic Functions ---
@st.cache_data(ttl=1800, show_spinner="♻️ Validating link...")
def validate_link(link: str, associated_keyword: str = None) -> dict:
    result = {"Group Name": "Unknown", "Group Link": link, "Logo URL": "", "Status": "Error: Initializing", "Keyword Relevancy": "N/A"}
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
    if not link or not link.startswith(WHATSAPP_DOMAIN):
        result["Status"] = "Invalid Format (No WA Domain)"
        return result
    try:
        response = requests.get(link, headers=headers, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'
        if response.status_code != 200:
            result["Status"] = f"HTTP Error: {response.status_code}"
            return result
        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid: Redirected Off-Platform"
            if WHATSAPP_DOMAIN in link: # Original was a WA link
                 soup_check = BeautifulSoup(response.text, 'html.parser')
                 if soup_check.find(string=re.compile(r"(invite link revoked|couldn't join|link was reset|no longer valid|cannot be used)", re.IGNORECASE)):
                     result["Status"] = "Expired/Revoked (Redirected)"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        if soup.find(string=re.compile(r"(invite link revoked|link was reset|no longer valid|cannot be used)", re.IGNORECASE)):
            result["Status"] = "Expired/Revoked (Content)"
            meta_title_revoked = soup.find('meta', property='og:title')
            result["Group Name"] = (unescape(meta_title_revoked['content']).strip() if meta_title_revoked and meta_title_revoked.get('content') else "Revoked Group") or "Revoked Group (Name N/A)"
        else:
            meta_title = soup.find('meta', property='og:title')
            result["Group Name"] = (unescape(meta_title['content']).strip() if meta_title and meta_title.get('content') else "Unnamed Group") or "Unnamed Group"
            
            img_tags = soup.find_all('img', src=True)
            logo_found = False
            for img in img_tags:
                src = unescape(img['src'])
                if IMAGE_PATTERN.match(src):
                    result["Logo URL"] = src
                    result["Status"] = "Active"
                    logo_found = True
                    break
            if not logo_found and result["Status"] == "Error: Initializing": # Not set to Active
                if result["Group Name"] not in ["Unnamed Group", "Revoked Group (Name N/A)"]:
                    result["Status"] = "Expired (No Logo, Had Name)"
                else:
                    result["Status"] = "Invalid/Expired (No Logo/Name)"
        
    except requests.exceptions.Timeout: result["Status"] = "Network Error: Timeout"
    except requests.exceptions.ConnectionError: result["Status"] = "Network Error: Connection Failed"
    except requests.exceptions.RequestException as e: result["Status"] = f"Network Error ({type(e).__name__})"
    except Exception as e: result["Status"] = f"Unexpected Error ({type(e).__name__})"

    if result["Status"] == "Active" and result["Group Name"] not in ["Unknown", "Unnamed Group"] and associated_keyword:
        result["Keyword Relevancy"] = get_relevancy(result["Group Name"], associated_keyword)
    elif not associated_keyword:
        result["Keyword Relevancy"] = "N/A (No Keyword)"
    else: # Not active or name unknown for relevancy check
        result["Keyword Relevancy"] = "N/A (Not Active/Named)"
    return result

@st.cache_data(ttl=3600, show_spinner="⚙️ Parsing HTML...")
def scrape_whatsapp_links_from_html(html_content: str, source_url: str = "") -> list:
    soup = BeautifulSoup(html_content, 'html.parser'); links = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', ''); match = re.search(WHATSAPP_LINK_REGEX, href) if WHATSAPP_DOMAIN in href else None
        if match: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
    text_matches = re.finditer(WHATSAPP_LINK_REGEX, soup.get_text(separator=" "))
    for match in text_matches: links.add(f"{WHATSAPP_DOMAIN}{match.group(1)}")
    return list(links)

@st.cache_data(ttl=3600, show_spinner=False) # Spinner managed by st.status
def scrape_url_for_whatsapp_links(url: str, status_container=None) -> list:
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

@st.cache_data(ttl=1800, show_spinner=False) # Spinner managed by st.status
def google_search_links(query: str, num_results: int = 10, status_container=None) -> list:
    if status_container: status_container.write(f"🕵️ Google: '{query}' (top {num_results})...")
    try:
        urls = list(search(query, num_results=num_results, lang="en", pause=2.5, user_agent=DEFAULT_USER_AGENT)) # Increased pause slightly
        if not urls: msg = f"ℹ️ No Google results for '{query}'."; (status_container.info(msg) if status_container else st.caption(msg)); return []
        if status_container: status_container.write(f"✅ Found {len(urls)} pages via Google for '{query}'.")
        return urls
    except Exception as e:
        error_msg = f"🚫 Google Err ('{query}'): {type(e).__name__}."
        if "429" in str(e) or "rate limit" in str(e).lower(): error_msg += " Rate limit likely. Wait or reduce queries."
        (status_container.error(error_msg) if status_container else st.error(error_msg)); return []

def load_links_from_file(uploaded_file) -> list:
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
    keywords = []
    try:
        df_excel = pd.read_excel(uploaded_file, header=None, sheet_name=0)
        keywords = [kw for kw in df_excel.iloc[:, 0].dropna().astype(str).str.strip().tolist() if kw]
    except Exception as e: st.error(f"Excel read error '{uploaded_file.name}': {e}")
    return keywords

def crawl_website(base_url: str, max_pages_to_crawl: int = 10, status_container=None) -> list:
    pages_with_links_data = []; urls_to_visit = {base_url}; visited_urls = set(); processed_pages = 0
    parsed_base_url = urlparse(base_url)
    if not parsed_base_url.scheme: base_url = "http://" + base_url; parsed_base_url = urlparse(base_url)
    base_domain = parsed_base_url.netloc
    if not base_domain: (status_container.error(f"Invalid base URL for crawl: '{base_url}'") if status_container else None); return []
    
    if status_container: status_container.write(f"🕷️ Crawling '{base_domain}' (max {max_pages_to_crawl} pages)...")
    prog = st.progress(0.0, text="Initializing crawl...")
    
    while urls_to_visit and processed_pages < max_pages_to_crawl:
        current_url = urls_to_visit.pop()
        if current_url in visited_urls: continue
        visited_urls.add(current_url); processed_pages += 1
        prog.progress(processed_pages / max_pages_to_crawl, text=f"Crawl: {processed_pages}/{max_pages_to_crawl} - {current_url[:60]}...")
        
        try:
            page_links = scrape_url_for_whatsapp_links(current_url, None) # Suppress nested status for crawl_website itself
            if page_links: pages_with_links_data.append((current_url, page_links))
            
            # Logic to find more internal links on the current_url's page
            headers = {"User-Agent": DEFAULT_USER_AGENT}
            response_crawl = requests.get(current_url, headers=headers, timeout=10, allow_redirects=True)
            response_crawl.raise_for_status()
            soup_crawl = BeautifulSoup(response_crawl.text, 'html.parser')
            for a_tag in soup_crawl.find_all('a', href=True):
                link_href = a_tag['href']
                abs_link = urljoin(current_url, link_href)
                parsed_abs_link = urlparse(abs_link)
                if parsed_abs_link.scheme in ['http', 'https'] and parsed_abs_link.netloc == base_domain:
                    if abs_link not in visited_urls and abs_link not in urls_to_visit:
                        if len(urls_to_visit) < (max_pages_to_crawl * 2 + 20): # Queue limit
                            urls_to_visit.add(abs_link)
            time.sleep(0.2) # Polite delay
        except Exception as e_crawl_page:
             if status_container: status_container.caption(f"⚠️ Crawl skip {current_url[:50]}: {e_crawl_page}")
    prog.empty()
    if status_container: status_container.write(f"Crawling of '{base_domain}' finished. {len(pages_with_links_data)} pages yielded WhatsApp links.")
    return pages_with_links_data

def generate_markdown_output(active_df_for_source: pd.DataFrame) -> str:
    if active_df_for_source.empty: return "No active groups found for this source."
    md_lines = ["| Group Logo | Group Name | Action |", "| :--------: | :--------- | -----: |"]
    for _, row in active_df_for_source.iterrows():
        logo, name, link = row.get("Logo URL", ""), row.get("Group Name", "N/A"), row.get("Group Link", "#")
        safe_name = f"**{str(name).replace('|', '\|').replace('`', '\`').replace('*', '\*').replace('_', '\_').replace('\r\n', ' ').replace('\n', ' ')}**"
        logo_md = f"![Logo]({logo}&w=50)" if logo else " "
        action_md = f"[**Join Group**]({link})"
        md_lines.append(f"| {logo_md} | {safe_name} | {action_md} |")
    return "\n".join(md_lines)

# --- Streamlit UI ---
def main():
    st.markdown('<h1 class="main-title">WhatsApp Link Scraper & Validator ProMax 🌠</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Discover, validate, and organize WhatsApp group links with relevancy scoring.</p>', unsafe_allow_html=True)

    # Initialize session state robustly
    if 'results_df' not in st.session_state: st.session_state.results_df = pd.DataFrame()
    if 'process_button_clicked' not in st.session_state: st.session_state.process_button_clicked = False
    if 'last_input_method' not in st.session_state: st.session_state.last_input_method = None

    with st.sidebar:
        st.header("⚙️ Configuration")
        input_method_options = [
            "Search & Scrape: Google (Single Keyword)", "Search & Scrape: Google (Excel Keywords)",
            "Scrape: Specific Webpage(s)", "Scrape: Entire Website (Domain)",
            "Validate: Manual Link Entry", "Validate: Upload File (TXT/CSV)"
        ]
        input_method = st.selectbox("Input Method:", input_method_options, index=0, key="sb_input_method_vfinal", help="Select your link source.")
        
        top_n_google, max_crawl_pages, max_workers_validation = 5, 5, 4 # Conservative defaults
        if "Google" in input_method: top_n_google = st.slider("Google Results/Query:", 1, 20, 5, key="sb_google_num_vfinal", help="Pages per keyword from Google.")
        elif "Entire Website" in input_method: max_crawl_pages = st.slider("Max Pages to Crawl:", 3, 20, 5, key="sb_crawl_num_vfinal", help="Max internal pages for domain crawl.")
        max_workers_validation = st.slider("Validation Workers:", 1, 8, 4, key="sb_workers_num_vfinal", help="Concurrent link validations.")
        st.caption("Tip: Start with lower limits for Google/crawl to test quickly.")

    if st.sidebar.button("🗑️ Clear Results & Cache", use_container_width=True, type="secondary", key="sb_clear_cache_vfinal", help="Resets all results and clears app cache."):
        st.cache_data.clear()
        keys_to_clear = [k for k in st.session_state.keys()] # Avoid modifying dict during iteration
        for key in keys_to_clear: del st.session_state[key]
        st.success("✅ All results and cache cleared! Re-run or refresh if needed."); st.experimental_rerun()

    st.markdown("---") # Main page separator
    
    input_area = st.container()
    keyword_gs, excel_file_gs, urls_text_specific, domain_url_crawl, links_text_manual, file_links_upload = "", None, "", "", "", None

    with input_area:
        # Input fields with unique keys
        if input_method == "Search & Scrape: Google (Single Keyword)": keyword_gs = st.text_input("Google Search Query:", key="in_gs_keyword_vfinal", placeholder="e.g., AI research groups")
        elif input_method == "Search & Scrape: Google (Excel Keywords)": excel_file_gs = st.file_uploader("Upload Excel (keywords in 1st col):", type=["xlsx", "xls"], key="in_gs_excel_vfinal")
        elif input_method == "Scrape: Specific Webpage(s)": urls_text_specific = st.text_area("Webpage URLs (one per line):", height=120, key="in_scrape_specific_vfinal", placeholder="https://site.com/links\nhttps://blog.com/groups")
        elif input_method == "Scrape: Entire Website (Domain)": domain_url_crawl = st.text_input("Base Domain URL to Crawl:", key="in_scrape_domain_vfinal", placeholder="https://mycommunity.org")
        elif input_method == "Validate: Manual Link Entry": links_text_manual = st.text_area("WhatsApp Links (one per line):", height=150, key="in_manual_links_vfinal", placeholder=f"{WHATSAPP_DOMAIN}XYZ123...")
        elif input_method == "Validate: Upload File (TXT/CSV)": file_links_upload = st.file_uploader("Upload TXT/CSV with Links:", type=["txt", "csv"], key="in_upload_file_vfinal")

        action_label = "🚀 Process Links" # Default
        # Dynamically update action_label based on input (abbreviated for clarity)
        if keyword_gs: action_label = f"🔍 Google '{keyword_gs[:15]}...'"
        elif excel_file_gs: action_label = f"📊 Process '{excel_file_gs.name}'"
        # ... Add similar for other inputs

        if st.button(action_label, use_container_width=True, type="primary", key="btn_process_main_vfinal"):
            st.session_state.process_button_clicked = True
            st.session_state.last_input_method = input_method
            all_validated_results = [] 

            with st.status(f"🚀 Processing via: {input_method}", expanded=True) as status_main:
                try:
                    links_to_validate_with_source_keyword = [] # List of (link_url, source_id, relevancy_keyword)
                    
                    # --- Link Collection Phase ---
                    if input_method == "Search & Scrape: Google (Single Keyword)":
                        if not keyword_gs: status_main.update(label="⚠️ Enter a search query.", state="error"); return
                        status_main.write(f"Searching Google for '{keyword_gs}'...")
                        google_urls = google_search_links(keyword_gs, top_n_google, status_main)
                        for g_url in google_urls:
                            scraped_links = scrape_url_for_whatsapp_links(g_url, status_main)
                            for link in scraped_links: links_to_validate_with_source_keyword.append((link, keyword_gs, keyword_gs))
                    
                    elif input_method == "Search & Scrape: Google (Excel Keywords)":
                        if not excel_file_gs: status_main.update(label="⚠️ Upload Excel file.", state="error"); return
                        keywords = load_keywords_from_excel(excel_file_gs)
                        if not keywords: status_main.update(label="⚠️ No keywords in Excel.", state="error"); return
                        kw_prog = st.progress(0.0, text=f"Processing 0/{len(keywords)} keywords...")
                        for i, kw in enumerate(keywords):
                            status_main.write(f"Keyword {i+1}/{len(keywords)}: '{kw}'")
                            g_urls = google_search_links(kw, top_n_google, status_main)
                            for g_url in g_urls:
                                s_links = scrape_url_for_whatsapp_links(g_url, status_main)
                                for sl in s_links: links_to_validate_with_source_keyword.append((sl, kw, kw))
                            kw_prog.progress((i+1)/len(keywords), text=f"Processed {i+1}/{len(keywords)} keywords...")
                        kw_prog.empty()
                    
                    elif input_method == "Scrape: Entire Website (Domain)":
                        if not domain_url_crawl: status_main.update(label="⚠️ Enter domain URL.", state="error"); return
                        crawled_data = crawl_website(domain_url_crawl, max_crawl_pages, status_main)
                        for page_url, links_on_page in crawled_data:
                            for link in links_on_page: links_to_validate_with_source_keyword.append((link, page_url, None))
                    
                    elif input_method == "Scrape: Specific Webpage(s)":
                        if not urls_text_specific: status_main.update(label="⚠️ Enter webpage URLs.", state="error"); return
                        specific_urls = [u.strip() for u in urls_text_specific.split('\n') if u.strip()]
                        for i, s_url in enumerate(specific_urls):
                            status_main.write(f"Scraping page {i+1}/{len(specific_urls)}: {s_url[:60]}...")
                            s_links = scrape_url_for_whatsapp_links(s_url, status_main)
                            for sl in s_links: links_to_validate_with_source_keyword.append((sl, s_url, None))
                    
                    elif "Manual Entry" in input_method or "Upload File" in input_method:
                        raw_links_input = []
                        source_name_input = "Manual Entry"
                        if "Manual Entry" in input_method:
                            if not links_text_manual: status_main.update(label="⚠️ Enter links manually.", state="error"); return
                            raw_links_input = [l.strip() for l in links_text_manual.split('\n') if l.strip()]
                        elif "Upload File" in input_method:
                            if not file_links_upload: status_main.update(label="⚠️ Upload a file.", state="error"); return
                            raw_links_input = load_links_from_file(file_links_upload)
                            source_name_input = f"File: {file_links_upload.name}"
                        
                        for link_input in raw_links_input:
                            if WHATSAPP_DOMAIN in link_input and re.search(WHATSAPP_LINK_REGEX, link_input): # Basic pre-filter
                                links_to_validate_with_source_keyword.append((link_input, source_name_input, None))
                    
                    # Deduplication before validation
                    unique_links_map = {item[0]: item for item in reversed(links_to_validate_with_source_keyword)}
                    items_to_validate_final = list(unique_links_map.values())

                    if not items_to_validate_final:
                        status_main.info("ℹ️ No WhatsApp links found from the input to validate.");
                        st.session_state.results_df = pd.DataFrame(); status_main.update(label="🏁 No links found.", state="complete"); return

                    # --- Validation Phase ---
                    status_main.update(label=f"🛡️ Validating {len(items_to_validate_final)} unique links...", state="running")
                    val_prog = st.progress(0.0, text=f"Validating 0/{len(items_to_validate_final)}...")
                    
                    with ThreadPoolExecutor(max_workers=max_workers_validation) as executor:
                        future_to_item_tuple = {
                            executor.submit(validate_link, item_tuple[0], item_tuple[2]): item_tuple
                            for item_tuple in items_to_validate_final # item_tuple is (link, source, relevancy_keyword)
                        }
                        for i, future in enumerate(as_completed(future_to_item_tuple)):
                            original_item_tuple = future_to_item_tuple[future]
                            link_url, source_id, _ = original_item_tuple # Unpack original item
                            try:
                                validated_res = future.result()
                                all_validated_results.append({**validated_res, "Source": source_id})
                            except Exception as e_val_thread:
                                all_validated_results.append({
                                    "Group Name": "Validation Error", "Group Link": link_url, "Logo URL": "",
                                    "Status": f"Validation Crash: {e_val_thread}", "Keyword Relevancy": "N/A", "Source": source_id
                                })
                            val_prog.progress((i+1)/len(items_to_validate_final), text=f"Validated {i+1}/{len(items_to_validate_final)}...")
                    val_prog.empty()
                    
                    st.session_state.results_df = pd.DataFrame(all_validated_results) if all_validated_results else pd.DataFrame()
                    status_main.update(label="🎉 Processing Complete! View results below.", state="complete")

                except Exception as e_global_process:
                    status_main.error(f"🚫 A critical error occurred during processing: {e_global_process}")
                    st.session_state.results_df = pd.DataFrame() # Clear results on major failure

    # --- Display Results ---
    if st.session_state.process_button_clicked:
        if not st.session_state.results_df.empty:
            df_results = st.session_state.results_df
            st.markdown("---"); st.subheader("📊 Results Dashboard")
            active_df_overall = df_results[df_results['Status'] == 'Active'].copy()
            expired_df_overall = df_results[df_results['Status'].str.contains("Expired|Revoked", case=False, na=False)].copy()
            error_df_overall = df_results[~df_results.index.isin(active_df_overall.index) & ~df_results.index.isin(expired_df_overall.index)].copy()

            col1,col2,col3,col4 = st.columns(4)
            with col1: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="Total Links Processed", value=len(df_results)); st.markdown('</div>', unsafe_allow_html=True)
            with col2: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="✅ Active Groups", value=len(active_df_overall)); st.markdown('</div>', unsafe_allow_html=True)
            with col3: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="⚠️ Expired/Revoked", value=len(expired_df_overall)); st.markdown('</div>', unsafe_allow_html=True)
            with col4: st.markdown('<div class="metric-card">', unsafe_allow_html=True); st.metric(label="❌ Errors/Invalid", value=len(error_df_overall)); st.markdown('</div>', unsafe_allow_html=True)

            with st.expander("🔎 Detailed Results Table & Filters", expanded=True):
                # Ensure necessary columns exist, provide defaults if not
                if "Keyword Relevancy" not in df_results.columns: df_results["Keyword Relevancy"] = "N/A"
                if "Source" not in df_results.columns: df_results["Source"] = "Unknown"
                
                display_columns_order = ["Group Name", "Group Link", "Status", "Keyword Relevancy", "Source", "Logo URL"]
                actual_display_columns = [col for col in display_columns_order if col in df_results.columns]

                # Filters
                status_options = ["All"] + sorted(df_results['Status'].dropna().unique().tolist())
                sel_status = st.selectbox("Filter by Status:", status_options, key="filter_status_vfinal")
                
                relevancy_options = ["All"] + sorted(df_results["Keyword Relevancy"].dropna().unique().tolist())
                sel_relevancy = st.selectbox("Filter by Relevancy:", relevancy_options, key="filter_relevancy_vfinal")

                filter_name = st.text_input("Filter by Group Name (contains):", key="filter_name_vfinal")
                filter_source = st.text_input("Filter by Source (contains):", key="filter_source_vfinal")

                filtered_display_df = df_results.copy()
                if sel_status != "All": filtered_display_df = filtered_display_df[filtered_display_df['Status'] == sel_status]
                if sel_relevancy != "All": filtered_display_df = filtered_display_df[filtered_display_df['Keyword Relevancy'] == sel_relevancy]
                if filter_name: filtered_display_df = filtered_display_df[filtered_display_df['Group Name'].str.contains(filter_name, case=False, na=False)]
                if filter_source: filtered_display_df = filtered_display_df[filtered_display_df['Source'].str.contains(filter_source, case=False, na=False)]

                st.dataframe(
                    filtered_display_df[actual_display_columns],
                    column_config={
                        "Group Name": st.column_config.TextColumn("Group Name", width="medium"),
                        "Group Link": st.column_config.LinkColumn("Invite Link", display_text="🔗 Join", width="medium"),
                        "Status": st.column_config.TextColumn("Validation", width="small"),
                        "Keyword Relevancy": st.column_config.TextColumn("Relevancy", width="small", help="Relevancy to search keyword."),
                        "Source": st.column_config.TextColumn("Source Origin", width="medium"),
                        "Logo URL": st.column_config.ImageColumn("Logo", width="small")
                    },
                    height=500, use_container_width=True, hide_index=True
                )
                csv_filtered = filtered_display_df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Filtered Table (CSV)", csv_filtered, "filtered_whatsapp_groups.csv", "text/csv", key="dl_filtered_csv_vfinal", use_container_width=True)

            # --- Per-Source File Downloads (ZIP) ---
            last_method_run = st.session_state.get('last_input_method', None)
            if last_method_run in ["Search & Scrape: Google (Excel Keywords)", "Scrape: Entire Website (Domain)"]:
                if not active_df_overall.empty and 'Source' in active_df_overall.columns:
                    st.markdown("---"); st.subheader("🗂️ Download Source-Specific Active Groups (ZIP)")
                    zip_buffer = io.BytesIO()
                    try:
                        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED, False) as zip_file:
                            summary_md = generate_markdown_output(active_df_overall[["Logo URL", "Group Name", "Group Link"]])
                            zip_file.writestr("_SUMMARY_all_active.md", summary_md.encode('utf-8'))
                            zip_file.writestr("_SUMMARY_all_active.csv", active_df_overall.to_csv(index=False).encode('utf-8'))

                            for source_name in active_df_overall['Source'].dropna().unique():
                                source_df = active_df_overall[active_df_overall['Source'] == source_name]
                                if source_df.empty: continue
                                s_filename = sanitize_filename(f"active_{source_name}")
                                zip_file.writestr(f"{s_filename}.md", generate_markdown_output(source_df).encode('utf-8'))
                                zip_file.writestr(f"{s_filename}.csv", source_df.to_csv(index=False).encode('utf-8'))
                        
                        zip_buffer.seek(0)
                        st.download_button("📥 Download Source Files & Summaries (ZIP)", zip_buffer, "whatsapp_groups_by_source.zip", "application/zip", use_container_width=True, key="dl_zip_vfinal")
                    except Exception as e_zip_create: st.error(f"Error creating ZIP: {e_zip_create}")
                else: st.caption("ℹ️ ZIP download available for Excel or Domain crawl results with active groups.")

            # --- Overall Markdown Export ---
            if not active_df_overall.empty:
                st.markdown("---"); st.subheader("📝 Overall Markdown Export (All Active Groups)")
                overall_md = generate_markdown_output(active_df_overall[["Logo URL", "Group Name", "Group Link"]])
                st.markdown("##### Preview (Markdown Table for WordPress):")
                st.markdown(f'<div class="markdown-output-area">{overall_md}</div>', unsafe_allow_html=True)
                st.download_button("📥 Download Overall Markdown (.md)", overall_md.encode('utf-8'), "overall_active_groups.md", "text/markdown", use_container_width=True, key="dl_overall_md_vfinal")
                st.text_area("Copy Overall Markdown:", value=overall_md, height=250, key="copy_overall_md_vfinal", help="Manually copy this Markdown for your posts.")
            
        elif st.session_state.process_button_clicked: # Button clicked, but df is empty
            st.info("🏁 Processing finished. No WhatsApp links were found or validated successfully.", icon="🤷")
    else: # Before any processing attempt
         st.info("✨ Welcome! Select an input method and click 'Process Links' to begin your search.", icon="👋")

if __name__ == "__main__":
    main()
