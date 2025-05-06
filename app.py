import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search
import os

# Streamlit Configuration
st.set_page_config(
    page_title="WhatsApp Link Validator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
WHATSAPP_DOMAIN = "https://chat.whatsapp.com/"
IMAGE_PATTERN = re.compile(r'https:\/\/pps\.whatsapp\.net\/.*\.jpg\?[^&]*&[^&]+')
EMOJI_PATTERN = re.compile(
    "["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags
    u"\U00002702-\U000027B0"  # dingbats
    u"\U000024C2-\U0001F251"  # enclosed characters
    u"\U0001F900-\U0001F9FF"  # supplemental symbols
    u"\U0001FA70-\U0001FAFF"  # symbols and pictographs extended
    u"\U00002600-\U000026FF"  # miscellaneous symbols
    u"\U00002700-\U000027BF"  # dingbats
    u"\U0001F700-\U0001F77F"  # alchemical symbols
    u"\U0001F7E0-\U0001F7FF"  # geometric shapes extended
    u"\U0001F800-\U0001F8FF"  # supplemental arrows
    u"\U0001F000-\U0001F0FF"  # mahjong tiles
    u"\U0001F100-\U0001F1FF"  # enclosed alphanumeric supplement
    "]+",
    flags=re.UNICODE
)

# Custom CSS for enhanced UI
st.markdown("""
    <style>
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
    .stTextInput, .stTextArea {
        border: 1px solid #25D366;
        border-radius: 5px;
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

def validate_link(link):
    """Validate a WhatsApp group link and return details if active."""
    result = {
        "Group Name": "Unknown",
        "Group Link": link,
        "Logo URL": "",
        "Status": "Error"
    }
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
        response = requests.get(link, headers=headers, timeout=10, allow_redirects=True)
        response.encoding = 'utf-8'  # Force UTF-8 to preserve Urdu and emojis

        if response.status_code != 200:
            result["Status"] = f"HTTP Error {response.status_code}"
            return result

        if WHATSAPP_DOMAIN not in response.url:
            result["Status"] = "Invalid Link"
            return result

        soup = BeautifulSoup(response.text, 'html.parser')
        meta_title = soup.find('meta', property='og:title')

        if meta_title and meta_title.get('content'):
            group_name = unescape(meta_title['content']).strip()
            # Optionally remove emojis (commented out to preserve them)
            # group_name = EMOJI_PATTERN.sub('', group_name)
            result["Group Name"] = group_name or "Unnamed Group"
        else:
            result["Group Name"] = "Unnamed Group"

        img_tags = soup.find_all('img', src=True)
        for img in img_tags:
            src = unescape(img['src'])
            if IMAGE_PATTERN.match(src):
                result["Logo URL"] = src
                result["Status"] = "Active"
                break
        else:
            result["Status"] = "Expired"

    except requests.exceptions.RequestException as e:
        result["Status"] = f"Network Error: {str(e)}"
    except Exception as e:
        result["Status"] = f"Error: {str(e)}"
    
    return result

def scrape_whatsapp_links(url):
    """Scrape WhatsApp group links from a webpage."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if a['href'].startswith(WHATSAPP_DOMAIN):
                links.append(a['href'])
        for text in soup.stripped_strings:
            if WHATSAPP_DOMAIN in text:
                found_links = re.findall(r'https?://chat\.whatsapp\.com/[^\s]+', text)
                links.extend(found_links)
        return list(set(links))
    except Exception:
        return []

def scrape_website(domain):
    """Crawl entire website for WhatsApp links"""
    visited = set()
    queue = [domain]
    all_links = []
    max_pages = 100  # Safety limit
    
    while queue and len(all_links) < 1000 and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited or not url.startswith(domain):
            continue
            
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract links
                for a in soup.find_all('a', href=True):
                    link = a['href']
                    if link.startswith(WHATSAPP_DOMAIN):
                        all_links.append(link)
                    elif link.startswith(domain) and '?' not in link:
                        queue.append(link)
                
                visited.add(url)
                time.sleep(1)  # Rate limiting
        except:
            continue
            
    return list(set(all_links))

def google_search_with_limit(query, pages=10):
    """Search Google with custom page limit"""
    results = []
    for i in range(pages):
        try:
            urls = list(search(query, num_results=10, lang="en", sleep=2))
            results.extend(urls)
            time.sleep(1)
        except Exception as e:
            st.warning(f"Search error on page {i+1}: {str(e)}")
            continue
    return list(set(results))

def process_bulk_keywords(file):
    """Process keywords from CSV/Excel file"""
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
        keywords_list = df.iloc[:, 0].dropna().str.strip().tolist()
        return list(set(keywords_list))
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
        return []

def is_relevant(group_name, keywords):
    """Check if group name contains any of the keywords"""
    if not keywords:
        return True
        
    group_name = group_name.lower()
    for keyword in keywords:
        keyword = keyword.lower()
        if keyword in group_name:
            return True
    return False

def main():
    st.markdown('<h1 class="main-title">WhatsApp Group Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Search, scrape, or validate WhatsApp group links with advanced filtering</p>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Settings")
        input_method = st.selectbox(
            "Input Method",
            ["Search and Scrape from Google", "Scrape Specific Website", "Bulk Keyword Processing", "Enter Links Manually", "Upload File (TXT/CSV)"],
            index=0
        )
        
        if input_method == "Search and Scrape from Google":
            keyword = st.text_input("Search Keyword")
            pages = st.slider("Number of pages to scrape", 1, 100, 10)
            relevance_filter = st.checkbox("Enable relevance filter", value=True)
            keywords_for_filter = []
            if relevance_filter:
                keywords_for_filter = [k.strip() for k in st.text_area("Additional keywords (one per line)", height=100).split('\n') if k.strip()]
        
        elif input_method == "Scrape Specific Website":
            domain = st.text_input("Website Domain (e.g., https://example.com)")
        
        elif input_method == "Bulk Keyword Processing":
            keyword_file = st.file_uploader("Upload CSV/Excel with keywords", type=["csv", "xlsx"])

    if st.button("🗑️ Clear Results", use_container_width=True):
        if 'results' in st.session_state:
            del st.session_state['results']
        st.success("Results cleared successfully!")

    with st.container():
        results = []
        if input_method == "Search and Scrape from Google":
            if st.button("Search & Validate", use_container_width=True):
                if not keyword:
                    st.warning("Please enter a search keyword.")
                    return
                    
                with st.spinner("Searching Google..."):
                    search_results = google_search_with_limit(keyword, pages)
                    
                if not search_results:
                    st.warning("No search results found.")
                    return
                    
                st.success(f"Found {len(search_results)} webpages. Scraping WhatsApp links...")
                all_links = []
                progress_bar = st.progress(0)
                
                for idx, url in enumerate(search_results):
                    links = scrape_whatsapp_links(url)
                    all_links.extend(links)
                    progress_bar.progress((idx + 1) / len(search_results))
                    
                unique_links = list(set(all_links))
                
                if not unique_links:
                    st.warning("No WhatsApp group links found.")
                    return
                    
                st.success(f"Scraped {len(unique_links)} unique WhatsApp group links. Validating...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in unique_links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        if not relevance_filter or is_relevant(result["Group Name"], keywords_for_filter or [keyword]):
                            results.append(result)
                        progress_bar.progress((i + 1) / len(unique_links))
                        status_text.text(f"Validated {i + 1}/{len(unique_links)} links")
        
        elif input_method == "Scrape Specific Website":
            if st.button("Scrape Website", use_container_width=True):
                if not domain:
                    st.warning("Please enter a domain URL.")
                    return
                    
                with st.spinner("Crawling website..."):
                    links = scrape_website(domain)
                    
                if not links:
                    st.warning("No WhatsApp links found on this website.")
                    return
                    
                st.success(f"Found {len(links)} WhatsApp links. Validating...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)
                        progress_bar.progress((i + 1) / len(links))
                        status_text.text(f"Validated {i + 1}/{len(links)} links")
        
        elif input_method == "Bulk Keyword Processing":
            if keyword_file:
                with st.spinner("Processing keywords..."):
                    keywords = process_bulk_keywords(keyword_file)
                    st.info(f"Found {len(keywords)} unique keywords")
                    
                if st.button("Process All Keywords", use_container_width=True):
                    all_results = []
                    progress_bar = st.progress(0)
                    
                    for idx, keyword in enumerate(keywords):
                        with st.spinner(f"Processing keyword: {keyword} ({idx+1}/{len(keywords)})"):
                            search_results = google_search_with_limit(keyword, 5)
                            all_links = []
                            
                            for url in search_results:
                                links = scrape_whatsapp_links(url)
                                all_links.extend(links)
                            
                            unique_links = list(set(all_links))
                            
                            with ThreadPoolExecutor(max_workers=3) as executor:
                                future_to_link = {executor.submit(validate_link, link): link for link in unique_links}
                                for future in as_completed(future_to_link):
                                    result = future.result()
                                    if is_relevant(result["Group Name"], [keyword]):
                                        result["Keyword"] = keyword
                                        all_results.append(result)
                        
                        progress_bar.progress((idx + 1) / len(keywords))
                    
                    results = all_results
                    st.success("Bulk processing completed!")
        
        elif input_method == "Enter Links Manually":
            links_text = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123")
            if st.button("Validate Links", use_container_width=True):
                links = [line.strip() for line in links_text.split('\n') if line.strip()]
                if not links:
                    st.warning("Please enter at least one link.")
                    return
                links = list(set(links))
                st.info(f"Processing {len(links)} unique links.")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)
        
        elif input_method == "Upload File (TXT/CSV)":
            uploaded_file = st.file_uploader("Upload TXT or CSV", type=["txt", "csv"])
            if uploaded_file and st.button("Validate File Links", use_container_width=True):
                links = []
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                    links = df.iloc[:, 0].astype(str).str.strip().dropna().unique().tolist()
                else:
                    links = [line.decode().strip() for line in uploaded_file.readlines() if line.strip()]
                if not links:
                    st.warning("No links found in the uploaded file.")
                    return
                st.info(f"Found {len(links)} links. Removing duplicates...")
                links = list(set(links))
                st.success(f"Processing {len(links)} unique links.")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)

        if results:
            st.session_state['results'] = results

    if 'results' in st.session_state:
        df = pd.DataFrame(st.session_state['results']).drop_duplicates(subset=['Group Link'])
        active_df = df[df['Status'] == 'Active']
        
        st.subheader("📊 Results Summary")
        col1, col2, col3 = st.columns(3)
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
            st.metric("Unique Groups", df['Group Name'].nunique())
            st.markdown('</div>', unsafe_allow_html=True)

        with st.expander("🔎 View and Filter Results", expanded=True):
            status_filter = st.multiselect("Filter by Status", options=df['Status'].unique(), default=["Active"])
            filtered_df = df[df['Status'].isin(status_filter)] if status_filter else df
            
            # Display DataFrame
            display_df = filtered_df.copy()
            display_df['Invite Link'] = display_df['Group Link'].apply(lambda url: f"[Join Group]({url})")
            display_df = display_df[['Group Name', 'Invite Link', 'Status']]
            
            st.dataframe(
                display_df,
                column_config={
                    "Invite Link": st.column_config.Column("Invite Link"),
                },
                height=400,
                use_container_width=True
            )

        # Markdown Export
        with st.expander("📄 Export to Markdown Table", expanded=True):
            st.markdown("Copy-paste the table below into a WordPress post or Markdown editor:")
            markdown_df = filtered_df[['Group Name', 'Group Link']].copy()
            markdown_df['Group Link'] = markdown_df['Group Link'].apply(lambda url: f"[Join Group]({url})")
            markdown_table = markdown_df.to_markdown(index=False)
            st.code(markdown_table, language="markdown")
            
            st.download_button(
                label="📥 Download Markdown File (.md)",
                data=markdown_table,
                file_name="whatsapp_groups_table.md",
                mime="text/markdown",
                use_container_width=True
            )

        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            csv_active = active_df.to_csv(index=False)
            st.download_button("📥 Download Active Groups", csv_active, "active_groups.csv", "text/csv", use_container_width=True)
        with col_dl2:
            csv_all = df.to_csv(index=False)
            st.download_button("📥 Download All Results", csv_all, "all_groups.csv", "text/csv", use_container_width=True)
    else:
        st.info("Start by searching for WhatsApp group links, entering them manually, or uploading a file!", icon="ℹ️")

if __name__ == "__main__":
    main()
