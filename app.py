import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from googlesearch import search

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
    u"\U0001F600-\U0001F64F"
    u"\U0001F300-\U0001F5FF"
    u"\U0001F680-\U0001F6FF"
    u"\U0001F1E0-\U0001F1FF"
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    u"\U0001F900-\U0001F9FF"
    u"\U0001FA70-\U0001FAFF"
    u"\U00002600-\U000026FF"
    u"\U00002700-\U000027BF"
    u"\U0001F700-\U0001F77F"
    u"\U0001F7E0-\U0001F7FF"
    u"\U0001F800-\U0001F8FF"
    u"\U0001F000-\U0001F0FF"
    u"\U0001F100-\U0001F1FF"
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
        response.encoding = 'utf-8'

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

def load_links(uploaded_file):
    """Load WhatsApp group links from an uploaded TXT or CSV file."""
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file).iloc[:, 0].astype(str).str.strip().dropna().unique().tolist()
    else:
        return [line.decode().strip() for line in uploaded_file.readlines() if line.strip()]

def google_search(query, top_n=5):
    """Fetch URLs from Google's top N search results using googlesearch-python."""
    try:
        urls = list(search(query, num_results=top_n, lang="en"))
        if not urls:
            st.warning(f"No search results found for the query '{query}'. Try refining your search terms.")
            return []
        return urls
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return []

def main():
    st.markdown('<h1 class="main-title">WhatsApp Group Validator 🚀</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Search, scrape, or validate WhatsApp group links with ease</p>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("⚙️ Settings")
        input_method = st.selectbox(
            "Input Method",
            ["Search and Scrape from Google", "Enter Links Manually", "Upload File (TXT/CSV)"],
            index=0
        )
        if input_method == "Search and Scrape from Google":
            top_n = st.slider("Number of top Google results to scrape from", min_value=1, max_value=10, value=5)

    if st.button("🗑️ Clear Results", use_container_width=True):
        if 'results' in st.session_state:
            del st.session_state['results']
        st.success("Results cleared successfully!")

    with st.container():
        results = []
        if input_method == "Search and Scrape from Google":
            keyword = st.text_input("Search Query:", placeholder="e.g., Islamic WhatsApp group")
            if st.button("Search, Scrape, and Validate", use_container_width=True):
                if not keyword:
                    st.warning("Please enter a search query.")
                    return
                with st.spinner("Searching Google..."):
                    search_results = list(search(keyword, num_results=top_n, lang="en"))
                if not search_results:
                    st.warning("No search results found. Try a different query.")
                    return
                st.success(f"Found {len(search_results)} webpages. Scraping WhatsApp links...")
                all_links = []
                for idx, url in enumerate(search_results):
                    links = scrape_whatsapp_links(url)
                    all_links.extend(links)
                all_links = list(set(all_links))
                if not all_links:
                    st.warning("No WhatsApp group links found in the scraped webpages.")
                    return
                st.success(f"Scraped {len(all_links)} unique WhatsApp group links. Validating...")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in all_links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)
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
                links = load_links(uploaded_file)
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
        expired_df = df[df['Status'] == 'Expired']

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
            st.metric("Expired Links", len(expired_df))
            st.markdown('</div>', unsafe_allow_html=True)

        with st.expander("🔎 View and Filter Results", expanded=True):
            status_filter = st.multiselect("Filter by Status", options=df['Status'].unique(), default=["Active"])
            filtered_df = df[df['Status'].isin(status_filter)] if status_filter else df

            display_df = filtered_df.copy()
            display_df['Invite Link'] = display_df['Group Link'].apply(lambda url: f"[Join Group]({url})")
            display_df = display_df[['Group Name', 'Invite Link', 'Logo URL', 'Status']]

            st.dataframe(
                display_df,
                column_config={
                    "Invite Link": st.column_config.Column("Invite Link"),
                    "Logo URL": st.column_config.ImageColumn("LOGO", width="small")
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
