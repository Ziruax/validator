import streamlit as st
import pandas as pd
import requests
from html import unescape
from bs4 import BeautifulSoup
import re
import time
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

# Custom CSS (same as before)
# ... [keep existing CSS] ...

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
            # Removed emoji removal
            result["Group Name"] = group_name if group_name else "Unnamed Group"
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

def load_links(uploaded_file):
    """Load WhatsApp group links from an uploaded TXT or CSV file."""
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file).iloc[:, 0].tolist()
    else:
        return [line.decode().strip() for line in uploaded_file.readlines()]

def generate_markdown_export(filtered_df):
    """Generate custom markdown format with image resizing"""
    markdown_output = ""
    
    for _, row in filtered_df.iterrows():
        group_block = f"""
![]({row['Logo URL']})

**{row['Group Name']}**

[**Join**]({row['Group Link']})

---
"""
        markdown_output += group_block
        
    return markdown_output.strip()

def generate_html_export(filtered_df):
    """Generate basic HTML page"""
    html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>WhatsApp Group Directory</title>
    <style>
        body {{ font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #25D366; }}
        .group {{ border-bottom: 1px solid #eee; padding: 20px 0; }}
        img {{ max-width: 100%; height: auto; }}
        a {{ color: #25D366; text-decoration: none; font-weight: bold; }}
    </style>
</head>
<body>
    <h1>WhatsApp Group Directory</h1>
    
    {content}
</body>
</html>
"""

    content = ""
    for _, row in filtered_df.iterrows():
        content += f"""
<div class="group">
    <h3>{row['Group Name']}</h3>
    <img src="{row['Logo URL']}" alt="{row['Group Name']}">
    <p><a href="{row['Group Link']}" target="_blank">Join WhatsApp Group</a></p>
</div>
"""

    return html_template.format(content=content)

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
                    search_results = google_search(keyword, top_n=top_n)
                if not search_results:
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
                    st.warning("No WhatsApp group links found in the scraped webpages.")
                    return
                st.success(f"Scraped {len(unique_links)} unique WhatsApp group links. Validating...")
                progress_bar = st.progress(0)
                status_text = st.empty()
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in unique_links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)
                        progress_bar.progress((i + 1) / len(unique_links))
                        status_text.text(f"Validated {i + 1}/{len(unique_links)} links")

        elif input_method == "Enter Links Manually":
            links_text = st.text_area("Enter WhatsApp Links (one per line):", height=200, placeholder="e.g., https://chat.whatsapp.com/ABC123")
            if st.button("Validate Links", use_container_width=True):
                links = [line.strip() for line in links_text.split('\n') if line.strip()]
                if not links:
                    st.warning("Please enter at least one link.")
                    return
                progress_bar = st.progress(0)
                status_text = st.empty()
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)
                        progress_bar.progress((i + 1) / len(links))
                        status_text.text(f"Validated {i + 1}/{len(links)} links")

        elif input_method == "Upload File (TXT/CSV)":
            uploaded_file = st.file_uploader("Upload TXT or CSV", type=["txt", "csv"])
            if uploaded_file and st.button("Validate File Links", use_container_width=True):
                links = load_links(uploaded_file)
                if not links:
                    st.warning("No links found in the uploaded file.")
                    return
                progress_bar = st.progress(0)
                status_text = st.empty()
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_link = {executor.submit(validate_link, link): link for link in links}
                    for i, future in enumerate(as_completed(future_to_link)):
                        result = future.result()
                        results.append(result)
                        progress_bar.progress((i + 1) / len(links))
                        status_text.text(f"Validated {i + 1}/{len(links)} links")

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
            display_df['Invite Link'] = display_df['Group Link'].apply(lambda url: f"[Join]({url})")
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
        with st.expander("📄 Export to Markdown Format", expanded=True):
            st.markdown("Copy-paste the output below into a WordPress post or Markdown editor:")
            markdown_output = generate_markdown_export(filtered_df)
            st.code(markdown_output, language="markdown")
            st.download_button(
                label="📥 Download Markdown File (.md)",
                data=markdown_output,
                file_name="whatsapp_groups.md",
                mime="text/markdown",
                use_container_width=True
            )

        # HTML Export
        with st.expander("📄 Export to HTML Format", expanded=True):
            st.markdown("Download a complete HTML page:")
            html_output = generate_html_export(filtered_df)
            st.code(html_output, language="html")
            st.download_button(
                label="📥 Download HTML File (.html)",
                data=html_output,
                file_name="whatsapp_groups.html",
                mime="text/html",
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
