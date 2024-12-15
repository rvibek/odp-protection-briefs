from requests_html import HTML, AsyncHTMLSession
from datetime import datetime
import asyncio
import json
from typing import List, Dict
import aiohttp
from urllib.parse import urljoin
import re
import atexit
import signal

# Create a single session to be used throughout
async def create_session():
    return AsyncHTMLSession()

async def extract_metadata_from_url(session: AsyncHTMLSession, url: str) -> Dict:
    try:
        # Fetch the page
        r = await session.get(url)
        await r.html.arender(timeout=30)
        
        # Initialize metadata dictionary
        metadata = {
            'url': url,
            'report_name': '',
            'sectors': [],
            'locations': [],
            'publish_date': '',
            'upload_date': '',
            'downloads': 0,
            'document_type': '',
            'document_language': '',
            'file_size': '',
            'population_groups': []
        }
        
        # Extract report name (using the visible title from larger screens)
        title_elem = r.html.find('h1.documentView_title.pageTitle.showFromMediumPlus', first=True)
        if title_elem:
            metadata['report_name'] = title_elem.text.strip()
        
        # Extract document type and language from the definition table
        def_tables = r.html.find('table.definitionTable tbody')
        for table in def_tables:
            rows = table.find('tr')
            for row in rows:
                header = row.find('th.definitionTable_title', first=True)
                if header:
                    header_text = header.text.strip().lower()
                    value = row.find('td.definitionTable_desc', first=True)
                    if value:
                        value_text = value.text.strip()
                        
                        if 'document type' in header_text:
                            metadata['document_type'] = value_text
                        elif 'document language' in header_text:
                            metadata['document_language'] = value_text
                        elif 'publish date' in header_text:
                            metadata['publish_date'] = value_text.split('(')[0].strip()
                        elif 'upload date' in header_text:
                            metadata['upload_date'] = value_text.split('(')[0].strip()
                        elif 'downloads' in header_text:
                            try:
                                # Remove commas before converting to int
                                clean_value = value_text.replace(',', '')
                                metadata['downloads'] = int(clean_value)
                            except ValueError:
                                pass
        
        # Extract sectors
        sectors_list = r.html.find('ul.documentView_sectorList li.inlineList_item')
        metadata['sectors'] = [s.text.strip() for s in sectors_list]
        
        # Extract locations
        locations_list = r.html.find('ul.documentView_locationList li.inlineList_item')
        metadata['locations'] = [l.text.strip() for l in locations_list]
        
        # Extract population groups
        pop_groups_table = r.html.find('table.documentView_popGroupTable tbody tr')
        for row in pop_groups_table:
            cells = row.find('td')
            if len(cells) >= 2:  # Make sure we have at least 2 cells
                group_text = cells[-1].text.strip()  # Get the last cell's text
                if group_text:
                    metadata['population_groups'].append(group_text)
        
        # Extract file size from download button
        download_btn = r.html.find('a.button.-cta.-tall.-fullWidth', first=True)
        if download_btn:
            size_match = re.search(r'\((.*?)\)', download_btn.text)
            if size_match:
                metadata['file_size'] = size_match.group(1).strip()
        
        return metadata
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return None

async def process_urls_concurrently(session: AsyncHTMLSession, urls: List[str], max_concurrent: int = 10):
    # Process URLs in chunks
    all_metadata = []
    for i in range(0, len(urls), max_concurrent):
        chunk = urls[i:i + max_concurrent]
        tasks = [extract_metadata_from_url(session, url) for url in chunk]
        chunk_results = await asyncio.gather(*tasks)
        all_metadata.extend([r for r in chunk_results if r is not None])
        print(f"Processed {len(all_metadata)} URLs so far...")
    
    return all_metadata

def extract_table_urls(html_content):
    # Create HTML object directly from content
    html = HTML(html=html_content)
    
    try:
        # Find all table elements
        tables = html.find('table')
        
        # List to store all found URLs
        document_urls = []
        
        # Iterate through each table
        for table in tables:
            # Find all links within the table
            links = table.absolute_links
            # Filter links that start with the specific URL pattern
            filtered_links = [link for link in links if link.startswith('https://data.unhcr.org/en/documents/details/')]
            document_urls.extend(filtered_links)
        
        return document_urls
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []

async def main():
    # Read the local HTML file
    with open('protection_briefs_doc2html.html', 'r') as f:
        html_content = f.read()
    
    urls = extract_table_urls(html_content)
    
    # Generate filename with today's date
    today = datetime.now().strftime('%Y%m%d')
    filename = f"{today}-odp-probriefs.txt"
    
    # Save URLs to file
    with open(filename, 'w') as f:
        for url in urls:
            f.write(f"{url}\n")
    
    print(f"Found {len(urls)} URLs and saved them to {filename}")
    
    # Process URLs and extract metadata
    print("\nExtracting metadata from URLs...")
    try:
        session = await create_session()
        metadata_list = await process_urls_concurrently(session, urls)
        
        # Save metadata to JSON file
        metadata_filename = f"{today}-odp-probriefs-metadata.json"
        with open(metadata_filename, 'w', encoding='utf-8') as f:
            json.dump(metadata_list, f, indent=2, ensure_ascii=False)
        
        print(f"\nExtracted metadata from {len(metadata_list)} URLs and saved to {metadata_filename}")
    except Exception as e:
        print(f"An error occurred during metadata extraction: {str(e)}")
    finally:
        if session:
            await session.close()

if __name__ == "__main__":
    asyncio.run(main())
