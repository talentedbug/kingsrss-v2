import os
import requests
import feedparser
import sqlite3
from bs4 import BeautifulSoup
from ebooklib import epub
from PIL import Image
from io import BytesIO
from datetime import datetime, date
import yaml
import secrets
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path

# Constants
DATABASE = 'records.db'
SRC_DIR = 'src'
COVER_DIR = 'cover'
EPUB_FILE = f'kingsrss_{date.today().strftime("%Y%m%d")}.epub'
CONFIG_FILE = 'srcconf.yml'

# Create necessary directories
os.makedirs(SRC_DIR, exist_ok=True)

def init_db():
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS articles
                     (url TEXT, title TEXT, published TEXT, UNIQUE(url, title))''')
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"[ERROR] Database initialization failed: {e}")

def is_article_processed(url, title):
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('SELECT 1 FROM articles WHERE url = ? AND title = ?', (url, title))
        result = c.fetchone()
        conn.close()
        return result is not None
    except sqlite3.Error as e:
        print(f"[ERROR] Database query failed: {e}")
        return False

def mark_article_processed(url, title):
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO articles (url, title, published) VALUES (?, ?, ?)', (url, title, str(datetime.now())))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"[ERROR] Database update failed: {e}")

def download_rss(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return feedparser.parse(response.content)
    except requests.RequestException as e:
        print(f"[ERROR] Error downloading RSS feed: {e}")
        return None

def save_html(content, filename):
    try:
        with open(os.path.join(SRC_DIR, filename), 'w', encoding='utf-8') as file:
            file.write(content)
    except OSError as e:
        print(f"[ERROR] Error saving HTML file {filename}: {e}")

def compress_image(image, max_size_mb=0.5):
    """Compress image to target size in MB"""
    img_byte_arr = BytesIO()
    quality = 95
    image.save(img_byte_arr, format='JPEG', quality=quality, optimize=True)
    
    # Gradually reduce quality until image is under max_size
    while img_byte_arr.tell() > max_size_mb * 1024 * 1024 and quality > 10:
        img_byte_arr = BytesIO()
        quality -= 5
        image.save(img_byte_arr, format='JPEG', quality=quality, optimize=True)
    
    return img_byte_arr.getvalue()

def download_and_embed_images(soup, book):
    """Download, compress, and embed images into the epub book"""
    for img in soup.find_all('img'):
        img_url = img.get('src')
        try:
            img_response = requests.get(img_url)
            img_response.raise_for_status()
            
            # Open and compress image
            image = Image.open(BytesIO(img_response.content))
            compressed_image = compress_image(image)
            
            img_id = ''.join(secrets.choice('0123456789abcdef') for _ in range(10))
            # Create unique filename for the image in epub
            img_filename = f'images/img_{img_id}.jpg'
            
            # Add image to epub book
            epub_image = epub.EpubItem(
                uid=f'image_{img_id}',
                file_name=img_filename,
                media_type='image/jpeg',
                content=compressed_image
            )
            book.add_item(epub_image)
            
            # Update image source in HTML to point to embedded image
            img['src'] = img_filename
            print(f"[INFO] Embedded compressed image {img_url} as {img_filename}")
            
        except requests.RequestException as e:
            print(f"[ERROR] Error downloading image: {e}")
            # Remove failed image from HTML
            img.decompose()
        except Exception as e:
            print(f"[ERROR] Error processing image: {e}")
            img.decompose()

def clean_src_directory():
    """Clean up the src directory after successful EPUB generation"""
    try:
        for file in os.listdir(SRC_DIR):
            file_path = os.path.join(SRC_DIR, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"[ERROR] Error deleting file {file_path}: {e}")
        print("[INFO] Successfully cleaned src directory")
    except Exception as e:
        print(f"[ERROR] Error cleaning src directory: {e}")

def get_random_file(directory):
    try:
        files = os.listdir(directory)
        if not files:
            raise ValueError(f"No files found in directory: {directory}")
        random_file = random.choice(files)
        full_path = os.path.join(directory, random_file)
        return full_path
    except OSError as e:
        print(f"[ERROR] Error accessing directory: {e}")
    except ValueError as e:
        print(f"[ERROR] {e}")

def load_existing_epub():
    """Load existing EPUB file if it exists, or create new one"""
    if os.path.exists(EPUB_FILE):
        try:
            # Create a temporary file to store existing content
            temp_book = epub.EpubBook()
            existing_book = epub.read_epub(EPUB_FILE)
            
            # Copy metadata
            temp_book.metadata = existing_book.metadata
            temp_book.spine = existing_book.spine
            temp_book.toc = existing_book.toc
            
            # Copy all items
            for item in existing_book.items:
                temp_book.add_item(item)
            
            return temp_book
        except Exception as e:
            print(f"[ERROR] Error loading existing EPUB: {e}")
            return epub.EpubBook()
    return epub.EpubBook()

def send_email_with_epub(epub_file, config):
    """Send the generated EPUB file via email"""
    if not config.get('email', {}).get('enabled', False):
        print("[INFO] Email sending is disabled in configuration")
        return

    email_config = config['email']
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = email_config['sender_email']
        msg['To'] = ', '.join(email_config['recipients'])
        msg['Subject'] = f'KingsRSS Daily Update - {date.today().strftime("%Y-%m-%d")}'

        # Add body
        body = "Here's your daily KingsRSS update. The EPUB file is attached."
        msg.attach(MIMEText(body, 'plain'))

        # Attach EPUB file
        with open(epub_file, 'rb') as f:
            epub_attachment = MIMEApplication(f.read(), _subtype='epub')
            epub_attachment.add_header('Content-Disposition', 'attachment', 
                                    filename=Path(epub_file).name)
            msg.attach(epub_attachment)

        # Connect to SMTP server and send email
        with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
            server.starttls()
            server.login(email_config['sender_email'], email_config['sender_password'])
            server.send_message(msg)

        print("[INFO] Email sent successfully")

    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}")

def create_epub(book, new_chapters, section_title):
    try:
        if not book.metadata:
            # Set metadata only if it's a new book
            book.set_identifier(secrets.token_hex(8))
            book.set_title(f"KingsRSS Feed Collection {date.today().strftime('%Y%m%d')}")
            book.set_language("en")

            # Add cover only for new books
            cover_image_path = get_random_file(COVER_DIR)
            if cover_image_path:
                book.set_cover("cover.jpg", open(cover_image_path, 'rb').read())

            # Add CSS style
            style = '''
            body { 
                font-family: Times, serif;
                margin: 5%; 
                text-align: justify;
            }
            img {
                max-width: 100%;
                height: auto;
            }
            '''
            nav_css = epub.EpubItem(
                uid="style_nav",
                file_name="style/nav.css",
                media_type="text/css",
                content=style
            )
            book.add_item(nav_css)

            # Add prefix HTML if configured
            config = load_config(CONFIG_FILE)
            if config and 'prefix_html' in config:
                prefix_chapter = epub.EpubHtml(
                    title='Introduction',
                    file_name='intro.xhtml'
                )
                prefix_chapter.content = f'''
                <html>
                    <head>
                        <link rel="stylesheet" href="style/nav.css" type="text/css"/>
                    </head>
                    <body>
                        {config['prefix_html']}
                    </body>
                </html>
                '''
                book.add_item(prefix_chapter)
                book.spine = ['nav', prefix_chapter]
            else:
                book.spine = ['nav']

        # Get existing chapters from spine
        existing_chapters = [item for item in book.spine if isinstance(item, epub.EpubHtml)]
        
        # Process new chapters
        for chapter in new_chapters:
            book.add_item(chapter)
            existing_chapters.append(chapter)

        # Update TOC with sections
        # Find existing section or create new one
        new_section = (epub.Section(section_title), new_chapters)
        
        if not book.toc:
            book.toc = [new_section]
        else:
            # Check if section exists
            section_exists = False
            for item in book.toc:
                if isinstance(item, tuple) and item[0].title == section_title:
                    # Append new chapters to existing section
                    item[1].extend(new_chapters)
                    section_exists = True
                    break
            if not section_exists:
                book.toc.append(new_section)

        # Update spine with new chapters
        book.spine = book.spine + new_chapters

        # Add navigation files
        book.add_item(epub.EpubNcx())
        book.add_item(epub.EpubNav())

        # Write epub file
        epub.write_epub(EPUB_FILE, book, {})
        print(f"[INFO] EPUB file updated successfully: {EPUB_FILE}")
        
        # Send email after successful EPUB generation
        config = load_config(CONFIG_FILE)
        if config:
            send_email_with_epub(EPUB_FILE, config)
        
    except Exception as e:
        print(f"[ERROR] Error creating/updating EPUB file: {e}")

def process_rss(url, name, section_title):
    feed = download_rss(url)
    if not feed:
        return

    book = load_existing_epub()
    new_chapters = []

    for entry in feed.entries:
        if not is_article_processed(url, entry.title):
            try:
                article_html = entry.content[0].value if 'content' in entry else entry.description
                soup = BeautifulSoup(article_html, 'html.parser')
                
                download_and_embed_images(soup, book)

                chapter = epub.EpubHtml(
                    title=entry.title,
                    file_name=f"{name}_{secrets.token_hex(4)}.xhtml"
                )
                chapter.content = f'''
                <html>
                    <head>
                        <link rel="stylesheet" href="style/nav.css" type="text/css"/>
                    </head>
                    <body>
                        <h1>{entry.title}</h1>
                        {str(soup)}
                    </body>
                </html>
                '''
                new_chapters.append(chapter)

                print(f"[INFO] Added chapter: {entry.title}")
                mark_article_processed(url, entry.title)

            except Exception as e:
                print(f"[ERROR] Error processing entry {entry.title}: {e}")
                continue

    if new_chapters:
        create_epub(book, new_chapters, section_title)
    else:
        print("[INFO] No new articles to process")

def load_config(config_file):
    try:
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    except OSError as e:
        print(f"[ERROR] Error reading configuration file {config_file}: {e}")
        return None

if __name__ == "__main__":
    init_db()
    config = load_config(CONFIG_FILE)
    if config is None:
        print("[ERROR] Configuration file loading failed.")
    else:
        urls_names = config['urls']
        # Get the global prefix HTML if defined in config
        prefix_html = config.get('prefix_html', '')
        
        for item in urls_names:
            process_rss(
                item['url'],
                item['name'],
                item.get('section', item['name']),
                prefix_html=prefix_html,
                add_prefix=item.get('add_prefix', True)  # Default to True if not specified
            )
