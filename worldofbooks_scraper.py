import asyncio
import re
import random
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

from scrapers.base_scraper import BaseScraper
from database.database import add_book, get_book_by_sku, get_db_session
from database.models import Book
from config.config import (
    WOB_BASE_URL, 
    WOB_TARGET_URL, 
    MAX_BOOKS_PER_RUN, 
    SCRAPE_DELAY_MIN, 
    SCRAPE_DELAY_MAX
)

class WorldOfBooksScraper(BaseScraper):
    """Scraper for World of Books Ireland rare non-fiction books."""
    
    def __init__(self):
        super().__init__()
        self.base_url = WOB_BASE_URL
        self.target_url = WOB_TARGET_URL
    
    async def scrape_new_arrivals(self, max_books=MAX_BOOKS_PER_RUN):
        """Scrape new arrivals using minimal working navigation."""
        self.logger.info(f"Starting to scrape new arrivals from World of Books Ireland (limit: {max_books} books)")
        await self.initialize()
        
        if not self.browser or not self.context:
            self.logger.error("Browser or context failed to initialize")
            await self.close()
            return 0

        scraped_count = 0
        skipped_count = 0
        page_num = 1
        
        try:
            self.logger.debug("Creating new page")
            page = await self.new_page()
            if not page:
                self.logger.error("Failed to create new page")
                await self.close()
                return 0
            
            self.logger.debug(f"Page created successfully: {page}")
            
            while scraped_count < max_books:
                current_page_url = self.target_url
                if page_num > 1:
                    if "?" in current_page_url:
                        current_page_url += f"&page={page_num}"
                    else:
                        current_page_url += f"?page={page_num}"
                
                self.logger.info(f"Navigating to page {page_num}: {current_page_url}")
                # Use direct page.goto instead of navigate()
                response = await page.goto(current_page_url, wait_until="domcontentloaded")
                if response is None:
                    self.logger.error("Goto returned None - aborting")
                    break
                if not response.ok:
                    self.logger.warning(f"Failed to load page {page_num} (Status: {response.status}). Stopping.")
                    break
                
                self.logger.info(f"Page loaded with status: {response.status}")
                await page.wait_for_load_state("domcontentloaded")
                self.logger.debug(f"Page {page_num} fully loaded")
                
                book_links = await self._extract_book_links(page)
                if book_links is None or not book_links:
                    self.logger.info(f"No book links found on page {page_num}. Stopping.")
                    break
                
                self.logger.info(f"Found {len(book_links)} books on page {page_num}")
                
                for book_url in book_links:
                    if scraped_count >= max_books:
                        break
                    
                    sku = self._extract_sku_from_url(book_url)
                    if sku and await self._is_book_in_database(sku):
                        self.logger.debug(f"Book with SKU {sku} already exists. Skipping.")
                        skipped_count += 1
                        continue
                    
                    delay = random.uniform(SCRAPE_DELAY_MIN, SCRAPE_DELAY_MAX)
                    self.logger.debug(f"Waiting {delay:.2f} seconds")
                    await asyncio.sleep(delay)
                    
                    try:
                        book_data = await self.scrape_book(book_url)
                        if book_data and book_data.get("title"):
                            book_data.update({
                                "category": "Rare Non-Fiction",
                                "subcategory": None
                            })
                            book_id = add_book(book_data)
                            if book_id:
                                scraped_count += 1
                                self.logger.info(f"Added book: {book_data.get('title')} (ID: {book_id}) - {scraped_count}/{max_books}")
                    except Exception as e:
                        self.logger.error(f"Error processing book {book_url}: {str(e)}")
                
                if scraped_count < max_books:
                    page_num += 1
                    page_delay = random.uniform(SCRAPE_DELAY_MAX, SCRAPE_DELAY_MAX + 3)
                    self.logger.info(f"Moving to page {page_num}. Waiting {page_delay:.2f} seconds")
                    await asyncio.sleep(page_delay)
                else:
                    break
            
            await page.close()
        
        except Exception as e:
            self.logger.error(f"Error during scraping: {str(e)}", exc_info=True)
        
        finally:
            await self.close()
        
        self.logger.info(f"Completed scraping. Added {scraped_count} new books, skipped {skipped_count} existing books")
        return scraped_count
    
    async def scrape_book(self, book_url):
        """Scrape details of a single book."""
        self.logger.debug(f"Scraping book: {book_url}")
        
        book_page = await self.new_page()
        if not book_page:
            self.logger.error(f"Failed to create page for {book_url}")
            return None
        
        response = await self.navigate(book_page, book_url)
        if not response or not response.ok:
            self.logger.warning(f"Failed to load book page: {book_url} (Status: {response.status if response else 'None'})")
            await book_page.close()
            return None
        
        try:
            await book_page.wait_for_load_state("domcontentloaded")
            sku = await self._extract_sku(book_page, book_url)
            title = await self._get_text(book_page, "h1.product__title")
            if not title:
                self.logger.warning(f"No title found for {book_url}")
                await book_page.close()
                return None
            
            author = await self._get_text(book_page, ".product-author") or "Unknown"
            price_text = await self._get_text(book_page, ".price-item--regular")
            price = self._extract_price(price_text)
            isbn = await self._extract_isbn(book_page)
            condition = await self._get_text(book_page, ".product-condition-text")
            binding = await self._extract_binding(book_page)
            description = await self._get_text(book_page, ".product__description")
            
            image_url = await book_page.evaluate(r"""
                () => {
                    const img = document.querySelector('.product__media img');
                    return img ? img.src : null;
                }
            """)
            
            publisher, publication_year = await self._extract_publication_details(book_page)
            
            book_data = {
                "title": title,
                "author": author,
                "isbn": isbn,
                "sku": sku,
                "wob_price": price,
                "wob_url": book_url,
                "condition": condition,
                "binding": binding,
                "publisher": publisher,
                "publication_year": publication_year,
                "description": description,
                "image_url": image_url
            }
            
            await book_page.close()
            return book_data
        
        except Exception as e:
            self.logger.error(f"Error extracting book details from {book_url}: {str(e)}")
            await book_page.close()
            return None
    
    async def _extract_book_links(self, page):
        """Extract links to individual book pages from the collection page."""
        try:
            links = await page.evaluate(r"""
                () => {
                    const bookElements = document.querySelectorAll('.grid-view-item__link');
                    return Array.from(bookElements).map(link => link.href);
                }
            """)
            return links if links else []
        except Exception as e:
            self.logger.error(f"Error extracting book links: {str(e)}")
            return []
    
    def _extract_sku_from_url(self, url):
        """Extract SKU from book URL."""
        parts = url.strip('/').split('/')
        if len(parts) > 0:
            potential_sku = parts[-1]
            if potential_sku and len(potential_sku) > 5:
                return potential_sku
        return None
    
    async def _is_book_in_database(self, sku):
        """Check if a book with the given SKU already exists in the database."""
        if not sku:
            return False
        book = get_book_by_sku(sku)
        return book is not None
    
    async def _extract_sku(self, page, url):
        """Extract the SKU from the page or URL."""
        sku = self._extract_sku_from_url(url)
        if sku:
            return sku
        
        try:
            sku = await page.evaluate(r"""
                () => {
                    const skuElement = document.querySelector('[data-sku], .product-single__sku');
                    if (skuElement) return skuElement.textContent.trim();
                    const jsonLd = document.querySelector('script[type="application/ld+json"]');
                    if (jsonLd) {
                        try {
                            const data = JSON.parse(jsonLd.textContent);
                            if (data.sku) return data.sku;
                            if (data.offers && data.offers.sku) return data.offers.sku;
                        } catch (e) {}
                    }
                    const pageText = document.body.textContent;
                    const skuMatch = pageText.match(/SKU:\s*([A-Z0-9-]+)/i);
                    if (skuMatch) return skuMatch[1].trim();
                    return null;
                }
            """)
            if sku:
                return sku
        except Exception as e:
            self.logger.warning(f"Error extracting SKU: {str(e)}")
        
        fallback_sku = f"WOB-{hash(url) % 10000000:07d}"
        self.logger.warning(f"Using fallback SKU: {fallback_sku}")
        return fallback_sku
    
    async def _extract_isbn(self, page):
        """Extract ISBN from the page."""
        try:
            isbn = await page.evaluate(r"""
                () => {
                    const detailsTable = document.querySelector('.product-details-wrapper');
                    if (detailsTable) {
                        const text = detailsTable.textContent;
                        const isbnMatch = text.match(/ISBN[:\s]*(97[89]\d{10}|\d{9}[0-9X])/i);
                        if (isbnMatch) return isbnMatch[1].trim();
                    }
                    const jsonLd = document.querySelector('script[type="application/ld+json"]');
                    if (jsonLd) {
                        try {
                            const data = JSON.parse(jsonLd.textContent);
                            if (data.isbn) return data.isbn;
                        } catch (e) {}
                    }
                    const pageText = document.body.textContent;
                    const isbnMatch = pageText.match(/ISBN[:\s]*(97[89]\d{10}|\d{9}[0-9X])/i);
                    if (isbnMatch) return isbnMatch[1].trim();
                    return null;
                }
            """)
            if isbn:
                isbn = re.sub(r'[^0-9X]', '', isbn)
                return isbn
        except Exception as e:
            self.logger.warning(f"Error extracting ISBN: {str(e)}")
        return None
    
    async def _extract_binding(self, page):
        """Extract binding/format information."""
        try:
            binding = await page.evaluate(r"""
                () => {
                    const detailsSection = document.querySelector('.product-details-wrapper');
                    if (detailsSection) {
                        const text = detailsSection.textContent;
                        const bindingTerms = [
                            'hardcover', 'hardback', 'hard cover', 'hard back',
                            'paperback', 'softcover', 'soft cover', 'soft back',
                            'leather bound', 'leatherbound', 'audio cd', 'audiobook'
                        ];
                        for (const term of bindingTerms) {
                            const regex = new RegExp('\\b' + term + '\\b', 'i');
                            if (regex.test(text)) {
                                return term.charAt(0).toUpperCase() + term.slice(1).toLowerCase();
                            }
                        }
                    }
                    return null;
                }
            """)
            return binding
        except Exception as e:
            self.logger.warning(f"Error extracting binding: {str(e)}")
            return None
    
    async def _extract_publication_details(self, page):
        """Extract publisher and publication year."""
        publisher, publication_year = None, None
        try:
            publication_info = await page.evaluate(r"""
                () => {
                    const result = { publisher: null, year: null };
                    const detailsSection = document.querySelector('.product-details-wrapper');
                    if (detailsSection) {
                        const text = detailsSection.textContent;
                        const publisherMatch = text.match(/Publisher[:\s]*([^,\n\r.]+)/i);
                        if (publisherMatch) result.publisher = publisherMatch[1].trim();
                        const yearMatch = text.match(/Published[:\s]*in[:\s]*(\d{4})/i) || 
                                        text.match(/Publication[:\s]*year[:\s]*(\d{4})/i) ||
                                        text.match(/Year[:\s]*(\d{4})/i);
                        if (yearMatch) result.year = yearMatch[1];
                    }
                    return result;
                }
            """)
            publisher = publication_info.get('publisher')
            publication_year = publication_info.get('year')
        except Exception as e:
            self.logger.warning(f"Error extracting publication details: {str(e)}")
        return publisher, publication_year
    
    async def _get_text(self, page, selector):
        """Safely extract text content from an element."""
        try:
            element = await page.query_selector(selector)
            if element:
                text = await element.text_content()
                return text.strip() if text else None
            self.logger.debug(f"No element found for selector: {selector}")
        except Exception as e:
            self.logger.debug(f"Error getting text from {selector}: {str(e)}")
        return None
    
    def _extract_price(self, price_text):
        """Extract numerical price from text."""
        if not price_text:
            return None
        price_match = re.search(r'€?(\d+\.\d{2})', price_text)
        if price_match:
            return float(price_match.group(1))
        price_match = re.search(r'(\d+)\.?(\d{2})?', price_text)
        if price_match:
            euros = price_match.group(1)
            cents = price_match.group(2) or '00'
            return float(f"{euros}.{cents}")
        return None
    
    async def scrape(self, *args, **kwargs):
        """Implementation of the abstract scrape method."""
        return await self.scrape_new_arrivals(*args, **kwargs)