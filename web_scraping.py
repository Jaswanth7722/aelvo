#web_scraping.py
import json
import re
import multiprocessing
import queue  # Fix 2: Required for safe queue fetching
from urllib.parse import urlparse

# The Industrial Speed Stack
import scrapy
from scrapy.crawler import CrawlerProcess
from selectolax.parser import HTMLParser
import markdownify

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
    if parsed.query:
        normalized += f"?{parsed.query}"
    return normalized

class AelvoSpider(scrapy.Spider):
    """
    The High-Speed Asynchronous Spider (The Carpet Bomber).
    Configured for Playwright JS rendering and aggressive timeout/throttling.
    """
    name = "aelvo_spider"

    custom_settings = {
        # 1. SCRAPY-PLAYWRIGHT INTEGRATION
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True, "args": ['--disable-dev-shm-usage']},
        
        # 2. DOMAIN THROTTLING & RATE LIMITING
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 0.5, 
        "DOWNLOAD_TIMEOUT": 15,
        
        # 3. STEALTH & EFFICIENCY (Legal Hardening)
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "ERROR",
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    def __init__(self, target_url, result_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_url = target_url
        self.result_queue = result_queue

    def start_requests(self):
        yield scrapy.Request(
            url=self.target_url, 
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_context_kwargs": {
                    "ignore_https_errors": True,
                }
            }
        )

    async def parse(self, response):
        content_type = response.headers.get(b'Content-Type', b'text/html').decode('utf-8').lower()
        
        if "application/pdf" in content_type or "application/zip" in content_type:
            self.result_queue.put({"error": f"Unsupported binary content: {content_type}"})
            return

        payload = {}

        # --- JSON API PARSING ---
        if "application/json" in content_type:
            try:
                tree = HTMLParser(response.text)
                raw_text = tree.text() if tree.body else response.text
                parsed = json.loads(raw_text)
                payload = {"type": "json", "content": parsed}
            except json.JSONDecodeError:
                payload = {"type": "text", "content": response.text[:25000]}
                
        # --- HTML PARSING (Selectolax Speed Run) ---
        else:
            page = response.meta.get("playwright_page")
            if page:
                await page.close() 
                
            tree = HTMLParser(response.text)
            tags_to_drop = ['script', 'style', 'noscript', 'iframe', 'svg', 'nav', 'footer', 'header', 'aside']
            tree.strip_tags(tags_to_drop)
            
            core_html = tree.body.html if tree.body else tree.html
            
            if tree.body:
                raw_text = tree.body.text(separator='\n', strip=True)
                
                cleaned_lines = [
                    line.strip() for line in raw_text.split('\n') 
                    if len(line.strip()) > 3 or line.strip().startswith('#') or line.strip().startswith('-')
                ]
                final_text = re.sub(r'\n{3,}', '\n\n', '\n'.join(cleaned_lines)).strip()
                
                MAX_CHARS = 25000
                if len(final_text) > MAX_CHARS:
                    final_text = final_text[:MAX_CHARS] + "\n\n...[AELVO SYSTEM: CONTENT TRUNCATED]..."
                    
                payload = {"type": "text", "content": final_text}
            else:
                payload = {"type": "text", "content": "No structural content found."}

        self.result_queue.put({"data": payload})


def _run_spider_process(url: str, result_queue: multiprocessing.Queue):
    """Isolates the Twisted Reactor with suppressed logging."""
    import logging as _logging
    # Suppress ALL logs in this subprocess
    _logging.disable(_logging.CRITICAL)
    for name in ["scrapy", "twisted", "playwright", "asyncio", "scrapy.core",
                 "scrapy.utils", "scrapy.crawler", "scrapy.extensions",
                 "scrapy.core.scraper", "scrapy.core.engine"]:
        _logging.getLogger(name).setLevel(_logging.CRITICAL)
        _logging.getLogger(name).propagate = False
    try:
        process = CrawlerProcess(settings={"LOG_ENABLED": False})
        process.crawl(AelvoSpider, target_url=url, result_queue=result_queue)
        process.start()
    except Exception as e:
        result_queue.put({"error": str(e)})


def execute_light_scrape(url: str, kernel=None) -> dict:
    """Lightweight scraper using requests + selectolax. No JS rendering."""
    import requests as _requests
    clean_url = normalize_url(url)

    if kernel and hasattr(kernel, 'authorize_scrape') and not kernel.authorize_scrape(clean_url):
        return {
            "status": "rejected",
            "logs": f"[AELVO BLOCKED] URL already scraped: '{clean_url}'.",
            "executed": {"url": clean_url}
        }

    try:
        # Tuple timeout: (connect_timeout, read_timeout) to prevent infinite socket hangs
        resp = _requests.get(clean_url, timeout=(5, 15), headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "text/html").lower()

        if "application/json" in content_type:
            try:
                parsed = resp.json()
                return {
                    "status": "success",
                    "logs": f"Light scraped JSON from {clean_url}",
                    "executed": {"url": clean_url},
                    "data": {"type": "json", "content": parsed}
                }
            except Exception:
                pass

        tree = HTMLParser(resp.text)
        tree.strip_tags(['script', 'style', 'noscript', 'iframe', 'svg', 'nav', 'footer', 'header', 'aside'])
        core_html = tree.body.html if tree.body else tree.html

        if tree.body:
            raw_text = tree.body.text(separator='\n', strip=True)
            cleaned = re.sub(r'\n{3,}', '\n\n', raw_text).strip()
            if len(cleaned) > 25000:
                cleaned = cleaned[:25000] + "\n\n...[AELVO: TRUNCATED]..."
            data = {"type": "text", "content": cleaned}
        else:
            data = {"type": "text", "content": "No structural content found."}

        result = {
            "status": "success",
            "logs": f"Light scraped {clean_url}",
            "executed": {"url": clean_url},
            "data": data
        }

        # FIX 5: Audit Log Enforcement (Operational Hardening)
        if kernel:
            try:
                with kernel.db_lock:
                    with kernel.conn:
                        kernel.conn.execute(
                            "INSERT INTO audit_trail (cmd_type, args, status, msg) VALUES (?, ?, ?, ?)",
                            ("light_scrape", json.dumps({"url": clean_url}), "SUCCESS", "Light scrape completed")
                        )
            except Exception: pass
            
        return result
    except Exception as e:
        return {
            "status": "error",
            "logs": f"Light scrape failed: {str(e)}",
            "executed": {"url": clean_url}
        }


def execute_heavy_crawl(url: str, kernel) -> dict:
    """The AELVO Executor Gateway for Deep Scrapes."""
    clean_url = normalize_url(url)
    
    if not kernel.authorize_scrape(clean_url):
        return {
            "status": "rejected",
            "logs": f"[AELVO BLOCKED] URL already scraped: '{clean_url}'.",
            "executed": {"url": clean_url} 
        }

    # Use multiprocessing to safely spin up Scrapy
    result_queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=_run_spider_process, args=(clean_url, result_queue))
    p.start()
    p.join(timeout=25) 
    
    if p.is_alive():
        p.terminate()
        p.join()
        return {
            "status": "error",
            "logs": "Scrapy Process Timed Out (OS Kill).",
            "executed": {"url": clean_url}
        }

    # FIX 2: Bulletproof Queue Fetching (No Race Conditions)
    try:
        result = result_queue.get(timeout=5)
        if "error" in result:
            return {
                "status": "error",
                "logs": result["error"],
                "executed": {"url": clean_url}
            }
        return {
            "status": "success",
            "logs": f"Successfully scraped {clean_url}",
            "executed": {"url": clean_url},
            "data": result["data"]
        }
    except queue.Empty:
        return {
            "status": "error",
            "logs": "Scrapy process crashed or finished without returning data to the queue.",
            "executed": {"url": clean_url}
        }