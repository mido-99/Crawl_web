import asyncio
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin
import httpx
from parsel import Selector
from loguru import logger as log
from Filter import UrlFilter


class Crawler:
    async def __aenter__(self):
        self.session = await httpx.AsyncClient(
            timeout=httpx.Timeout(60.0),
            limits=httpx.Limits(max_connections=5),
            headers={
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "accept-language": "en-US;en;q=0.9",
                "accept-encoding": "gzip, deflate, br",
            },
        ).__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.session.__aexit__(*args, **kwargs)

    def __init__(self, filter: UrlFilter, callbacks: Optional[Dict[str, Callable]] = None) -> None:
        self.url_filter = filter
        self.callbacks = callbacks or {}

    def parse(self, responses: List[httpx.Response]) -> List[str]:
        """find valid urls in responses"""
        all_unique_urls = set()
        found = []
        for response in responses:
            sel = Selector(text=response.text, base_url=str(response.url))
            _urls_in_response = set(
                urljoin(str(response.url), url.strip())
                for url in sel.xpath("//a/@href").getall()
            )
            all_unique_urls |= _urls_in_response

        urls_to_follow = self.url_filter.filter(all_unique_urls)
        log.info(f"found {len(urls_to_follow)} urls to follow (from total {len(all_unique_urls)})")
        return urls_to_follow

    async def scrape_url(self, url):
        return await self.session.get(url, follow_redirects=True)

    async def scrape(self, urls: List[str]) -> Tuple[List[httpx.Response], List[Exception]]:
        """scrape urls and return their responses"""
        responses = []
        failures = []
        log.info(f"scraping {len(urls)} urls")

        tasks = [self.scrape_url(url) for url in urls]
        for result in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(result, httpx.Response):
                responses.append(result)
            else:
                failures.append(result)
        return responses, failures

    async def run(self, start_urls: List[str], max_depth=5) -> None:
        """crawl target to maximum depth or until no more urls are found"""
        url_pool = start_urls
        depth = 0
        while url_pool and depth <= max_depth:
            responses, failures = await self.scrape(url_pool)
            log.info(f"depth {depth}: scraped {len(responses)} pages and failed {len(failures)}")
            url_pool = self.parse(responses)
            await self.callback(responses)
            depth += 1

    async def callback(self, responses):
        for response in responses:
            for pattern, fn in self.callbacks.items():
                if pattern.match(str(response.url)):
                    log.debug(f'found matching callback for {response.url}')
                    fn(response=response)
