from typing import List, Pattern
import posixpath
from urllib.parse import urlparse
from tldextract import tldextract
from w3lib.url import canonicalize_url
from loguru import logger as log


class UrlFilter:
    IGNORED_EXTENSIONS = [
        # archives
        '7z', '7zip', 'bz2', 'rar', 'tar', 'tar.gz', 'xz', 'zip',
        # images
        'mng', 'pct', 'bmp', 'gif', 'jpg', 'jpeg', 'png', 'pst', 'psp', 'tif', 'tiff', 'ai', 'drw', 'dxf', 'eps', 'ps', 'svg', 'cdr', 'ico',
        # audio
        'mp3', 'wma', 'ogg', 'wav', 'ra', 'aac', 'mid', 'au', 'aiff',
        # video
        '3gp', 'asf', 'asx', 'avi', 'mov', 'mp4', 'mpg', 'qt', 'rm', 'swf', 'wmv', 'm4a', 'm4v', 'flv', 'webm',
        # office suites
        'xls', 'xlsx', 'ppt', 'pptx', 'pps', 'doc', 'docx', 'odt', 'ods', 'odg', 'odp',
        # other
        'css', 'pdf', 'exe', 'bin', 'rss', 'dmg', 'iso', 'apk',
    ]

    def __init__(self, domain:str=None, subdomain: str=None, follow:List[Pattern]=None) -> None:
        # restrict filtering to specific TLD
        self.domain = domain or ""
        # restrict filtering to sepcific subdomain
        self.subdomain = subdomain or ""
        self.follow = follow or []
        log.info(f"filter created for domain {self.subdomain}.{self.domain} with follow rules {follow}")
        self.seen = set()

    def is_valid_ext(self, url):
        """ignore non-crawlable documents"""
        return posixpath.splitext(urlparse(url).path)[1].lower() not in self.IGNORED_EXTENSIONS

    def is_valid_scheme(self, url):
        """ignore non http/s links"""
        return urlparse(url).scheme in ['https', 'http']
    
    def is_valid_domain(self, url):
        """ignore offsite urls"""
        parsed = tldextract.extract(url)
        return parsed.registered_domain == self.domain and parsed.subdomain == self.subdomain
    
    def is_valid_path(self, url):
        """ignore urls of undesired paths"""
        if not self.follow:
            return True
        path = urlparse(url).path
        for pattern in self.follow:
            if pattern.match(path):
                return True
        return False

    def is_new(self, url):
        """ignore visited urls (in canonical form)"""
        return canonicalize_url(url) not in self.seen

    def filter(self, urls: List[str]) -> List[str]:
        """filter list of urls"""
        found = []
        for url in urls:
            if not self.is_valid_scheme(url):
                log.debug(f"drop ignored scheme {url}")
                continue
            if not self.is_valid_domain(url):
                log.debug(f"drop domain missmatch {url}")
                continue
            if not self.is_valid_ext(url):
                log.debug(f"drop ignored extension {url}")
                continue
            if not self.is_valid_path(url):
                log.debug(f"drop ignored path {url}")
                continue
            if not self.is_new(url):
                log.debug(f"drop duplicate {url}")
                continue
            self.seen.add(canonicalize_url(url))
            found.append(url)
        return found