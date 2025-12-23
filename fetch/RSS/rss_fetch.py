import feedparser
import logging
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class RSSFetch:
    def __init__(self, max_workers: Optional[int] = None, timeout: float = 30.0) -> None:
        """Initialize RSSFetch.

        Args:
            max_workers: Maximum number of threads for concurrent fetching.
                        If None, defaults to min(32, os.cpu_count() + 4).
                        Set to 1 to effectively disable threading.
                        Recommended: 5-10 for typical RSS fetching to avoid overwhelming servers.
            timeout: Timeout in seconds for each RSS feed fetch (default: 30.0).
                    If a feed takes longer than this, it will fail with a timeout error.
        """
        self.sources: List[Dict[str, Any]] = []
        self.max_workers: Optional[int] = max_workers
        self.timeout: float = timeout

    def add_source(self, url: str, fields: List[str]) -> None:
        """Add an RSS feed source with specified fields to extract."""
        self.sources.append({
            'url': url,
            'fields': fields
        })

    def _fetch_feed(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetch and parse a single RSS feed."""
        url = source['url']
        fields = source['fields']

        # Set socket timeout for this fetch
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self.timeout)

        try:
            feed = feedparser.parse(url)
        finally:
            # Restore original timeout
            socket.setdefaulttimeout(old_timeout)

        # Check if feed failed to parse or has errors
        if hasattr(feed, 'bozo') and feed.bozo:
            if hasattr(feed, 'bozo_exception'):
                logger.error(f"Failed to fetch RSS feed from '{url}': {feed.bozo_exception}")
            else:
                logger.error(f"Failed to fetch RSS feed from '{url}': Parse error")
            return []

        # Check if feed has no entries
        if not feed.entries:
            logger.warning(f"RSS feed from '{url}' contains no entries")
            return []

        results = []
        missing_fields_logged = set()

        for entry in feed.entries:
            item = {}
            for field in fields:
                if hasattr(entry, field):
                    item[field] = getattr(entry, field)
                elif field in entry:
                    item[field] = entry[field]
                else:
                    # Log missing field once per feed (not for every entry)
                    if field not in missing_fields_logged:
                        logger.warning(f"Field '{field}' not found in RSS feed from '{url}'")
                        missing_fields_logged.add(field)

            if item:
                results.append(item)

        return results

    def parse(self, mthred: bool = False) -> List[List[Dict[str, Any]]]:
        """Parse all added RSS feeds.

        Args:
            mthred: If True, use multi-threading to fetch feeds in parallel.

        Returns:
            List of lists, where each sublist contains dictionaries with extracted fields from one feed.
            Access pattern: data[source_index][item_index]["field_name"]
        """
        if mthred:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self._fetch_feed, source): idx
                          for idx, source in enumerate(self.sources)}

                results_dict = {}
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        results_dict[idx] = future.result()
                    except Exception as e:
                        source_url = self.sources[idx]['url']
                        logger.error(f"Exception while fetching '{source_url}': {e}")
                        results_dict[idx] = []

                all_results = [results_dict[i] for i in range(len(self.sources))]
        else:
            all_results = []
            for source in self.sources:
                try:
                    results = self._fetch_feed(source)
                    all_results.append(results)
                except Exception as e:
                    logger.error(f"Exception while fetching '{source['url']}': {e}")
                    all_results.append([])

        return all_results
