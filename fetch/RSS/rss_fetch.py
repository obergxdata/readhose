import feedparser
import logging
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Union

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
        self._source_counter: int = 0

    def add_source(self, url: str, fields: Optional[Dict[str, str]] = None, name: Optional[str] = None) -> None:
        """Add an RSS feed source with field mappings for extraction.

        Args:
            url: The RSS feed URL to fetch
            fields: Dictionary mapping custom field names to RSS entry attributes
                   Example: {"article_title": "title", "article_link": "link"}

                   The keys are the custom field names you want in the output,
                   and the values are the RSS entry attribute names to extract.

                   If None or empty dict, returns all available fields from each entry.
            name: Optional name for this source. If not provided, auto-generates as "source_0", "source_1", etc.
        """
        if name is None:
            name = f"source_{self._source_counter}"
            self._source_counter += 1

        self.sources.append({
            'url': url,
            'fields': fields if fields is not None else {},
            'name': name
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

        # If no fields specified, return all entry attributes
        if not fields:
            results = []
            for entry in feed.entries:
                # Convert entry to dict with all available attributes
                item = dict(entry)
                if item:
                    results.append(item)
            return results

        results = []
        missing_fields_logged = set()

        for entry in feed.entries:
            item = {}
            for custom_name, rss_field in fields.items():
                if hasattr(entry, rss_field):
                    item[custom_name] = getattr(entry, rss_field)
                elif rss_field in entry:
                    item[custom_name] = entry[rss_field]
                else:
                    # Log missing field once per feed (not for every entry)
                    if rss_field not in missing_fields_logged:
                        logger.warning(f"RSS field '{rss_field}' not found in RSS feed from '{url}'")
                        missing_fields_logged.add(rss_field)

            if item:
                results.append(item)

        return results

    def parse(self, mthred: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        """Parse all added RSS feeds.

        Args:
            mthred: If True, use multi-threading to fetch feeds in parallel.

        Returns:
            Dictionary mapping source names to lists of extracted items.
            Access pattern: data[source_name][item_index]["field_name"]
        """
        if mthred:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self._fetch_feed, source): source['name']
                          for source in self.sources}

                results_dict = {}
                for future in as_completed(futures):
                    source_name = futures[future]
                    try:
                        results_dict[source_name] = future.result()
                    except Exception as e:
                        # Find source by name for error logging
                        source = next((s for s in self.sources if s['name'] == source_name), None)
                        source_url = source['url'] if source else 'unknown'
                        logger.error(f"Exception while fetching '{source_url}': {e}")
                        results_dict[source_name] = []
        else:
            results_dict = {}
            for source in self.sources:
                try:
                    results = self._fetch_feed(source)
                    results_dict[source['name']] = results
                except Exception as e:
                    logger.error(f"Exception while fetching '{source['url']}': {e}")
                    results_dict[source['name']] = []

        return results_dict
