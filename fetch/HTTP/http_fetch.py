import requests
import logging
from lxml import html, etree
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class HTTPFetch:
    def __init__(
        self, max_workers: Optional[int] = None, timeout: float = 30.0
    ) -> None:
        """Initialize HTTPFetch.

        Args:
            max_workers: Maximum number of threads for concurrent fetching.
                        If None, defaults to min(32, os.cpu_count() + 4).
                        Set to 1 to effectively disable threading.
                        Recommended: 5-10 for typical HTTP fetching to avoid overwhelming servers.
            timeout: Timeout in seconds for each HTTP request (default: 30.0).
                    If a request takes longer than this, it will fail with a timeout error.
        """
        self.sources: List[Dict[str, Any]] = []
        self.max_workers: Optional[int] = max_workers
        self.timeout: float = timeout
        self._source_counter: int = 0

    def add_source(self, url: str, fields: Optional[Dict[str, str]] = None, name: Optional[str] = None) -> None:
        """Add an HTTP source with XPath expressions for field extraction.

        Args:
            url: The URL to fetch
            fields: Dictionary mapping field names to XPath expressions
                   Example: {"title": "//title/text()", "items": "//li/text()"}

                   Note: XPath results are always returned as lists for consistency.
                   - Single match: ["value"]
                   - Multiple matches: ["value1", "value2", "value3"]
                   - No matches: field is omitted from results

                   If None or empty dict, returns the full HTML content as a string
                   under the "html" key.
            name: Optional name for this source. If not provided, auto-generates as "source_0", "source_1", etc.
        """
        if name is None:
            name = f"source_{self._source_counter}"
            self._source_counter += 1

        self.sources.append(
            {"url": url, "fields": fields if fields is not None else {}, "name": name}
        )

    def _fetch_page(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch and parse a single HTML page using XPath.

        Args:
            source: Dictionary containing 'url' and 'fields'

        Returns:
            Dictionary with extracted field values, or {"html": "..."} if no fields specified
        """
        url = source["url"]
        fields = source["fields"]

        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching '{url}' (>{self.timeout}s)")
            return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch HTTP page from '{url}': {e}")
            return {}

        # If no fields specified, return the full HTML content
        if not fields:
            return {"html": response.text}

        try:
            tree = html.fromstring(response.content)
        except Exception as e:
            logger.error(f"Failed to parse HTML from '{url}': {e}")
            return {}

        result = {}
        for field_name, xpath_expr in fields.items():
            try:
                elements = tree.xpath(xpath_expr)
                if elements:
                    # Ensure elements is a list
                    if not isinstance(elements, list):
                        elements = [elements]

                    # Check if list contains Element objects (should use /text() or /@attr)
                    if len(elements) > 0 and isinstance(
                        elements[0], (html.HtmlElement, etree._Element)
                    ):
                        logger.warning(
                            f"XPath '{xpath_expr}' for field '{field_name}' returned an Element object instead of text/attribute. "
                            f"Consider using '/text()' or '/@attribute' in your XPath. Skipping field."
                        )
                        continue

                    # Always return a list for consistency
                    result[field_name] = elements
                else:
                    logger.warning(
                        f"XPath '{xpath_expr}' for field '{field_name}' returned no results from '{url}'"
                    )
            except Exception as e:
                logger.error(f"XPath error for field '{field_name}' on '{url}': {e}")

        return result

    def parse(self, mthred: bool = False) -> Dict[str, Dict[str, Any]]:
        """Parse all added HTTP sources.

        Args:
            mthred: If True, use multi-threading to fetch pages in parallel.

        Returns:
            Dictionary mapping source names to dictionaries with extracted fields.
            Access pattern: data[source_name]["field_name"]
        """
        if mthred:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._fetch_page, source): source["name"]
                    for source in self.sources
                }

                results_dict = {}
                for future in as_completed(futures):
                    source_name = futures[future]
                    try:
                        results_dict[source_name] = future.result()
                    except Exception as e:
                        # Find source by name for error logging
                        source = next((s for s in self.sources if s["name"] == source_name), None)
                        source_url = source["url"] if source else "unknown"
                        logger.error(f"Exception while fetching '{source_url}': {e}")
                        results_dict[source_name] = {}
        else:
            results_dict = {}
            for source in self.sources:
                try:
                    result = self._fetch_page(source)
                    results_dict[source["name"]] = result
                except Exception as e:
                    logger.error(f"Exception while fetching '{source['url']}': {e}")
                    results_dict[source["name"]] = {}

        return results_dict
