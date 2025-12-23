import requests
import logging
from lxml import html, etree
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class HTTPFetch:
    def __init__(self, max_workers: Optional[int] = None, timeout: float = 30.0) -> None:
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

    def add_source(self, url: str, fields: Optional[Dict[str, str]] = None) -> None:
        """Add an HTTP source with XPath expressions for field extraction.

        Args:
            url: The URL to fetch
            fields: Dictionary mapping field names to XPath expressions
                   Example: {"title": "//title/text()", "heading": "//h1/text()"}
                   If None or empty dict, returns the full HTML content as a string
        """
        self.sources.append({
            'url': url,
            'fields': fields if fields is not None else {}
        })

    def _fetch_page(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch and parse a single HTML page using XPath.

        Args:
            source: Dictionary containing 'url' and 'fields'

        Returns:
            Dictionary with extracted field values, or {"html": "..."} if no fields specified
        """
        url = source['url']
        fields = source['fields']

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
                    # If xpath returns a list, take the first element
                    if isinstance(elements, list) and len(elements) > 0:
                        value = elements[0]
                    else:
                        value = elements

                    # Check if we got an Element object instead of text/attribute
                    if isinstance(value, (html.HtmlElement, etree._Element)):
                        logger.warning(
                            f"XPath '{xpath_expr}' for field '{field_name}' returned an Element object instead of text/attribute. "
                            f"Consider using '/text()' or '/@attribute' in your XPath. Skipping field."
                        )
                        continue

                    result[field_name] = value
                else:
                    logger.warning(f"XPath '{xpath_expr}' for field '{field_name}' returned no results from '{url}'")
            except Exception as e:
                logger.error(f"XPath error for field '{field_name}' on '{url}': {e}")

        return result

    def parse(self, mthred: bool = False) -> List[Dict[str, Any]]:
        """Parse all added HTTP sources.

        Args:
            mthred: If True, use multi-threading to fetch pages in parallel.

        Returns:
            List of dictionaries containing the extracted fields from each source.
            Access pattern: data[source_index]["field_name"]
        """
        if mthred:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self._fetch_page, source): idx
                          for idx, source in enumerate(self.sources)}

                results_dict = {}
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        results_dict[idx] = future.result()
                    except Exception as e:
                        source_url = self.sources[idx]['url']
                        logger.error(f"Exception while fetching '{source_url}': {e}")
                        results_dict[idx] = {}

                all_results = [results_dict[i] for i in range(len(self.sources))]
        else:
            all_results = []
            for source in self.sources:
                try:
                    result = self._fetch_page(source)
                    all_results.append(result)
                except Exception as e:
                    logger.error(f"Exception while fetching '{source['url']}': {e}")
                    all_results.append({})

        return all_results
