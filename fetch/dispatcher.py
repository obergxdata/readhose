import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin
from fetch.HTTP.http_fetch import HTTPFetch
from fetch.RSS.rss_fetch import RSSFetch

logger = logging.getLogger(__name__)


class Dispatcher:
    """Dispatcher for fetching data from multiple sources defined in a YAML config.

    Supports recursive fetching - sources can specify follow=true to fetch
    linked pages using the same configuration structure.
    """

    def __init__(self, source: str, mthred: bool = False, max_workers: Optional[int] = None, timeout: float = 30.0):
        """Initialize the Dispatcher.

        Args:
            source: Path to the YAML configuration file
            mthred: Whether to use multi-threading for fetching
            max_workers: Maximum number of worker threads (None = auto)
            timeout: Timeout in seconds for each fetch operation
        """
        self.source_path = Path(source)
        self.mthred = mthred
        self.max_workers = max_workers
        self.timeout = timeout
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load and parse the YAML configuration file."""
        if not self.source_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.source_path}")

        with open(self.source_path, 'r') as f:
            config = yaml.safe_load(f)

        if not config or 'sources' not in config:
            raise ValueError("Invalid configuration: missing 'sources' key")

        return config

    def run_fetchers(self, location: str, fetch_type: str, fields: List[Dict[str, Any]], base_url: Optional[str] = None) -> Dict[str, Any]:
        """Recursively fetch data from a location.

        This function handles both HTML and RSS fetching, and recursively
        follows links if any field has follow=true.

        Args:
            location: URL to fetch
            fetch_type: 'html' or 'rss'
            fields: List of field configurations
            base_url: Base URL for resolving relative URLs

        Returns:
            Dictionary with extracted field data
        """
        # Build field selectors and identify follow fields
        field_selectors = {}
        follow_field = None

        for field in fields:
            field_name = field['name']
            selector = field['selector']
            field_selectors[field_name] = selector

            if field.get('follow', False):
                if follow_field:
                    raise ValueError("Only one field can have follow=true per source")
                follow_field = field

        # Fetch data based on type
        if fetch_type == 'html':
            fetcher = HTTPFetch(max_workers=self.max_workers, timeout=self.timeout)
            fetcher.add_source(url=location, fields=field_selectors)
            results = fetcher.parse(mthred=self.mthred)
            data = results[0] if results else {}

        elif fetch_type == 'rss':
            fetcher = RSSFetch(max_workers=self.max_workers, timeout=self.timeout)
            # For RSS, the selector is the field name in the RSS feed
            field_names = list(field_selectors.values())
            fetcher.add_source(url=location, fields=field_names)
            results = fetcher.parse(mthred=self.mthred)
            data = results[0] if results else []

        else:
            raise ValueError(f"Unsupported fetch type: {fetch_type}")

        # If no follow field, return data as-is
        if not follow_field:
            return data

        # Follow links recursively
        field_name = follow_field['name']
        selector = follow_field['selector']

        # Extract links based on fetch type
        if fetch_type == 'rss':
            # RSS returns a list of items, extract the selector field from each
            links = []
            for item in data:
                if selector in item:
                    links.append(item[selector])
        else:
            # HTML returns a dict with the field containing a list of links
            links = data.get(field_name, [])

        if not links:
            logger.warning(f"No links found for field '{field_name}' at {location}")
            return {field_name: []}

        # Make links absolute
        absolute_links = []
        for link in links:
            if not link.startswith('http'):
                base = base_url or location
                absolute_links.append(urljoin(base, link))
            else:
                absolute_links.append(link)

        # Get follow configuration and recursively fetch
        follow_config = follow_field['follow_config']
        follow_type = follow_config['type']
        follow_fields = follow_config['fields']

        followed_data = []
        for link in absolute_links:
            try:
                result = self.run_fetchers(link, follow_type, follow_fields, base_url=location)
                if result:
                    followed_data.append(result)
            except Exception as e:
                logger.error(f"Failed to fetch {link}: {e}")

        return {field_name: followed_data}

    def run(self) -> Dict[str, Any]:
        """Execute the dispatcher and fetch all configured sources.

        Returns:
            Dictionary mapping source names to their extracted data
        """
        results = {}

        for source_config in self.config['sources']:
            name = source_config['name']
            location = source_config['location']
            source_type = source_config['type']
            fields = source_config['fields']

            logger.info(f"Processing source '{name}' from {location}")

            try:
                results[name] = self.run_fetchers(location, source_type, fields)
            except Exception as e:
                logger.error(f"Failed to process source '{name}': {e}")
                results[name] = {}

        return results
