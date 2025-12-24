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

    Batches all requests by type (HTML/RSS) and executes them in parallel.
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

    def _package_jobs(self, urls: List[str], fields: List[Dict], job_type: str) -> List:
        """Package URLs and fields into jobs.

        Args:
            urls: List of URLs to fetch
            fields: List of field configurations
            job_type: Type of job ('html' or 'rss')

        Returns:
            List of (url, field_selectors) tuples
        """
        field_selectors = {field['name']: field['selector'] for field in fields}
        return [(url, field_selectors) for url in urls]

    def _execute_jobs(self, jobs: List, job_type: str) -> List[Any]:
        """Execute a batch of jobs.

        Args:
            jobs: List of (url, field_selectors) tuples
            job_type: Type of job ('html' or 'rss')

        Returns:
            List of fetched data (in same order as jobs)
        """
        if not jobs:
            return []

        # Create appropriate fetcher
        if job_type == 'html':
            fetcher = HTTPFetch(max_workers=self.max_workers, timeout=self.timeout)
        else:  # rss
            fetcher = RSSFetch(max_workers=self.max_workers, timeout=self.timeout)

        # Add sources
        for url, field_selectors in jobs:
            if job_type == 'html':
                fetcher.add_source(url=url, fields=field_selectors)
            else:  # rss
                field_names = list(field_selectors.keys())
                fetcher.add_source(url=url, fields=field_names)

        # Execute batch and return results
        return fetcher.parse(mthred=self.mthred)

    def run(self) -> Dict[str, Any]:
        """Execute the dispatcher and fetch all configured sources.

        Returns:
            Dictionary mapping source names to their extracted data
        """
        html_jobs = []
        rss_jobs = []
        source_info = []  # (name, url, type, follow_field, job_index_by_type)

        # A) Package jobs by type
        for source_config in self.config['sources']:
            name = source_config['name']
            url = source_config['location']
            source_type = source_config['type']
            fields = source_config['fields']

            # Find follow field
            follow_field = next((field for field in fields if field.get('follow')), None)

            # Package job for this source
            jobs = self._package_jobs([url], fields, source_type)

            # Track which index this job will have in the results
            if source_type == 'html':
                job_index = len(html_jobs)
                html_jobs.extend(jobs)
            else:
                job_index = len(rss_jobs)
                rss_jobs.extend(jobs)

            source_info.append((name, url, source_type, follow_field, job_index))

        # B) Execute all jobs by type
        html_results = self._execute_jobs(html_jobs, 'html')
        rss_results = self._execute_jobs(rss_jobs, 'rss')

        # Process results and handle follow fields
        final_results = {}

        for name, url, source_type, follow_field, job_index in source_info:
            # Get data by index
            if source_type == 'html':
                data = html_results[job_index]
            else:
                data = rss_results[job_index]

            # No follow? Return data as-is
            if not follow_field:
                final_results[name] = data
                continue

            # 2) Get links from data
            field_name = follow_field['name']
            selector = follow_field['selector']

            if source_type == 'rss':
                links = [item[selector] for item in data if selector in item]
            else:  # html
                links = data.get(field_name, [])

            if not links:
                final_results[name] = {field_name: []}
                continue

            # Make links absolute
            absolute_links = [urljoin(url, link) if not link.startswith('http') else link for link in links]

            # 3) Package follow jobs
            follow_config = follow_field['follow_config']
            follow_type = follow_config['type']
            follow_fields = follow_config['fields']

            follow_jobs = self._package_jobs(absolute_links, follow_fields, follow_type)

            # 4) Execute follow jobs
            follow_results = self._execute_jobs(follow_jobs, follow_type)

            # Results are already in order matching absolute_links
            final_results[name] = {field_name: follow_results}

        return final_results
