from dataclasses import dataclass, field
from fetch.HTTP.http_fetch import HTTPFetch
from fetch.RSS.rss_fetch import RSSFetch
import yaml
from pathlib import Path


@dataclass
class Job:
    name: str
    location: str
    type: str
    fields: dict


@dataclass
class Source:
    name: str
    location: str
    type: str
    fields: dict


@dataclass
class FollowSource:
    name: str
    location: str
    type: str
    selector: str
    fields: dict
    follow_type: str


@dataclass
class Sources:
    sources: list[Source] = field(default_factory=list)
    follow_sources: list[FollowSource] = field(default_factory=list)

    def get_follow(self) -> dict[str, FollowSource]:
        # name -> FollowSource
        return {fs.name: fs for fs in self.follow_sources}

    def get_sources(self) -> dict[str, Source]:
        # name -> Source
        return {s.name: s for s in self.sources}


class Dispatcher:

    def __init__(self, config_path: str):
        self.jobs = []
        self.config = self.load_config(config_path)
        self.sources: Sources = self.generate_sources()

    def load_config(self, config_path) -> dict:
        # Builds config file from sources.yml
        path = Path(config_path)
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _convert_fields(self, fields_list: list[dict]) -> dict[str, str]:
        """Convert fields from YAML list format to dict format for fetchers."""
        return {field['name']: field['selector'] for field in fields_list}

    def generate_sources(self) -> Sources:
        # Build Sources class
        sources_obj = Sources()

        # Process regular sources
        if 'sources' in self.config:
            for source_config in self.config['sources']:
                source = Source(
                    name=source_config['name'],
                    location=source_config['location'],
                    type=source_config['type'],
                    fields=self._convert_fields(source_config.get('fields', []))
                )
                sources_obj.sources.append(source)

        # Process follow-sources
        if 'follow-sources' in self.config:
            for follow_config in self.config['follow-sources']:
                follow_source = FollowSource(
                    name=follow_config['name'],
                    location=follow_config['location'],
                    type=follow_config['type'],
                    selector=follow_config['selector'],
                    fields=self._convert_fields(follow_config.get('fields', [])),
                    follow_type=follow_config['follow_type']
                )
                sources_obj.follow_sources.append(follow_source)

        return sources_obj

    def build_jobs(self):
        # build jobs from follow sources
        jobs = []
        sources = self.sources.get_follow()

        for name, source in sources.items():
            jobs.append(
                Job(
                    name=name,
                    location=source.location,
                    type=source.type,
                    fields={source.selector: source.selector},
                )
            )

        results = self.execute_jobs(jobs=jobs)
        # build new jobs
        for name, data in results.items():
            org_source = sources[name]
            if org_source.type == "rss":
                # RSS results: list of dicts, each dict has the selector field
                locations = []
                for entry in data:
                    if org_source.selector in entry:
                        locations.append(entry[org_source.selector])
            else:
                # HTTP results: single dict with selector field containing list of URLs
                locations = data.get(org_source.selector, [])

            for i, location in enumerate(locations):
                self.jobs.append(
                    Job(
                        name=f"{name}_{i}",
                        location=location,
                        type=org_source.follow_type,
                        fields=org_source.fields,
                    )
                )

    def execute_jobs(self, jobs: list[Job]):

        rss = RSSFetch()
        http = HTTPFetch()

        for job in jobs:
            if job.type == "rss":
                rss.add_source(url=job.location, fields=job.fields, name=job.name)
            elif job.type == "html":
                http.add_source(url=job.location, fields=job.fields, name=job.name)

        return {**rss.parse(mthred=True), **http.parse(mthred=True)}
