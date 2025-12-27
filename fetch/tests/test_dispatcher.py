from fetch.dispatcher import Dispatcher, Job
from pathlib import Path


def test_build_jobs():
    # Tests building jobs from follow-source
    config_path = Path(__file__).parent / "test_source.yml"
    d = Dispatcher(config_path=str(config_path))
    d.build_jobs()

    # Expected fields for jobs created from follow-sources
    expected_fields = {
        'title': '//h1/text()',
        'author': "//span[@class='author']/text()"
    }

    # Expected jobs from rss_follow (RSS feed returns 3 links)
    expected_rss_jobs = [
        Job(
            name='rss_follow_0',
            location='http://localhost:8765/link.html',
            type='html',
            fields=expected_fields
        ),
        Job(
            name='rss_follow_1',
            location='http://localhost:8765/link2.html',
            type='html',
            fields=expected_fields
        ),
        Job(
            name='rss_follow_2',
            location='http://localhost:8765/home.html',
            type='html',
            fields=expected_fields
        ),
    ]

    # Expected jobs from listing_page (HTML page returns 2 links)
    expected_listing_jobs = [
        Job(
            name='listing_page_0',
            location='link.html',
            type='html',
            fields=expected_fields
        ),
        Job(
            name='listing_page_1',
            location='link2.html',
            type='html',
            fields=expected_fields
        ),
    ]

    expected_jobs = expected_rss_jobs + expected_listing_jobs

    # Validate number of jobs
    assert len(d.jobs) == len(expected_jobs), \
        f"Expected {len(expected_jobs)} jobs, got {len(d.jobs)}"

    # Validate each job matches expected
    assert d.jobs == expected_jobs, \
        f"Jobs don't match expected.\nGot: {d.jobs}\nExpected: {expected_jobs}"
