from fetch.HTTP.http_fetch import HTTPFetch

# Test server is started by conftest.py
TEST_URL = "http://localhost:8765/home.html"


def test_single_source_sequential():
    """Test fetching a single HTTP source without multi-threading."""
    fetcher = HTTPFetch()
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})

    data = fetcher.parse(mthred=False)

    assert data[0]["title"] == ["Test Page"]


def test_single_source_multithreaded():
    """Test fetching a single HTTP source with multi-threading enabled."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url=TEST_URL,
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert data[0]["title"] == ["Test Page"]


def test_multiple_sources_multithreaded():
    """Test fetching multiple HTTP sources with multi-threading."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})

    data = fetcher.parse(mthred=True)

    assert data[0]["title"] == ["Test Page"]
    assert data[1]["title"] == ["Test Page"]


def test_faulty_url_returns_empty():
    """Test that faulty URLs return empty dicts without crashing."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://invalid-domain-that-does-not-exist-12345.com/",
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert data[0] == {}


def test_custom_timeout():
    """Test that custom timeout can be set."""
    fetcher = HTTPFetch(max_workers=2, timeout=10.0)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})

    data = fetcher.parse(mthred=True)

    assert data[0]["title"] == ["Test Page"]
    assert fetcher.timeout == 10.0


def test_custom_max_workers():
    """Test that custom max_workers can be set."""
    fetcher = HTTPFetch(max_workers=3)

    assert fetcher.max_workers == 3


def test_field_extraction_multiple_fields():
    """Test that multiple XPath fields are extracted correctly, including lists."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url=TEST_URL,
        fields={
            "title": "//title/text()",
            "h1": "//h1/text()",
            "list_items": "//ul/li/text()"
        }
    )

    data = fetcher.parse(mthred=True)

    assert data[0]["title"] == ["Test Page"]
    assert data[0]["h1"] == ["Test Heading"]
    assert data[0]["list_items"] == ["Item 1", "Item 2", "Item 3"]


def test_missing_xpath_field():
    """Test that missing XPath results are handled gracefully."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url=TEST_URL,
        fields={
            "title": "//title/text()",
            "nonexistent": "//doesnotexist/text()"
        }
    )

    data = fetcher.parse(mthred=True)

    assert data[0]["title"] == ["Test Page"]
    assert "nonexistent" not in data[0]


def test_element_object_warning():
    """Test that Element objects are detected and skipped with warning."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url=TEST_URL,
        fields={
            "title_element": "//title",  # Returns Element object, should be skipped
            "title_text": "//title/text()"
        }
    )

    data = fetcher.parse(mthred=True)

    assert "title_element" not in data[0]
    assert data[0]["title_text"] == ["Test Page"]


def test_mixed_valid_and_invalid_sources():
    """Test handling of mixed valid and invalid sources."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})
    fetcher.add_source(url="https://invalid-domain-xyz-123.com/", fields={"title": "//title/text()"})

    data = fetcher.parse(mthred=True)

    assert data[0]["title"] == ["Test Page"]
    assert data[1] == {}


def test_no_fields_returns_full_html():
    """Test that when no fields are specified, full HTML is returned."""
    fetcher = HTTPFetch()
    fetcher.add_source(url=TEST_URL)

    data = fetcher.parse(mthred=False)

    assert "Test Page" in data[0]["html"]


def test_empty_fields_returns_full_html():
    """Test that when empty dict is passed as fields, full HTML is returned."""
    fetcher = HTTPFetch()
    fetcher.add_source(url=TEST_URL, fields={})

    data = fetcher.parse(mthred=False)

    assert "Test Page" in data[0]["html"]
