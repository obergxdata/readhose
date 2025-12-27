from fetch.HTTP.http_fetch import HTTPFetch

# Test server is started by conftest.py
TEST_URL = "http://localhost:8765/home.html"


def test_single_source_sequential():
    """Test fetching a single HTTP source without multi-threading."""
    fetcher = HTTPFetch()
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"}, name="test_source")

    data = fetcher.parse(mthred=False)

    assert len(data) == 1
    assert "test_source" in data
    assert data["test_source"]["title"] == ["Test Page"]


def test_single_source_multithreaded():
    """Test fetching a single HTTP source with multi-threading enabled."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"}, name="http_job_1")

    data = fetcher.parse(mthred=True)

    assert len(data) == 1
    assert "http_job_1" in data
    assert data["http_job_1"]["title"] == ["Test Page"]


def test_multiple_sources_multithreaded():
    """Test fetching multiple HTTP sources with multi-threading."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"}, name="source_1")
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"}, name="source_2")

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert "source_1" in data
    assert "source_2" in data
    assert data["source_1"]["title"] == ["Test Page"]
    assert data["source_2"]["title"] == ["Test Page"]


def test_faulty_url_returns_empty():
    """Test that faulty URLs return empty dicts without crashing."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://invalid-domain-that-does-not-exist-12345.com/",
        fields={"title": "//title/text()"},
        name="faulty_source"
    )

    data = fetcher.parse(mthred=True)

    assert "faulty_source" in data
    assert data["faulty_source"] == {}


def test_custom_timeout():
    """Test that custom timeout can be set."""
    fetcher = HTTPFetch(max_workers=2, timeout=10.0)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"}, name="timeout_test")

    data = fetcher.parse(mthred=True)

    assert "timeout_test" in data
    assert data["timeout_test"]["title"] == ["Test Page"]
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
            "list_items": "//ul/li/text()",
        },
        name="multi_field"
    )

    data = fetcher.parse(mthred=True)

    assert "multi_field" in data
    assert data["multi_field"]["title"] == ["Test Page"]
    assert data["multi_field"]["h1"] == ["Test Heading"]
    assert data["multi_field"]["list_items"] == ["Item 1", "Item 2", "Item 3"]


def test_missing_xpath_field():
    """Test that missing XPath results are handled gracefully."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url=TEST_URL,
        fields={"title": "//title/text()", "nonexistent": "//doesnotexist/text()"},
        name="missing_field"
    )

    data = fetcher.parse(mthred=True)

    assert "missing_field" in data
    assert data["missing_field"]["title"] == ["Test Page"]
    assert "nonexistent" not in data["missing_field"]


def test_element_object_warning():
    """Test that Element objects are detected and skipped with warning."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url=TEST_URL,
        fields={
            "title_element": "//title",  # Returns Element object, should be skipped
            "title_text": "//title/text()",
        },
        name="element_test"
    )

    data = fetcher.parse(mthred=True)

    assert "element_test" in data
    assert "title_element" not in data["element_test"]
    assert data["element_test"]["title_text"] == ["Test Page"]


def test_mixed_valid_and_invalid_sources():
    """Test handling of mixed valid and invalid sources."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"}, name="valid_source")
    fetcher.add_source(
        url="https://invalid-domain-xyz-123.com/", fields={"title": "//title/text()"}, name="invalid_source"
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert "valid_source" in data
    assert "invalid_source" in data
    assert data["valid_source"]["title"] == ["Test Page"]
    assert data["invalid_source"] == {}


def test_no_fields_returns_full_html():
    """Test that when no fields are specified, full HTML is returned."""
    fetcher = HTTPFetch()
    fetcher.add_source(url=TEST_URL, name="html_source")

    data = fetcher.parse(mthred=False)

    assert "html_source" in data
    assert "Test Page" in data["html_source"]["html"]


def test_empty_fields_returns_full_html():
    """Test that when empty dict is passed as fields, full HTML is returned."""
    fetcher = HTTPFetch()
    fetcher.add_source(url=TEST_URL, fields={}, name="empty_fields")

    data = fetcher.parse(mthred=False)

    assert "empty_fields" in data
    assert "Test Page" in data["empty_fields"]["html"]


def test_auto_generated_names():
    """Test that auto-generated names work when name is not provided."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})
    fetcher.add_source(url=TEST_URL, fields={"title": "//title/text()"})

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert "source_0" in data
    assert "source_1" in data
    assert data["source_0"]["title"] == ["Test Page"]
    assert data["source_1"]["title"] == ["Test Page"]
