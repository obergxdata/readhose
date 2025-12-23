from fetch.HTTP.http_fetch import HTTPFetch


def test_single_source_sequential():
    """Test fetching a single HTTP source without multi-threading."""
    fetcher = HTTPFetch()
    fetcher.add_source(url="https://brax.nu/", fields={"title": "//title/text()"})

    data = fetcher.parse(mthred=False)

    assert len(data) == 1
    assert len(data[0]) > 0
    assert data[0]["title"] == "brax.nu"


def test_single_source_multithreaded():
    """Test fetching a single HTTP source with multi-threading enabled."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://brax.nu/",
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 1
    assert len(data[0]) > 0
    assert data[0]["title"] == "brax.nu"


def test_multiple_sources_multithreaded():
    """Test fetching multiple HTTP sources with multi-threading."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://brax.nu/",
        fields={"title": "//title/text()"}
    )
    fetcher.add_source(
        url="https://brax.nu/",
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert data[0]["title"] == "brax.nu"
    assert data[1]["title"] == "brax.nu"


def test_faulty_url_returns_empty():
    """Test that faulty URLs return empty dicts without crashing."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://invalid-domain-that-does-not-exist-12345.com/",
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert data[0] == {}
    assert len(data[0]) == 0


def test_custom_timeout():
    """Test that custom timeout can be set."""
    fetcher = HTTPFetch(max_workers=2, timeout=10.0)
    fetcher.add_source(
        url="https://brax.nu/",
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert len(data[0]) > 0
    assert fetcher.timeout == 10.0


def test_custom_max_workers():
    """Test that custom max_workers can be set."""
    fetcher = HTTPFetch(max_workers=3)

    assert fetcher.max_workers == 3


def test_field_extraction_multiple_fields():
    """Test that multiple XPath fields are extracted correctly."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://brax.nu/",
        fields={
            "title": "//title/text()",
            "h1": "//h1/text()"
        }
    )

    data = fetcher.parse(mthred=True)

    assert len(data[0]) > 0
    assert "title" in data[0]
    assert data[0]["title"] == "brax.nu"


def test_missing_xpath_field():
    """Test that missing XPath results are handled gracefully."""
    fetcher = HTTPFetch(max_workers=2)
    fetcher.add_source(
        url="https://brax.nu/",
        fields={
            "title": "//title/text()",
            "nonexistent": "//doesnotexist/text()"
        }
    )

    data = fetcher.parse(mthred=True)

    assert "title" in data[0]
    assert "nonexistent" not in data[0]


def test_element_object_warning():
    """Test that Element objects are detected and skipped with warning."""
    fetcher = HTTPFetch(max_workers=2)
    # Using //title without /text() returns an Element object
    fetcher.add_source(
        url="https://brax.nu/",
        fields={
            "title_element": "//title",  # Returns Element object
            "title_text": "//title/text()"  # Returns text
        }
    )

    data = fetcher.parse(mthred=True)

    # Element object should be skipped
    assert "title_element" not in data[0]
    # Text should be extracted
    assert "title_text" in data[0]
    assert data[0]["title_text"] == "brax.nu"


def test_mixed_valid_and_invalid_sources():
    """Test handling of mixed valid and invalid sources."""
    fetcher = HTTPFetch(max_workers=2)

    # Valid source
    fetcher.add_source(
        url="https://brax.nu/",
        fields={"title": "//title/text()"}
    )

    # Invalid source
    fetcher.add_source(
        url="https://invalid-domain-xyz-123.com/",
        fields={"title": "//title/text()"}
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert len(data[0]) > 0  # Valid source has data
    assert len(data[1]) == 0  # Invalid source is empty


def test_no_fields_returns_full_html():
    """Test that when no fields are specified, full HTML is returned."""
    fetcher = HTTPFetch()
    fetcher.add_source(url="https://brax.nu/")  # No fields specified

    data = fetcher.parse(mthred=False)

    assert len(data) == 1
    assert "html" in data[0]
    assert isinstance(data[0]["html"], str)
    assert len(data[0]["html"]) > 0
    assert "<html" in data[0]["html"].lower()
    assert "brax.nu" in data[0]["html"]


def test_empty_fields_returns_full_html():
    """Test that when empty dict is passed as fields, full HTML is returned."""
    fetcher = HTTPFetch()
    fetcher.add_source(url="https://brax.nu/", fields={})  # Empty fields dict

    data = fetcher.parse(mthred=False)

    assert len(data) == 1
    assert "html" in data[0]
    assert isinstance(data[0]["html"], str)
    assert len(data[0]["html"]) > 0
