from fetch.RSS.rss_fetch import RSSFetch


def test_single_source_multithreaded():
    """Test fetching a single RSS source with multi-threading enabled."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title", "description": "description"},
        name="rss_job_1"
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 1
    assert "rss_job_1" in data
    assert len(data["rss_job_1"]) > 0
    assert data["rss_job_1"][0]["title"] == "Where to find Episodes 2-7 of The Apology Line"


def test_single_source_sequential():
    """Test fetching a single RSS source without multi-threading."""
    fetcher = RSSFetch()
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title"},
        name="test_source"
    )

    data = fetcher.parse(mthred=False)

    assert len(data) == 1
    assert "test_source" in data
    assert len(data["test_source"]) > 0
    assert "title" in data["test_source"][0]


def test_multiple_sources_multithreaded():
    """Test fetching multiple RSS sources with multi-threading."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title", "description": "description"},
        name="source_1"
    )
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title", "description": "description"},
        name="source_2"
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert "source_1" in data
    assert "source_2" in data
    assert data["source_1"][0]["title"] == "Where to find Episodes 2-7 of The Apology Line"
    assert "Sorry Now" in data["source_1"][1]["title"]
    assert data["source_2"][0]["title"] == "Where to find Episodes 2-7 of The Apology Line"


def test_faulty_url_returns_empty():
    """Test that faulty URLs return empty lists without crashing."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.hackboyerror.com/apology-line",
        fields={"title": "title", "link": "link", "description": "description"},
        name="faulty_source"
    )

    data = fetcher.parse(mthred=True)

    assert "faulty_source" in data
    assert data["faulty_source"] == []
    assert len(data["faulty_source"]) == 0


def test_custom_timeout():
    """Test that custom timeout can be set."""
    fetcher = RSSFetch(max_workers=2, timeout=10.0)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title"},
        name="timeout_test"
    )

    data = fetcher.parse(mthred=True)

    assert "timeout_test" in data
    assert len(data["timeout_test"]) > 0
    assert fetcher.timeout == 10.0


def test_custom_max_workers():
    """Test that custom max_workers can be set."""
    fetcher = RSSFetch(max_workers=3)

    assert fetcher.max_workers == 3


def test_field_extraction():
    """Test that specific fields are extracted correctly."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title", "description": "description"},
        name="field_test"
    )

    data = fetcher.parse(mthred=True)

    assert "field_test" in data
    assert len(data["field_test"]) > 0
    first_item = data["field_test"][0]
    assert "title" in first_item
    assert "description" in first_item
    assert isinstance(first_item["title"], str)


def test_mixed_valid_and_invalid_sources():
    """Test handling of mixed valid and invalid sources."""
    fetcher = RSSFetch(max_workers=2)

    # Valid source
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title"},
        name="valid_source"
    )

    # Invalid source
    fetcher.add_source(
        url="https://rss.hackboyerror.com/invalid",
        fields={"title": "title"},
        name="invalid_source"
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert "valid_source" in data
    assert "invalid_source" in data
    assert len(data["valid_source"]) > 0  # Valid source has data
    assert len(data["invalid_source"]) == 0  # Invalid source is empty


def test_custom_field_names():
    """Test that custom field names work correctly (rename fields)."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"article_title": "title", "article_desc": "description"},
        name="custom_fields"
    )

    data = fetcher.parse(mthred=True)

    assert "custom_fields" in data
    assert len(data["custom_fields"]) > 0
    first_item = data["custom_fields"][0]
    # Check that custom field names are used
    assert "article_title" in first_item
    assert "article_desc" in first_item
    # Check that original field names are not present
    assert "title" not in first_item
    assert "description" not in first_item
    # Check that values are correct
    assert (
        first_item["article_title"] == "Where to find Episodes 2-7 of The Apology Line"
    )


def test_auto_generated_names():
    """Test that auto-generated names work when name is not provided."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title"},
    )
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields={"title": "title"},
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert "source_0" in data
    assert "source_1" in data
    assert len(data["source_0"]) > 0
    assert len(data["source_1"]) > 0
