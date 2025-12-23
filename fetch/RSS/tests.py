from fetch.RSS.rss_fetch import RSSFetch


def test_single_source_multithreaded():
    """Test fetching a single RSS source with multi-threading enabled."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields=["title", "description"],
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 1
    assert len(data[0]) > 0
    assert data[0][0]["title"] == "Where to find Episodes 2-7 of The Apology Line"


def test_single_source_sequential():
    """Test fetching a single RSS source without multi-threading."""
    fetcher = RSSFetch()
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields=["title"],
    )

    data = fetcher.parse(mthred=False)

    assert len(data) == 1
    assert len(data[0]) > 0
    assert "title" in data[0][0]


def test_multiple_sources_multithreaded():
    """Test fetching multiple RSS sources with multi-threading."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields=["title", "description"],
    )
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields=["title", "description"],
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert data[0][0]["title"] == "Where to find Episodes 2-7 of The Apology Line"
    assert "Sorry Now" in data[0][1]["title"]
    assert data[1][0]["title"] == "Where to find Episodes 2-7 of The Apology Line"


def test_faulty_url_returns_empty():
    """Test that faulty URLs return empty lists without crashing."""
    fetcher = RSSFetch(max_workers=2)
    fetcher.add_source(
        url="https://rss.hackboyerror.com/apology-line",
        fields=["title", "link", "description"],
    )

    data = fetcher.parse(mthred=True)

    assert data[0] == []
    assert len(data[0]) == 0


def test_custom_timeout():
    """Test that custom timeout can be set."""
    fetcher = RSSFetch(max_workers=2, timeout=10.0)
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields=["title"],
    )

    data = fetcher.parse(mthred=True)

    assert len(data[0]) > 0
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
        fields=["title", "description"],
    )

    data = fetcher.parse(mthred=True)

    assert len(data[0]) > 0
    first_item = data[0][0]
    assert "title" in first_item
    assert "description" in first_item
    assert isinstance(first_item["title"], str)


def test_mixed_valid_and_invalid_sources():
    """Test handling of mixed valid and invalid sources."""
    fetcher = RSSFetch(max_workers=2)

    # Valid source
    fetcher.add_source(
        url="https://rss.art19.com/apology-line",
        fields=["title"],
    )

    # Invalid source
    fetcher.add_source(
        url="https://rss.hackboyerror.com/invalid",
        fields=["title"],
    )

    data = fetcher.parse(mthred=True)

    assert len(data) == 2
    assert len(data[0]) > 0  # Valid source has data
    assert len(data[1]) == 0  # Invalid source is empty
