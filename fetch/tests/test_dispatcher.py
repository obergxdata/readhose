from pathlib import Path
from fetch.dispatcher import Dispatcher


def test_dispatcher_single_page():
    """Test single-page fetching without following links."""
    test_dir = Path(__file__).parent
    config_path = test_dir / "test_source.yml"

    dispatch = Dispatcher(source=str(config_path), mthred=True, max_workers=4)
    result = dispatch.run()

    # Test single page results
    assert "single_page" in result
    assert result["single_page"]["title"] == ["Sample Article Title"]
    assert result["single_page"]["author"] == ["John Doe"]
    assert len(result["single_page"]["paragraphs"]) == 3


def test_dispatcher_listing_page():
    """Test multi-step fetching (listing page -> detail pages)."""
    test_dir = Path(__file__).parent
    config_path = test_dir / "test_source.yml"

    dispatch = Dispatcher(source=str(config_path), mthred=True, max_workers=4)
    result = dispatch.run()

    # Test listing page with followed links
    assert "listing_page" in result
    assert "links" in result["listing_page"]

    followed_links = result["listing_page"]["links"]
    assert len(followed_links) == 2  # Two links in home.html

    # First link (link.html)
    assert followed_links[0]["title"] == ["Sample Article Title"]
    assert followed_links[0]["author"] == ["John Doe"]

    # Second link (link2.html)
    assert followed_links[1]["title"] == ["Second Article Title"]
    assert followed_links[1]["author"] == ["Jane Smith"]


def test_dispatcher_rss_feed():
    """Test single-step RSS feed fetching."""
    test_dir = Path(__file__).parent
    config_path = test_dir / "test_source.yml"

    dispatch = Dispatcher(source=str(config_path), mthred=True, max_workers=4)
    result = dispatch.run()

    # Test RSS feed results
    assert "rss_feed" in result
    rss_items = result["rss_feed"]

    # RSS returns a list of items
    assert isinstance(rss_items, list)
    assert len(rss_items) == 3  # Three items in feed.xml

    # Check first item
    assert rss_items[0]["title"] == "First Article"
    assert rss_items[0]["description"] == "This is the first article in the feed"
    assert rss_items[0]["author"] == "John Doe"

    # Check second item
    assert rss_items[1]["title"] == "Second Article"
    assert rss_items[1]["author"] == "Jane Smith"


def test_dispatcher_rss_with_follow():
    """Test multi-step RSS feed fetching (RSS -> follow links -> HTML)."""
    test_dir = Path(__file__).parent
    config_path = test_dir / "test_source.yml"

    dispatch = Dispatcher(source=str(config_path), mthred=True, max_workers=4)
    result = dispatch.run()

    # Test RSS with follow
    assert "rss_with_follow" in result
    assert "link" in result["rss_with_follow"]

    followed_links = result["rss_with_follow"]["link"]
    assert len(followed_links) == 3  # Three items in the RSS feed

    # First item links to link.html
    assert followed_links[0]["title"] == ["Sample Article Title"]
    assert followed_links[0]["author"] == ["John Doe"]

    # Second item links to link2.html
    assert followed_links[1]["title"] == ["Second Article Title"]
    assert followed_links[1]["author"] == ["Jane Smith"]

    # Third item links to home.html (different content)
    assert followed_links[2]["title"] == ["Test Heading"]
    assert "author" not in followed_links[2]  # home.html doesn't have author field
