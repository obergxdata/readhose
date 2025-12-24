.PHONY: test test-rss test-http test-dispatcher clean help

# Run all tests
test:
	pytest -v

# Run only RSS tests
test-rss:
	pytest -v fetch/RSS/tests/

# Run only HTTP tests
test-http:
	pytest -v fetch/HTTP/tests/

# Run only dispatcher tests
test-dispatcher:
	pytest -v fetch/tests/

# Clean up cache and temporary files
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Show available targets
help:
	@echo "Available targets:"
	@echo "  test            - Run all tests"
	@echo "  test-rss        - Run only RSS tests"
	@echo "  test-http       - Run only HTTP tests"
	@echo "  test-dispatcher - Run only dispatcher tests"
	@echo "  clean           - Remove cache and temporary files"
	@echo "  help            - Show this help message"
