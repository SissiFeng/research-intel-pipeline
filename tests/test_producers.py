"""Basic unit tests for arXiv producer logic."""
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_arxiv_paper_structure():
    """Verify expected fields in a mock arXiv paper dict."""
    paper = {
        "id": "2401.00001",
        "title": "Test Paper",
        "abstract": "Abstract text.",
        "authors": ["Alice", "Bob"],
        "categories": ["cs.AI"],
        "published_date": "2024-01-01T00:00:00Z",
        "updated_date": "2024-01-01T00:00:00Z",
        "doi": None,
        "journal_ref": None,
    }
    required = ["id", "title", "abstract", "authors", "categories", "published_date"]
    for field in required:
        assert field in paper, f"Missing field: {field}"


def test_paper_serializable():
    """Papers must be JSON-serializable for Supabase REST upsert."""
    paper = {
        "id": "2401.00001",
        "title": "Test",
        "authors": ["A"],
        "categories": ["cs.AI"],
    }
    dumped = json.dumps(paper)
    loaded = json.loads(dumped)
    assert loaded["id"] == paper["id"]
