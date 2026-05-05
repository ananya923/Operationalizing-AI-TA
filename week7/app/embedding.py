"""
embedding.py — Policy document loader and TF-IDF search index.

Loads all 74 TechCorp policy documents from documents.json at startup,
builds an in-memory TF-IDF index, and exposes a search() function
for semantic-ish retrieval (no GPU or API key required).
"""

import json
import os
from typing import Optional
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Path to documents corpus.
# Can be overridden via the DOCS_PATH environment variable — used by Week 7
# diagnosis and optimization scripts to swap corpora without editing code.
_DOCS_PATH = os.environ.get(
    "DOCS_PATH",
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "data",
        "raw",
        "techcorp",
        "documents_week7_bloated.json",  # Week 7 default: bloated corpus
    ),
)

# ---------------------------------------------------------------------------
# Load and index documents at module import time
# ---------------------------------------------------------------------------


def _load_documents() -> list[dict]:
    with open(_DOCS_PATH) as f:
        return json.load(f)


def _build_index(docs: list[dict]):
    """Build a TF-IDF matrix over document titles + content."""
    # Combine title and content so title keywords get matched too
    corpus = [f"{d['title']} {d['content']}" for d in docs]
    vectorizer = TfidfVectorizer(
        stop_words="english",
        max_features=10000,
        ngram_range=(1, 2),  # unigrams + bigrams for better phrase matching
        sublinear_tf=True,  # dampen term frequency for long docs
    )
    matrix = vectorizer.fit_transform(corpus)
    return vectorizer, matrix


# These are built once when the module is first imported
DOCUMENTS: list[dict] = _load_documents()
_VECTORIZER, _MATRIX = _build_index(DOCUMENTS)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def search(query: str, top_k: int = 3, category: Optional[str] = None) -> list[dict]:
    """
    Search policy documents by query string.

    Args:
        query:    Natural language query (e.g. "what is the travel expense limit")
        top_k:    Number of top results to return
        category: Optional filter by document category (e.g. "HR", "Finance")

    Returns:
        List of matching documents with keys: id, title, category, sensitivity,
        content (truncated to 500 chars), score
    """
    # Filter by category if requested
    if category:
        indices = [
            i
            for i, d in enumerate(DOCUMENTS)
            if d.get("category", "").lower() == category.lower()
        ]
        if not indices:
            return []
        sub_matrix = _MATRIX[indices]
        docs_subset = [DOCUMENTS[i] for i in indices]
    else:
        sub_matrix = _MATRIX
        docs_subset = DOCUMENTS
        indices = list(range(len(DOCUMENTS)))

    # Vectorize the query and compute cosine similarity
    query_vec = _VECTORIZER.transform([query])
    scores = cosine_similarity(query_vec, sub_matrix).flatten()

    # Get top_k results sorted by score descending
    top_indices = np.argsort(scores)[::-1][:top_k]

    results = []
    for idx in top_indices:
        if scores[idx] == 0.0:
            continue  # no match at all — skip
        doc = docs_subset[idx]
        results.append(
            {
                "id": doc["id"],
                "title": doc["title"],
                "category": doc.get("category", ""),
                "sensitivity": doc.get("sensitivity", ""),
                "version": doc.get("version", ""),
                "last_updated": doc.get("last_updated", ""),
                "snippet": doc["content"][:500].strip(),  # first 500 chars as preview
                "score": round(float(scores[idx]), 4),
            }
        )

    return results


def get_document_by_id(doc_id: str) -> Optional[dict]:
    """Retrieve a full document by its ID."""
    for doc in DOCUMENTS:
        if doc["id"] == doc_id:
            return doc
    return None


def get_documents_by_category(category: str) -> list[dict]:
    """Return all documents in a given category."""
    return [d for d in DOCUMENTS if d.get("category", "").lower() == category.lower()]


def list_categories() -> list[str]:
    """Return all unique document categories."""
    return sorted(set(d.get("category", "") for d in DOCUMENTS))
