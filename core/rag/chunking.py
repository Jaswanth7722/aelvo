from __future__ import annotations

import re
import hashlib
import logging
from typing import List

from core.rag.types import Document, Chunk, ChunkingStrategy

log = logging.getLogger("aelvo.rag.chunking")


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return len(text) // 4 + 1


def chunk_document(
    document: Document,
    strategy: ChunkingStrategy = ChunkingStrategy.RECURSIVE,
    chunk_size: int = 512,
    chunk_overlap: int = 64,
) -> List[Chunk]:
    if strategy == ChunkingStrategy.FIXED_SIZE:
        return _fixed_size_chunk(document, chunk_size, chunk_overlap)
    elif strategy == ChunkingStrategy.RECURSIVE:
        return _recursive_chunk(document, chunk_size, chunk_overlap)
    elif strategy == ChunkingStrategy.SENTENCE:
        return _sentence_chunk(document, chunk_size, chunk_overlap)
    elif strategy == ChunkingStrategy.SEMANTIC:
        return _semantic_chunk(document, chunk_size, chunk_overlap)
    return _recursive_chunk(document, chunk_size, chunk_overlap)


def _chunk_id(document_id: str, index: int) -> str:
    raw = f"{document_id}_{index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _fixed_size_chunk(doc: Document, size: int, overlap: int) -> List[Chunk]:
    content = doc.content
    chunks: List[Chunk] = []
    start = 0
    index = 0
    while start < len(content):
        end = min(start + size, len(content))
        chunk_text = content[start:end]
        chunks.append(Chunk(
            id=_chunk_id(doc.id, index),
            document_id=doc.id,
            content=chunk_text,
            index=index,
            metadata={**doc.metadata, "chunk_index": index, "chunk_start": start, "chunk_end": end},
            token_count=estimate_tokens(chunk_text),
        ))
        index += 1
        if end >= len(content):
            break
        start = end - overlap
    return chunks


def _recursive_chunk(doc: Document, size: int, overlap: int) -> List[Chunk]:
    content = doc.content
    separators = ["\n\n", "\n", ". ", " ", ""]
    chunks: List[Chunk] = []
    index = 0
    remaining = content
    while remaining:
        if len(remaining) <= size:
            chunks.append(Chunk(
                id=_chunk_id(doc.id, index),
                document_id=doc.id,
                content=remaining.strip(),
                index=index,
                metadata={**doc.metadata, "chunk_index": index},
                token_count=estimate_tokens(remaining),
            ))
            break
        split_point = size
        for sep in separators:
            pos = remaining.rfind(sep, 0, size)
            if pos > size // 2:
                split_point = pos + len(sep)
                break
        chunk_text = remaining[:split_point].strip()
        if chunk_text:
            chunks.append(Chunk(
                id=_chunk_id(doc.id, index),
                document_id=doc.id,
                content=chunk_text,
                index=index,
                metadata={**doc.metadata, "chunk_index": index},
                token_count=estimate_tokens(chunk_text),
            ))
            index += 1
        remaining = remaining[split_point:]
        if overlap > 0 and remaining and index > 0:
            overlap_text = chunks[-1].content[-overlap:] if len(chunks[-1].content) > overlap else chunks[-1].content
            remaining = overlap_text + remaining
    return chunks


def _sentence_chunk(doc: Document, size: int, overlap: int) -> List[Chunk]:
    content = doc.content
    sentences = re.split(r'(?<=[.!?])\s+', content)
    chunks: List[Chunk] = []
    index = 0
    buffer = ""
    for sentence in sentences:
        if estimate_tokens(buffer + sentence) > size and buffer:
            chunks.append(Chunk(
                id=_chunk_id(doc.id, index),
                document_id=doc.id,
                content=buffer.strip(),
                index=index,
                metadata={**doc.metadata, "chunk_index": index},
                token_count=estimate_tokens(buffer),
            ))
            index += 1
            if overlap > 0:
                sentences_at_end = re.split(r'(?<=[.!?])\s+', buffer)
                overlap_sentences = sentences_at_end[-max(1, overlap // 20):] if len(sentences_at_end) > 1 else [buffer[-overlap:]]
                buffer = " ".join(overlap_sentences) + " "
            else:
                buffer = ""
        buffer += sentence + " "
    if buffer.strip():
        chunks.append(Chunk(
            id=_chunk_id(doc.id, index),
            document_id=doc.id,
            content=buffer.strip(),
            index=index,
            metadata={**doc.metadata, "chunk_index": index},
            token_count=estimate_tokens(buffer),
        ))
    return chunks


def _semantic_chunk(doc: Document, size: int, overlap: int) -> List[Chunk]:
    return _recursive_chunk(doc, size, overlap)
