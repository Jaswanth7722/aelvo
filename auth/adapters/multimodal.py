"""Multimodal payload normalization across providers."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any, Optional, Union


class MultimodalAdapter:
    """Normalizes multimodal payloads (images, audio, video) across providers."""

    @staticmethod
    def normalize_image(
        image_data: Union[str, bytes, Path],
        target_provider: str = "openai",
        detail: str = "auto",
    ) -> dict[str, Any]:
        """Normalize an image for a specific provider format."""
        if isinstance(image_data, Path):
            image_data = image_data.read_bytes()
        if isinstance(image_data, str) and image_data.startswith(("http://", "https://")):
            return MultimodalAdapter._url_image(image_data, target_provider, detail)
        return MultimodalAdapter._base64_image(
            image_data if isinstance(image_data, bytes) else image_data.encode(),
            target_provider, detail,
        )

    @staticmethod
    def _url_image(url: str, provider: str, detail: str) -> dict[str, Any]:
        if provider in ("openai", "azure", "groq", "together", "fireworks", "xai", "openrouter", "lm_studio", "vllm"):
            return {"type": "image_url", "image_url": {"url": url, "detail": detail}}
        if provider in ("anthropic",):
            return {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": url}}
        if provider in ("google", "vertex"):
            return {"inline_data": {"mime_type": "image/jpeg", "data": url}}
        return {"type": "image_url", "image_url": {"url": url}}

    @staticmethod
    def _base64_image(data: bytes, provider: str, detail: str) -> dict[str, Any]:
        b64 = base64.b64encode(data).decode("utf-8")
        if provider in ("openai", "azure", "groq", "together", "fireworks", "xai", "openrouter", "lm_studio", "vllm"):
            return {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": detail}}
        if provider in ("anthropic",):
            return {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}}
        if provider in ("google", "vertex"):
            return {"inline_data": {"mime_type": "image/jpeg", "data": b64}}
        return {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}

    @staticmethod
    def extract_content_with_media(
        text: str,
        images: Optional[list[Union[str, bytes, Path]]] = None,
        provider: str = "openai",
    ) -> Union[str, list[dict[str, Any]]]:
        """Build message content with embedded media."""
        if not images:
            return text

        content: list[dict[str, Any]] = [{"type": "text", "text": text}]
        for img in images:
            content.append(MultimodalAdapter.normalize_image(img, provider))
        return content

    @staticmethod
    def detect_media_type(data: bytes) -> str:
        """Detect media type from bytes."""
        if data.startswith(b"\xff\xd8"):
            return "image/jpeg"
        if data.startswith(b"\x89PNG"):
            return "image/png"
        if data.startswith(b"GIF8"):
            return "image/gif"
        if data.startswith(b"RIFF"):
            return "image/webp"
        return "image/jpeg"
