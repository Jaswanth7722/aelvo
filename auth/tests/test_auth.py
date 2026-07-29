"""Tests for auth subsystem."""

import pytest
from auth.auth.api_key import APIKeyAuth
from auth.auth.session import SessionManager, SessionToken
from auth.auth.local import LocalRuntimeAuth, LocalAuthConfig
from auth.auth.provider import ProviderAuthOrchestrator
from auth.types import AuthCredentials


class TestAPIKeyAuth:
    def test_store_and_resolve(self):
        auth = APIKeyAuth()
        # Use a key matching OpenAI's pattern: sk- + at least 20 alphanumeric chars
        api_key = "sk-" + "a" * 22
        result = auth.store("openai", api_key)
        assert result is True
        # Resolve from store
        from auth.types import ProviderConfig, ProviderKind, AuthConfig, AuthMethod, CredentialType
        config = ProviderConfig(
            name="openai",
            kind=ProviderKind.FOUNDATION,
            auth=AuthConfig(method=AuthMethod.API_KEY, credential_type=CredentialType.API_KEY),
        )
        key = auth.resolve(config, use_store=True)
        assert key == api_key

    def test_validate_key(self):
        auth = APIKeyAuth()
        api_key = "sk-" + "a" * 22
        valid, reason = auth.validate(api_key, "openai")
        assert valid is True, f"Validation failed: {reason}"

    def test_validate_invalid_key(self):
        auth = APIKeyAuth()
        valid, _ = auth.validate("too-short", "openai")
        assert valid is False

    def test_validate_placeholder_key(self):
        auth = APIKeyAuth()
        valid, _ = auth.validate("your-api-key-here", "openai")
        assert valid is False

    def test_store_invalid_key(self):
        auth = APIKeyAuth()
        result = auth.store("openai", "dummy-key")
        assert result is False


class TestSessionManager:
    def test_register_and_get_session(self):
        mgr = SessionManager()
        token = SessionToken(provider_id="openai", access_token="sk-test")
        mgr.register(token)
        assert mgr.get("openai") is not None
        assert mgr.get("openai").access_token == "sk-test"

    def test_invalidate_session(self):
        mgr = SessionManager()
        mgr.register(SessionToken(provider_id="openai", access_token="sk-test"))
        mgr.invalidate("openai")
        assert mgr.get("openai") is None

    def test_list_active(self):
        mgr = SessionManager()
        mgr.register(SessionToken(provider_id="openai", access_token="sk-1"))
        mgr.register(SessionToken(provider_id="anthropic", access_token="sk-2"))
        assert len(mgr.list_active()) == 2

    def test_refresh_needed_no_session(self):
        mgr = SessionManager()
        assert mgr.refresh_needed("openai")

    def test_refresh_needed_expired(self):
        import time
        mgr = SessionManager()
        mgr.register(SessionToken(
            provider_id="openai",
            access_token="sk-test",
            expires_at=time.time() - 100,
        ))
        assert mgr.refresh_needed("openai")

    def test_get_status(self):
        mgr = SessionManager()
        status = mgr.get_status("openai")
        assert not status.authenticated
        mgr.register(SessionToken(provider_id="openai", access_token="sk-test"))
        status = mgr.get_status("openai")
        assert status.authenticated


class TestSessionToken:
    def test_token_properties(self):
        import time
        token = SessionToken(provider_id="openai", access_token="sk-test")
        assert not token.is_expired
        assert token.token_type == "bearer"

        expired = SessionToken(
            provider_id="openai",
            access_token="sk-test",
            expires_at=time.time() - 100,
        )
        assert expired.is_expired

    def test_from_credentials(self):
        creds = AuthCredentials(provider_id="openai", api_key="sk-test")
        token = SessionToken.from_credentials(creds)
        assert token.provider_id == "openai"
        assert token.access_token == "sk-test"

    def test_serialization(self):
        token = SessionToken(provider_id="openai", access_token="sk-test")
        data = token.to_dict()
        restored = SessionToken.from_dict(data)
        assert restored.provider_id == "openai"
        assert restored.access_token == "sk-test"
