# auth/auth/__init__.py - Auth subsystem for AELVO Provider Runtime
"""Authentication subsystem supporting API keys, OAuth, browser sessions, and more."""

from auth.auth.api_key import APIKeyAuth, APIKeyValidator
from auth.auth.oauth import OAuthDeviceFlow, OAuthClientFlow
from auth.auth.browser import BrowserAuthFlow, BrowserAuthConfig, SessionAuth
from auth.auth.browser import SessionAuthConfig as BrowserSessionAuthConfig
from auth.auth.session import SessionToken, SessionManager
from auth.auth.local import LocalRuntimeAuth, LocalAuthConfig
from auth.auth.cloud import AzureAuth, AzureAuthConfig, BedrockAuth, BedrockAuthConfig, VertexAuth, VertexAuthConfig
from auth.auth.refresh import CredentialRefreshEngine, RefreshConfig

__all__ = [
    "APIKeyAuth", "APIKeyValidator",
    "OAuthDeviceFlow", "OAuthClientFlow",
    "BrowserAuthFlow", "BrowserAuthConfig", "SessionAuth", "BrowserSessionAuthConfig",
    "SessionToken", "SessionManager",
    "LocalRuntimeAuth", "LocalAuthConfig",
    "AzureAuth", "AzureAuthConfig", "BedrockAuth", "BedrockAuthConfig",
    "VertexAuth", "VertexAuthConfig",
    "CredentialRefreshEngine", "RefreshConfig",
]
