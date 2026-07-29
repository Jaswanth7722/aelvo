"""Cloud provider credential management for Azure, AWS Bedrock, and GCP Vertex AI."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from ..types import AuthCredentials

logger = logging.getLogger(__name__)


@dataclass
class AzureAuthConfig:
    """Configuration for Azure OpenAI authentication."""

    provider_id: str = "azure"
    endpoint: str = ""
    api_version: str = "2024-06-01"
    deployment_name: str = ""
    use_entra_id: bool = False
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class AzureAuth:
    """Handles Azure OpenAI authentication (API key & Entra ID)."""

    def __init__(self, config: Optional[AzureAuthConfig] = None) -> None:
        self.config = config or AzureAuthConfig()
        self._token_cache: Optional[tuple[str, float]] = None

    def _resolve_config(self) -> AzureAuthConfig:
        """Resolve config from env vars if not provided."""
        cfg = self.config
        if not cfg.endpoint:
            cfg.endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
        if not cfg.api_version:
            cfg.api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01")
        if not cfg.deployment_name:
            cfg.deployment_name = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", "")
        if not cfg.tenant_id:
            cfg.tenant_id = os.environ.get("AZURE_TENANT_ID")
        if not cfg.client_id:
            cfg.client_id = os.environ.get("AZURE_CLIENT_ID")
        if not cfg.client_secret:
            cfg.client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        return cfg

    async def authenticate(self) -> AuthCredentials:
        """Authenticate with Azure OpenAI."""
        cfg = self._resolve_config()

        if cfg.use_entra_id:
            return await self._entra_auth(cfg)

        api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
        return AuthCredentials(
            provider_id="azure",
            api_key=api_key,
            metadata={
                "auth_type": "azure_api_key",
                "endpoint": cfg.endpoint,
                "api_version": cfg.api_version,
                "deployment": cfg.deployment_name,
            },
        )

    async def _entra_auth(self, cfg: AzureAuthConfig) -> AuthCredentials:
        """Authenticate using Azure Entra ID (formerly Azure AD)."""
        if self._token_cache and self._token_cache[1] > time.time() + 300:
            access_token = self._token_cache[0]
        else:
            try:
                from azure.identity import (
                    ClientSecretCredential,
                    DefaultAzureCredential,
                )
            except ImportError:
                raise ImportError(
                    "azure-identity package required for Entra ID auth. "
                    "Install with: pip install azure-identity"
                )

            if cfg.client_id and cfg.client_secret and cfg.tenant_id:
                credential = ClientSecretCredential(
                    tenant_id=cfg.tenant_id,
                    client_id=cfg.client_id,
                    client_secret=cfg.client_secret,
                )
            else:
                credential = DefaultAzureCredential()

            # Use token directly — azure-identity async is optional
            token = credential.get_token("https://cognitiveservices.azure.com/.default")
            access_token = token.token
            self._token_cache = (access_token, time.time() + token.expires_on - time.time())

        return AuthCredentials(
            provider_id="azure",
            api_key=access_token,
            access_token=access_token,
            metadata={
                "auth_type": "azure_entra_id",
                "endpoint": cfg.endpoint,
                "api_version": cfg.api_version,
                "deployment": cfg.deployment_name,
            },
        )


@dataclass
class BedrockAuthConfig:
    """Configuration for AWS Bedrock authentication."""

    provider_id: str = "bedrock"
    region: str = ""
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    profile: Optional[str] = None
    role_arn: Optional[str] = None


class BedrockAuth:
    """Handles AWS Bedrock authentication via boto3/aioboto3."""

    def __init__(self, config: Optional[BedrockAuthConfig] = None) -> None:
        self.config = config or BedrockAuthConfig()

    def _resolve_config(self) -> BedrockAuthConfig:
        cfg = self.config
        if not cfg.region:
            cfg.region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        if not cfg.access_key_id:
            cfg.access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        if not cfg.secret_access_key:
            cfg.secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not cfg.session_token:
            cfg.session_token = os.environ.get("AWS_SESSION_TOKEN")
        if not cfg.profile:
            cfg.profile = os.environ.get("AWS_PROFILE")
        return cfg

    async def authenticate(self) -> AuthCredentials:
        """Authenticate with AWS Bedrock."""
        cfg = self._resolve_config()

        return AuthCredentials(
            provider_id="bedrock",
            api_key=cfg.access_key_id or "",
            access_token=cfg.session_token,
            metadata={
                "auth_type": "aws_bedrock",
                "region": cfg.region,
                "profile": cfg.profile,
                "role_arn": cfg.role_arn,
                "has_access_key": bool(cfg.access_key_id),
                "has_secret_key": bool(cfg.secret_access_key),
            },
        )

    async def get_session(self) -> Any:
        """Get a configured boto3 session for Bedrock."""
        cfg = self._resolve_config()
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 package required for Bedrock auth. Install with: pip install boto3")

        session_kwargs: dict[str, Any] = {"region_name": cfg.region}
        if cfg.profile:
            session_kwargs["profile_name"] = cfg.profile
        if cfg.access_key_id and cfg.secret_access_key:
            session_kwargs["aws_access_key_id"] = cfg.access_key_id
            session_kwargs["aws_secret_access_key"] = cfg.secret_access_key
        if cfg.session_token:
            session_kwargs["aws_session_token"] = cfg.session_token

        session = boto3.Session(**session_kwargs)

        if cfg.role_arn:
            sts = session.client("sts")
            assumed = sts.assume_role(
                RoleArn=cfg.role_arn,
                RoleSessionName="aelvo-bedrock-session",
            )
            creds = assumed["Credentials"]
            session = boto3.Session(
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
                region_name=cfg.region,
            )

        return session


@dataclass
class VertexAuthConfig:
    """Configuration for Google Vertex AI authentication."""

    provider_id: str = "vertex"
    project_id: str = ""
    location: str = "us-central1"
    credentials_path: Optional[str] = None
    service_account_json: Optional[str] = None


class VertexAuth:
    """Handles Google Vertex AI authentication."""

    def __init__(self, config: Optional[VertexAuthConfig] = None) -> None:
        self.config = config or VertexAuthConfig()

    def _resolve_config(self) -> VertexAuthConfig:
        cfg = self.config
        if not cfg.project_id:
            cfg.project_id = os.environ.get(
                "VERTEX_AI_PROJECT_ID",
                os.environ.get("GOOGLE_CLOUD_PROJECT", ""),
            )
        if not cfg.location:
            cfg.location = os.environ.get("VERTEX_AI_LOCATION", "us-central1")
        if not cfg.credentials_path:
            cfg.credentials_path = os.environ.get(
                "GOOGLE_APPLICATION_CREDENTIALS",
                os.environ.get("VERTEX_AI_CREDENTIALS"),
            )
        return cfg

    async def authenticate(self) -> AuthCredentials:
        """Authenticate with Google Vertex AI."""
        cfg = self._resolve_config()

        return AuthCredentials(
            provider_id="vertex",
            api_key=cfg.credentials_path or "",
            metadata={
                "auth_type": "gcp_vertex",
                "project_id": cfg.project_id,
                "location": cfg.location,
                "has_credentials_file": bool(cfg.credentials_path),
                "has_service_account_json": bool(cfg.service_account_json),
            },
        )
