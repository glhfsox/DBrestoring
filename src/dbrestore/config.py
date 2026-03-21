"""This module loads and validates the app configuration from YAML.
It turns raw user input into typed models that the rest of the system can trust.
Environment placeholders, defaults, aliases, and schedule or storage options are resolved here before runtime starts.
If something feels wrong at the profile level, this file is usually the first place to inspect."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SecretStr,
    field_validator,
    model_validator,
)
from yaml.constructor import ConstructorError

from dbrestore.errors import ConfigError
from dbrestore.utils import (
    collect_env_placeholders,
    ensure_directory,
    expand_env_placeholders,
    expand_user_path,
)

DEFAULT_CONFIG_PATH = Path("dbrestore.yaml")
LEGACY_DEFAULT_CONFIG_PATH = Path("dbrestore.yml")
DB_TYPE_ALIASES = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "mariadb": "mariadb",
    "mongodb": "mongo",
    "mongo": "mongo",
    "sqlite": "sqlite",
}
DEFAULT_PORTS = {
    "postgres": 5432,
    "mysql": 3306,
    "mariadb": 3306,
    "mongo": 27017,
}
NotificationEvent = Literal[
    "backup.completed",
    "backup.failed",
    "restore.failed",
    "verification.completed",
    "verification.failed",
]
DEFAULT_NOTIFICATION_EVENTS: tuple[NotificationEvent, ...] = (
    "backup.completed",
    "backup.failed",
    "restore.failed",
    "verification.completed",
    "verification.failed",
)


def _default_notification_events() -> set[NotificationEvent]:
    return set(DEFAULT_NOTIFICATION_EVENTS)


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


class DefaultsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    output_dir: Path = Path("./backups")
    log_dir: Path = Path("./logs")
    compression: Literal["gzip", "none"] = "gzip"
    retention: RetentionModel | None = None
    notifications: NotificationsModel | None = None


class RetentionModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keep_last: int | None = None
    max_age_days: int | None = None

    @field_validator("keep_last", "max_age_days")
    @classmethod
    def validate_positive(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("retention values must be positive integers")
        return value

    @model_validator(mode="after")
    def validate_retention(self) -> RetentionModel:
        if self.keep_last is None and self.max_age_days is None:
            raise ValueError("retention requires at least one of keep_last or max_age_days")
        return self


class SlackNotificationModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    webhook_url: SecretStr
    events: set[NotificationEvent] = Field(default_factory=_default_notification_events)

    @property
    def webhook_url_value(self) -> str:
        return self.webhook_url.get_secret_value()


class NotificationsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slack: SlackNotificationModel | None = None


class VerificationModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_profile: str
    schedule_after_backup: bool = True

    @field_validator("target_profile", mode="before")
    @classmethod
    def normalize_target_profile(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("verification target_profile must be a string")
        normalized = value.strip()
        if not normalized:
            raise ValueError("verification target_profile cannot be empty")
        return normalized


class ScheduleModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    preset: Literal["hourly", "daily", "weekly"]
    persistent: bool = True

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_fields(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "on_calendar" not in value:
            return value

        normalized = dict(value)
        legacy_value = normalized.pop("on_calendar")
        if "preset" in normalized and normalized["preset"] != legacy_value:
            raise ValueError("schedule preset and on_calendar must match when both are provided")
        normalized.setdefault("preset", legacy_value)
        return normalized

    @field_validator("preset", mode="before")
    @classmethod
    def normalize_preset(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("schedule preset must be a string")
        normalized = value.strip().lower()
        if normalized not in {"hourly", "daily", "weekly"}:
            raise ValueError("schedule preset must be one of: hourly, daily, weekly")
        return normalized

    @property
    def on_calendar(self) -> str:
        return self.preset


class ProfileModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    db_type: Literal["postgres", "mysql", "mariadb", "mongo", "sqlite"]
    host: str | None = None
    port: int | None = None
    username: str | None = None
    password: SecretStr | None = None
    database: str
    output_dir: Path | None = None
    compression: bool | None = None
    auth_database: str | None = None
    retention: RetentionModel | None = None
    schedule: ScheduleModel | None = None
    verification: VerificationModel | None = None
    notifications: NotificationsModel | None = None
    _base_dir: Path = PrivateAttr(default=Path.cwd())

    @field_validator("db_type", mode="before")
    @classmethod
    def normalize_db_type(cls, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("db_type must be a string")
        normalized = DB_TYPE_ALIASES.get(value.strip().lower())
        if normalized is None:
            raise ValueError(f"Unsupported db_type: {value}")
        return normalized

    @field_validator("port")
    @classmethod
    def validate_port(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("port must be a positive integer")
        return value

    @model_validator(mode="after")
    def validate_profile(self) -> ProfileModel:
        if self.db_type == "sqlite":
            if not self.database:
                raise ValueError("sqlite profiles require a database path")
            return self

        if not self.database:
            raise ValueError("database name is required")
        if self.db_type in {"postgres", "mysql", "mariadb"} and not self.username:
            raise ValueError(f"{self.db_type} profiles require username")
        if self.db_type == "mongo" and self.password and not self.username:
            raise ValueError("mongo profiles require username when password is set")
        return self

    def set_base_dir(self, base_dir: Path) -> None:
        self._base_dir = base_dir

    @property
    def password_value(self) -> str | None:
        return self.password.get_secret_value() if self.password else None

    @property
    def effective_host(self) -> str:
        if self.db_type == "sqlite":
            raise ValueError("sqlite does not use host")
        return self.host or "localhost"

    @property
    def effective_port(self) -> int | None:
        if self.db_type == "sqlite":
            return None
        return self.port or DEFAULT_PORTS[self.db_type]

    def resolved_database_path(self) -> Path:
        if self.db_type != "sqlite":
            raise ValueError("Only sqlite profiles have a filesystem database path")
        path = expand_user_path(Path(self.database), field_name="database")
        if path.is_absolute():
            return path
        return (self._base_dir / path).resolve()

    def public_source_metadata(self) -> dict[str, Any]:
        if self.db_type == "sqlite":
            return {
                "database": str(self.resolved_database_path()),
            }
        return {
            "host": self.effective_host,
            "port": self.effective_port,
            "username": self.username,
            "database": self.database,
            "auth_database": self.auth_database,
        }


class StorageModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["local", "s3"] = "local"
    bucket: str | None = None
    prefix: str = "dbrestore"
    region: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: SecretStr | None = None
    session_token: SecretStr | None = None

    @field_validator("prefix", mode="before")
    @classmethod
    def normalize_prefix(cls, value: Any) -> str:
        if value is None:
            return "dbrestore"
        if not isinstance(value, str):
            raise ValueError("storage prefix must be a string")
        return value.strip().strip("/")

    @model_validator(mode="after")
    def validate_storage(self) -> StorageModel:
        if self.type == "local":
            return self
        if not self.bucket or not self.bucket.strip():
            raise ValueError("s3 storage requires bucket")
        return self

    @property
    def secret_access_key_value(self) -> str | None:
        return self.secret_access_key.get_secret_value() if self.secret_access_key else None

    @property
    def session_token_value(self) -> str | None:
        return self.session_token.get_secret_value() if self.session_token else None


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int
    defaults: DefaultsModel = Field(default_factory=DefaultsModel)
    storage: StorageModel = Field(default_factory=StorageModel)
    profiles: dict[str, ProfileModel]
    _base_dir: Path = PrivateAttr(default=Path.cwd())
    _source_path: Path | None = PrivateAttr(default=None)

    def set_source(self, source_path: Path) -> None:
        self._source_path = source_path.resolve()
        self._base_dir = self._source_path.parent
        for profile in self.profiles.values():
            profile.set_base_dir(self._base_dir)

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    @property
    def source_path(self) -> Path | None:
        return self._source_path

    def resolve_path(self, path: Path, *, field_name: str = "path") -> Path:
        try:
            expanded = expand_user_path(path, field_name=field_name)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        if expanded.is_absolute():
            return expanded
        return (self._base_dir / expanded).resolve()

    def get_profile(self, name: str) -> ProfileModel:
        try:
            return self.profiles[name]
        except KeyError as exc:
            available = ", ".join(sorted(self.profiles))
            raise ConfigError(
                f"Profile '{name}' not found. Available profiles: {available}"
            ) from exc

    def output_dir_for(self, profile: ProfileModel, override: Path | None = None) -> Path:
        if override is not None:
            return self.resolve_path(override, field_name="output_dir")
        configured = profile.output_dir or self.defaults.output_dir
        return self.resolve_path(configured, field_name="output_dir")

    def log_file_path(self) -> Path:
        return self.resolve_path(self.defaults.log_dir, field_name="log_dir") / "runs.jsonl"

    def compression_enabled_for(self, profile: ProfileModel, cli_disable: bool = False) -> bool:
        if cli_disable:
            return False
        if profile.compression is not None:
            return profile.compression
        return self.defaults.compression == "gzip"

    def retention_for(self, profile: ProfileModel) -> RetentionModel | None:
        settings: dict[str, Any] = {}
        if self.defaults.retention is not None:
            settings.update(self.defaults.retention.model_dump(exclude_none=True))
        if profile.retention is not None:
            settings.update(profile.retention.model_dump(exclude_none=True))
        if not settings:
            return None
        return RetentionModel.model_validate(settings)

    def notifications_for(self, profile: ProfileModel) -> NotificationsModel | None:
        if profile.notifications is not None:
            return profile.notifications
        return self.defaults.notifications

    def scheduled_profiles(self, selected_profile: str | None = None) -> dict[str, ProfileModel]:
        if selected_profile is not None:
            profile = self.get_profile(selected_profile)
            if profile.schedule is None:
                raise ConfigError(
                    f"Profile '{selected_profile}' does not have a schedule configured"
                )
            return {selected_profile: profile}

        scheduled = {
            name: profile for name, profile in self.profiles.items() if profile.schedule is not None
        }
        if not scheduled:
            raise ConfigError("No scheduled profiles found in config")
        return scheduled


def read_raw_config(config_path: Path = DEFAULT_CONFIG_PATH) -> tuple[Path, dict[str, Any]]:
    resolved = _resolve_existing_config_path(config_path)

    try:
        raw_data = yaml.load(resolved.read_text(encoding="utf-8"), Loader=UniqueKeyLoader) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Unable to parse YAML config: {exc}") from exc
    if not isinstance(raw_data, dict):
        raise ConfigError("Config root must be a mapping")
    return resolved, raw_data


def validate_raw_config_data(
    raw_data: dict[str, Any],
    *,
    source_path: Path | None = None,
    require_env: bool = True,
) -> AppConfig:
    if not isinstance(raw_data, dict):
        raise ConfigError("Config root must be a mapping")

    expanded, missing = expand_env_placeholders(raw_data)
    if require_env and missing:
        missing_names = ", ".join(sorted(missing))
        raise ConfigError(f"Missing environment variables referenced by config: {missing_names}")

    try:
        config = AppConfig.model_validate(expanded)
    except Exception as exc:
        raise ConfigError(f"Invalid configuration: {exc}") from exc

    resolved_source = (
        source_path.resolve() if source_path is not None else DEFAULT_CONFIG_PATH.resolve()
    )
    config.set_source(resolved_source)
    return config


def write_raw_config(config_path: Path, raw_data: dict[str, Any]) -> Path:
    resolved = expand_user_path(config_path, field_name="config path").resolve()
    ensure_directory(resolved.parent)
    serialized = yaml.safe_dump(raw_data, sort_keys=False, default_flow_style=False)
    resolved.write_text(serialized, encoding="utf-8")
    return resolved


def collect_profile_env_vars(config_path: Path, profile_name: str) -> list[str]:
    _, raw_data = read_raw_config(config_path)
    profiles = raw_data.get("profiles")
    if not isinstance(profiles, dict) or profile_name not in profiles:
        available = ", ".join(sorted(profiles.keys())) if isinstance(profiles, dict) else ""
        raise ConfigError(f"Profile '{profile_name}' not found. Available profiles: {available}")

    defaults = raw_data.get("defaults", {})
    profile_data = profiles[profile_name]
    merged = {
        "defaults": defaults if isinstance(defaults, dict) else {},
        "storage": raw_data.get("storage", {})
        if isinstance(raw_data.get("storage", {}), dict)
        else {},
        "profile": profile_data if isinstance(profile_data, dict) else {},
    }
    return sorted(collect_env_placeholders(merged))


def load_config(config_path: Path = DEFAULT_CONFIG_PATH, *, require_env: bool = True) -> AppConfig:
    resolved, raw_data = read_raw_config(config_path)
    return validate_raw_config_data(raw_data, source_path=resolved, require_env=require_env)


def _resolve_existing_config_path(config_path: Path) -> Path:
    try:
        resolved = expand_user_path(config_path, field_name="config path").resolve()
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc

    if resolved.exists():
        return resolved

    if resolved.name == DEFAULT_CONFIG_PATH.name:
        legacy_resolved = resolved.with_name(LEGACY_DEFAULT_CONFIG_PATH.name)
        if legacy_resolved.exists():
            return legacy_resolved

    raise ConfigError(f"Config file not found: {resolved}")
