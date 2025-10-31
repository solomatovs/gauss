from pathlib import Path
from typing import Optional, Type, TypeVar, Iterable
from typing import cast

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
    YamlConfigSettingsSource,
    JsonConfigSettingsSource,
    DotEnvSettingsSource,
    CliSettingsSource,
)

T = TypeVar("T", bound=BaseSettings)


class ConfigHelper:
    @staticmethod
    def load(
        settings_cls: Type[T],
        config_files: Optional[Iterable[str | Path]] = None,
        env_prefix: str = "",
        case_sensitive: bool = False,
    ) -> T:
        def get_config_source(file):
            f = Path(file)
            encoding = "utf-8"

            for matching in [f.suffix, f.name]:
                match matching:
                    case ".toml":
                        return TomlConfigSettingsSource(settings_cls, toml_file=f)
                    case ".yml" | ".yaml":
                        return YamlConfigSettingsSource(
                            settings_cls, yaml_file=f, yaml_file_encoding=encoding
                        )
                    case ".json":
                        return JsonConfigSettingsSource(
                            settings_cls, json_file=f, json_file_encoding=encoding
                        )
                    case ".env":
                        return DotEnvSettingsSource(
                            settings_cls,
                            env_file=f,
                            env_file_encoding=encoding,
                            case_sensitive=False,
                        )

            raise ValueError(
                f"config: '{file}' must end with one of the extensions: toml, yaml, yml, json, env"
            )

        # Создаем класс с кастомными источниками
        class ConfigWithSources(settings_cls):  # type: ignore
            model_config = SettingsConfigDict(
                env_prefix=env_prefix,
                case_sensitive=case_sensitive,
                # env_file=str(env_file) if env_file else None,
                extra="ignore",
            )

            @classmethod
            def settings_customise_sources(
                cls,
                settings_cls: type[BaseSettings],
                init_settings: PydanticBaseSettingsSource,
                env_settings: PydanticBaseSettingsSource,
                dotenv_settings: PydanticBaseSettingsSource,
                file_secret_settings: PydanticBaseSettingsSource,
            ) -> tuple[PydanticBaseSettingsSource, ...]:
                """
                Определяет порядок источников настроек (от высшего приоритета к низшему).
                """
                sources = []

                # CLI аргументы (высший приоритет)
                sources.append(
                    CliSettingsSource(settings_cls, cli_parse_args=True),
                )

                # file secret settings
                sources.append(file_secret_settings)

                # Переменные окружения
                sources.append(env_settings)

                # Конфигурационные файлы (включая .env файлы)
                if config_files:
                    sources.extend(map(get_config_source, config_files))

                # .env
                sources.append(dotenv_settings)

                # Значения по умолчанию из init
                sources.append(init_settings)

                return tuple(sources)

        return cast(T, ConfigWithSources())

    @staticmethod
    def typical_config_files():
        return [
            ".env",
            "dev.env",
            "test.env",
            "prod.env",
            "config.json",
            "config.yml",
            "config.toml",
        ]


# Пример использования
if __name__ == "__main__":
    from pydantic import Field

    class DatabaseConfig(BaseSettings):
        host: str = Field(default="localhost", description="Database host")
        port: int = Field(default=5432, description="Database port")
        name: str = Field(default="mydb", description="Database name")

    class AppConfig(BaseSettings):
        app_name: str = Field(default="MyApp", description="Application name")
        debug: bool = Field(default=False, description="Debug mode")
        database: DatabaseConfig = Field(default_factory=DatabaseConfig)
        secret_key: str = Field(default="", description="Secret key")

    # Загрузка конфигурации
    config = ConfigHelper.load(
        AppConfig,
        config_files=[
            "config.yaml",
            "config.local.yaml",
        ],
        env_prefix="APP_",
    )

    print(f"App Name: {config.app_name}")
    print(f"Debug: {config.debug}")
    print(
        f"Database: {config.database.host}:{config.database.port}/{config.database.name}"
    )
