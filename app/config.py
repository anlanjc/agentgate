"""应用配置(pydantic-settings,环境变量前缀 AGENTGATE_)。"""

from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGENTGATE_", env_file=".env", extra="ignore")

    app_name: str = "agentgate"
    environment: str = "dev"  # prod 时强制 admin_token,拒绝"部署了但管理面裸奔"
    host: str = "127.0.0.1"
    port: int = 8100
    log_level: str = "INFO"

    # 存储(ADR-002):默认 SQLite 零依赖起步,生产 compose 注入 postgres+asyncpg
    db_url: str = "sqlite+aiosqlite:///./agentgate.db"
    redis_url: str = "redis://127.0.0.1:6379/0"

    # 上游渠道(M0 单渠道;M2 换渠道注册表+故障转移)
    upstream_name: str = "dashscope-compat"
    upstream_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    upstream_api_key: str = ""
    upstream_connect_timeout: float = 5.0
    upstream_read_timeout: float = 300.0
    # 该机注册表系统代理坑:LLM 上游必须直连绕过环境代理(PC 会话教训)
    llm_trust_env: bool = False

    # /v1/models 暴露的模型清单(演示口径)
    models: list[str] = ["qwen-plus", "qwen-turbo", "qwen-max"]

    # M1:鉴权与多租户(auth_enabled=False 为纯代理模式,仅 M0 语义/测试用)
    auth_enabled: bool = True
    admin_token: str = ""  # 为空时 admin API 不设防(仅限本机开发)

    # 渠道密钥静态加密主密钥(Fernet;为空=明文口径仅限 dev,prod 强制要求)
    channel_enc_key: str = ""

    # 鉴权开启时,流式请求强制向上游要 usage(否则客户端不传 include_usage 时账本记 0 token)
    force_stream_usage: bool = True

    # Idempotency-Key 响应重放留存时长(重试窗口;仅非流式,见 app/idempotency.py)
    idempotency_ttl_seconds: int = 86_400

    # 成本价目表(P4):JSON 串,模型 → {input, output}(元/百万 token);
    # 未登记的模型账本 cost=NULL(不猜价)。改价需重启(运行时热更不做,诚实边界)
    model_prices: str = ""

    # M5:精确响应缓存(默认关闭;只缓显式 temperature≤0 的非流式无工具请求,租户隔离见 app/cache/response.py)
    response_cache_enabled: bool = False
    response_cache_ttl: int = 300

    # M5+:语义缓存(默认关闭;HashEmbedder 词面相似/生产换 DashScopeEmbedder,见 app/cache/semantic.py)
    semantic_cache_enabled: bool = False
    semantic_cache_threshold: float = 0.92
    semantic_cache_ttl: int = 600
    embed_api_key: str = ""
    embed_api_base: str = ""

    # M6:分布式追踪(默认关;console 导出演示/otlp 生产,span 属性含 tenant/model/channel)
    otel_enabled: bool = False
    otel_exporter: str = "console"  # console | otlp
    otel_otlp_endpoint: str = ""
    otel_service_version: str = "0.1.0"

    @model_validator(mode="after")
    def _fail_closed_in_prod(self):
        if self.environment.lower() != "prod":
            return self
        if not self.admin_token or self.admin_token in {"change-me", "changeme"}:
            raise ValueError(
                "AGENTGATE_ENVIRONMENT=prod 时必须设置非默认 AGENTGATE_ADMIN_TOKEN"
            )
        if not self.auth_enabled:
            raise ValueError("AGENTGATE_ENVIRONMENT=prod 时禁止关闭 auth_enabled")
        if not self.channel_enc_key:
            raise ValueError(
                "AGENTGATE_ENVIRONMENT=prod 时必须设置 AGENTGATE_CHANNEL_ENC_KEY"
                "(渠道密钥静态加密主密钥,cryptography.fernet.Fernet.generate_key() 生成)"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
