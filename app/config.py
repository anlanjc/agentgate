"""应用配置(pydantic-settings,环境变量前缀 AGENTGATE_)。"""

from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# GW-6:非生产环境白名单 —— 只有显式列出的名字豁免安全闸校验;'production'/
# 'live'/'main' 等任何未识别值一律按 prod 校验(fail-closed 反转,误配必报错)。
NON_PROD_ENVIRONMENTS = frozenset({"dev", "test", "testing", "staging", "local"})


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AGENTGATE_", env_file=".env", extra="ignore")

    app_name: str = "agentgate"
    # 不在白名单(dev/test/testing/staging/local)内的环境名一律按 prod 强制校验
    environment: str = "dev"
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
    # 幂等锁 TTL(GW-3):必须 ≥ 渠道 read_timeout 上限 —— 锁活不过合法执行期,
    # 重试就会并发执行双计费。心跳续租不做(复杂度不划算),配足 TTL 即覆盖。
    # GW-14:该不变量不在注释里守,在两处硬闸守 —— ①下方 _validate_lock_covers_read_timeout
    # 校验器(默认渠道,启动期 fail-closed);②admin 渠道注册端点(自建渠道,注册时 400)。
    # 缺省 450 = 默认读超时 300 的 1.5x —— 修复版 300==300 零余量,执行满 300s 的请求
    # 收尾时锁恰好过期,compare-and-delete 返回 False,成功响应放弃进重放库。
    idempotency_lock_ttl_seconds: int = 450

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
    # P4-4:检索后端 redis(ZSET 暴力余弦,≤200 条) | pgvector(KNN+HNSW,PG 口径;
    # 开启时 semantic_pg_vector_dim 必须与 embedder 输出维度一致,启动时幂等建表)
    semantic_backend: str = "redis"
    semantic_pg_vector_dim: int = 0

    # M6:分布式追踪(默认关;console 导出演示/otlp 生产,span 属性含 tenant/model/channel)
    otel_enabled: bool = False
    otel_exporter: str = "console"  # console | otlp
    otel_otlp_endpoint: str = ""
    otel_service_version: str = "0.1.0"

    @model_validator(mode="after")
    def _fail_closed_in_prod(self):
        # GW-6(评审 2.0):原实现 `environment != 'prod' 即 return` —— 精确匹配
        # 意味着 'production'/'live'/'main' 等任何变体都静默跳过 admin_token/
        # auth_enabled/channel_enc_key 三道安全闸,误配无任何提示。反转为白名单
        # 豁免:不在 NON_PROD_ENVIRONMENTS 里的一律按 prod 校验(fail-closed)。
        if self.environment.lower() in NON_PROD_ENVIRONMENTS:
            return self
        if not self.admin_token or self.admin_token in {"change-me", "changeme"}:
            raise ValueError(
                f"AGENTGATE_ENVIRONMENT={self.environment}(非 dev/test/staging/local 白名单)"
                "时必须设置非默认 AGENTGATE_ADMIN_TOKEN"
            )
        if not self.auth_enabled:
            raise ValueError(
                f"AGENTGATE_ENVIRONMENT={self.environment}(非白名单环境)禁止关闭 auth_enabled"
            )
        if not self.channel_enc_key:
            raise ValueError(
                f"AGENTGATE_ENVIRONMENT={self.environment}(非白名单环境)时必须设置"
                " AGENTGATE_CHANNEL_ENC_KEY(渠道密钥静态加密主密钥,"
                "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\" 生成)"
            )
        return self

    @model_validator(mode="after")
    def _validate_lock_covers_read_timeout(self):
        # GW-14:幂等锁 TTL 必须 ≥ 默认渠道读超时(所有环境,与 fail-closed 同款
        # 启动期硬约束)—— 锁比合法执行期先死,重试就并发执行双计费(GW-3 原始病灶)。
        if self.idempotency_lock_ttl_seconds < self.upstream_read_timeout:
            raise ValueError(
                f"AGENTGATE_IDEMPOTENCY_LOCK_TTL_SECONDS({self.idempotency_lock_ttl_seconds})"
                f" 必须 ≥ AGENTGATE_UPSTREAM_READ_TIMEOUT({self.upstream_read_timeout}):"
                "幂等锁必须活过最长合法执行期,否则重试并发执行双计费"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
