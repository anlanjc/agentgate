"""GW-7(评审 2.0)回归:部署配置完整性 —— 生产拓扑必须强制携带 Fernet 主密钥。

根因:compose 置 ENVIRONMENT=prod 却没有 CHANNEL_ENC_KEY 注入位 → gateway 启动即被
prod 校验器拒绝 → crash-loop;运维最省事的"修复"是删 ENVIRONMENT=prod,三道安全闸
随之静默失效。修复:compose 与 admin_token 同款 `:?` 强制项 + 网关 fail-fast 校验器
(缺密钥时清晰报错拒绝启动,错误信息含生成命令)。

诚实边界:本机无 Docker —— 这里是静态验证口径(compose 文件结构 + Settings 校验),
容器真启动冒烟属部署日动作(PRODUCTION_GAP A1)。
"""

from pathlib import Path

import pytest
import yaml

from app.security import crypto

_DEPLOY = Path(__file__).resolve().parents[1] / "deploy"


def test_compose_production_topology_requires_master_key():
    """compose 生产拓扑:gateway 的 AGENTGATE_CHANNEL_ENC_KEY 必须是无默认值的
    `:?` 强制项(缺一个 env,docker compose up 直接报错,不给 crash-loop 机会)。"""
    compose = yaml.safe_load((_DEPLOY / "docker-compose.yml").read_text(encoding="utf-8"))
    gw_env = compose["services"]["gateway"]["environment"]
    assert gw_env["AGENTGATE_ENVIRONMENT"] == "prod"
    for required in ("AGENTGATE_ADMIN_TOKEN", "AGENTGATE_CHANNEL_ENC_KEY"):
        assert required in gw_env, f"compose 缺 {required} 注入位(GW-7)"
        # ${VAR:?msg} 形态 = 必填无默认;不允许 ${VAR:-default} 之类弱化写法
        assert gw_env[required].startswith("${") and ":?" in gw_env[required], (
            f"{required} 必须是 :? 强制项,实际: {gw_env[required]}"
        )


def test_k8s_secret_template_has_master_key_placeholder():
    """k8s 交付件同口径:secret.yaml 必须有主密钥占位(部署日替换,不许遗漏)。"""
    import re

    text = (_DEPLOY / "k8s" / "secret.yaml").read_text(encoding="utf-8")
    assert re.search(r"AGENTGATE_CHANNEL_ENC_KEY\s*:", text)


def test_gateway_startup_fails_fast_without_master_key_in_prod():
    """网关启动 fail-fast:prod(及一切非白名单环境)缺主密钥时,Settings 校验器
    拒绝启动,报错含主密钥生成指引 —— 不许带病运行,也不许运维靠删 prod 蒙混。"""
    from pydantic import ValidationError

    from app.config import Settings

    with pytest.raises(ValidationError, match="CHANNEL_ENC_KEY") as ei:
        Settings(
            _env_file=None, environment="prod", admin_token="strong-token",
        )
    # 错误信息可操作:包含生成命令的关键词
    assert "Fernet" in str(ei.value)

    # 补上主密钥即可启动
    s = Settings(
        _env_file=None, environment="prod", admin_token="strong-token",
        channel_enc_key=crypto.generate_key(),
    )
    assert s.channel_enc_key.startswith("enc:v1:") or len(s.channel_enc_key) > 40
