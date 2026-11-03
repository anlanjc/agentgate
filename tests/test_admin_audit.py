"""P2 安全 hardening:管理面审计日志——动作落流水/同事务/查询端点/敏感字段纪律。"""

from tests.conftest import TENANT_ID


def _admin_h():
    return {"X-Admin-Token": "dev-admin"}  # 测试 Settings 默认 admin_token


async def test_admin_actions_leave_audit_trail(auth_gateway):
    """租户创建/key 签发/key 吊销/渠道注册/缓存清空各留一条审计,action 与目标正确。"""
    client, app, mock, _ = auth_gateway
    h = _admin_h()

    t = (await client.post("/admin/tenants", json={"name": "audit-t"}, headers=h)).json()
    k = (
        await client.post(f"/admin/tenants/{t['id']}/keys", json={"name": "a"}, headers=h)
    ).json()
    await client.post(f"/admin/keys/{k['key_id']}/deactivate", headers=h)
    await client.post(
        "/admin/channels",
        json={"name": "audited-ch", "base_url": "http://mock/v1", "api_key": "sk-x"},
        headers=h,
    )
    await client.delete("/admin/cache", headers=h)

    entries = (await client.get("/admin/audit", headers=h)).json()["entries"]
    actions = [e["action"] for e in entries]
    assert actions.count("tenant.create") == 1
    assert actions.count("key.issue") == 1
    assert actions.count("key.revoke") == 1
    assert actions.count("channel.register") == 1
    assert actions.count("cache.purge") == 1
    issue = next(e for e in entries if e["action"] == "key.issue")
    assert issue["target_id"] == k["key_id"]
    assert issue["detail"]["prefix"] == k["prefix"]


async def test_audit_never_contains_raw_key(auth_gateway):
    """敏感字段纪律:raw key 与渠道密钥不入审计(泄露审计面=泄露可用凭据)。"""
    client, app, mock, _ = auth_gateway
    h = _admin_h()
    k = (
        await client.post(f"/admin/tenants/{TENANT_ID}/keys", json={"name": "s"}, headers=h)
    ).json()
    await client.post(
        "/admin/channels",
        json={"name": "sec-ch", "base_url": "http://mock/v1", "api_key": "sk-channel-secret"},
        headers=h,
    )
    entries = (await client.get("/admin/audit", headers=h)).json()["entries"]
    blob = str(entries)
    assert k["key"] not in blob
    assert "sk-channel-secret" not in blob


async def test_audit_requires_admin_token(tmp_path, redis_client):
    """审计查询也是管理面:配置了 admin_token 后,无 token 401。"""
    import httpx

    from app.api.main import create_app
    from app.config import Settings
    from app.db.session import init_db
    from tests.conftest import _build_upstream
    from tests.mock_upstream import build_mock_upstream

    settings = Settings(
        upstream_name="mock", upstream_base_url="http://mock/v1", upstream_api_key="sk-t",
        models=["mock-model"], auth_enabled=True,
        admin_token="tok-xyz",
        db_url=f"sqlite+aiosqlite:///{tmp_path}/audit.db",
    )
    upstream = _build_upstream(settings, build_mock_upstream())
    app = create_app(settings=settings, upstream=upstream, redis_client=redis_client)
    await init_db(app.state.engine)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://gw"
    ) as client:
        assert (await client.get("/admin/audit")).status_code == 401
        assert (
            await client.get("/admin/audit", headers={"X-Admin-Token": "tok-xyz"})
        ).status_code == 200
    await upstream.aclose()
    await app.state.task_queue.stop()
    await app.state.engine.dispose()


async def test_audit_same_transaction_as_change(auth_gateway):
    """审计与业务变更同事务:成功的变更必有审计行(集成口径,rollback 路径由 DB 保证)。"""
    client, app, mock, _ = auth_gateway
    h = _admin_h()
    before = len((await client.get("/admin/audit", headers=h)).json()["entries"])
    await client.post("/admin/tenants", json={"name": "tx-t"}, headers=h)
    after = len((await client.get("/admin/audit", headers=h)).json()["entries"])
    assert after == before + 1
    # 失败的变更(重复租户名 409)不留审计行 —— 同事务回滚
    r = await client.post("/admin/tenants", json={"name": "tx-t"}, headers=h)
    assert r.status_code == 409
    assert len((await client.get("/admin/audit", headers=h)).json()["entries"]) == after
