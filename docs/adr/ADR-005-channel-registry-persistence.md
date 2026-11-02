# ADR-005 渠道注册表持久化(channels 表激活)

状态:已接受 · 2026-11-03 · 关联:[ADR-002](ADR-002-storage-strategy.md)

## 背景

审计发现:channels 表建了但路由从不读它——admin API 注册的渠道只存在于进程内存,
**网关重启即丢**。生产语义下"运行时注册的渠道"是最重要的动态配置之一。

## 决策

双写 + 启动恢复:

1. `POST /admin/channels`:运行时注册(立即生效)**并** upsert 进 channels 表;
2. 启动/`load_channels_from_db(app)`:读取 is_active 渠道,补注册进路由(默认渠道
   settings 优先,表内不覆盖);
3. 渠道健康状态(EMA/冷却)仍存 Redis(多副本一致,与注册表分离——配置是持久态,
   健康是运行态,生命周期不同)。

## 后果

- 正面:重启不丢渠道(k8s 滚动更新/崩溃恢复都安全);测试有"重启恢复"直接断言;
- 负面:渠道 api_key 明文入库(与 One API 同形态;生产建议 KMS/环境变量引用,backlog);
- 测试:PG 口径下 cache_first 测试需先清账本行再删 key(PG 强制外键,SQLite 不强制——
  双口径暴露的差异,已修)。
