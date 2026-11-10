# 语义缓存后端基准(2026-11-11,S9-OPT1):Redis 暴力余弦 vs pgvector KNN

**结论(不硬凑,按实测说):当前规模(N≤200)暴力扫描够用且更快;pgvector 的实测优势只有"召回完整性"(N>200 后暴力扫描命中率崩塌至 200/N),而它的出厂查询形态在本机 pgvector 0.6.2 上触发 Seq Scan——HNSW 索引对出厂路径形同虚设,N=5000 时比暴力扫描慢一个数量级(159ms vs 9ms)。** 语义缓存维持默认 `semantic_backend=redis`;pgvector 后端启用前需先做查询形态改造(见"发现"),当前它换召回、付延迟。

## 方法

- **被测对象 = 生产代码本体**:`app/cache/semantic.py`(Redis ZSET 索引 + MGET + Python 余弦,索引容量上限 200)vs `app/cache/semantic_pg.py`(pgvector `<=>` KNN + HNSW)。任务口径中的"SQLite/暴力扫描"落为暴力扫描后端——SQLite 无向量路径(semantic_pg 是 PG 专属,SQLite 口径诚实 skip),生产默认后端即暴力扫描。
- **embedder = HashEmbedder(dim=128)**,mock 口径(禁真模型红线),两后端共用同一 embedder 与同一问题流,embed 开销对等(实测 p50 ≈ 0.03–0.05ms,可忽略)。
- N=100/1000/5000,每轮独立清场(flushdb + TRUNCATE);条目文本互不相同(序号+12 伪字),查询=目标条目的前缀+后缀改写(同条目词面相似 ≈0.9,跨条目低);阈值 0.75(与测试口径一致);**命中后校验返回答案确为目标条目**,答非所问单列(三轮全部 0,词面口径下无错误命中)。
- 查询目标均匀采样(种子 42,K=60,顺序单飞);N>200 时均匀采样天然包含"已被挤出索引"的旧条目——这正是容量上限语义的诚实测法。
- HNSW 是否被走:每轮 EXPLAIN 记录,三轮全部 **Seq Scan**(见"发现")。
- 装置:pgserver 内嵌 PG 16.2(vector 0.6.2,HNSW 索引由 `ensure_table` 建好)+ 便携 Redis 5.0;脚本 `.tooling/agentgate-bench/semantic_pg_bench.py`(不入库,同 chaos 装置先例),产物 bench_output.json 同目录。

## 数据

| N | 后端 | 查询 p50 | 查询 p95 | 命中率(60 问) | 答非所问 | 实际可召回量 |
|---|---|---|---|---|---|---|
| 100 | redis 暴力 | 4.83ms | 5.31ms | 60/60 = **1.00** | 0 | 100/100 |
| 100 | pgvector | 4.84ms | 5.52ms | 60/60 = **1.00** | 0 | 100/100 |
| 1000 | redis 暴力 | 10.02ms | 10.61ms | 6/60 = **0.10** | 0 | **200**/1000(索引容量截断) |
| 1000 | pgvector | 33.22ms | 34.49ms | 60/60 = **1.00** | 0 | 1000/1000 |
| 5000 | redis 暴力 | 9.22ms | 9.95ms | 4/60 = **0.067** | 0 | **200**/5000(索引容量截断) |
| 5000 | pgvector | 158.89ms | 165.90ms | 60/60 = **1.00** | 0 | 5000/5000 |

写入侧:redis 0.39–0.46 ms/条;pgvector 2.03–2.89 ms/条(单行事务 + HNSW 增量维护,贵 ~6 倍)。

## 发现(比数字更重要的是这个)

1. **出厂查询形态触发 Seq Scan,HNSW 索引形同虚设**:`search_pg` 的查询 = `WHERE tenant_id AND model AND ctx ORDER BY embedding <=> v LIMIT 200`,本机 pgvector 0.6.2 在 N=100/1000/5000 三轮 EXPLAIN 全部选择 **Seq Scan**。定位实验:同一张表,去掉过滤的纯 KNN → HNSW 索引扫描(1.5ms);过滤 + LIMIT 1 → 有时走索引;调 `hnsw.ef_search`(200/400)不改变计划。机制:0.6.x 的过滤是索引扫描后的 post-filter,过滤 + 大 LIMIT 下优化器放弃索引(0.8.0 的 iterative index scans 才解决)。**即:pgvector 后端当前出厂形态在大库上是 O(N) 全表余弦,不是 KNN**——N=5000 的 159ms 就是全表扫的账。
2. **redis 暴力后端的命中率上限是 200/N(结构性的,不是调参问题)**:索引 ZSET `zremrangebyrank 0,-201` 按新旧截断,N=1000/5000 时均匀采样的旧条目物理不可召回(0.10/0.067)。延迟反而平缓(9–10ms,扫描宽度恒为 200)。
3. 交叉点判读:**N≤200(且按租户天然分片)区间,暴力扫描全面占优**(延迟持平或更好、写入便宜 6 倍、零 PG 依赖);N>200 后 pgvector 提供暴力扫描给不出的**召回完整性**,但以出厂形态付出 3.3–17 倍查询延迟。

## 建议(按序,均不在本批实施)

- 维持 `semantic_backend=redis` 默认;`semantic_pg` 保持"显式开启才生效"现状。
- 若启用 pgvector:优先升级 pgvector ≥0.8(iterative index scans,过滤进索引扫描)再复测;短期替代改造=查询改"无过滤 KNN + 结果侧租户断言"(语义等价前提:ORDER BY 距离后首个同租户条目即为召回,需论证跨租户条目挤占 top-K 的行为)或把 `max_scan` 降到优化器肯走索引的档位(LIMIT 1 实测可走,但 200 召回上限语义随之变化)——两条都需要独立设计轮与复测,不宜基准日热改。
- redis 后端若要保召回:`semantic.py` 的 200 容量与业务"最近热问"分布吻合时可接受;否则是下一个要立项的边界。

## 诚实边界

- 单机、顺序单飞、mock embedder(词面 bigram):**绝对延迟不代表生产**(真 embedding 走网络,embed 主导);本基准回答的是"两后端检索段的相对伸缩与召回",不是端到端延迟。
- N=5000 以下未测 HNSW 的真实收益(出厂形态没走索引,测不出);pgvector ≥0.8 的行为未验证(本机 0.6.2)。
- 命中率=词面口径命中率;真语义相似下两后端的"答非所问"行为未测(需要真 embedding,踩 mock 红线)。

## 复现

```
.venv/Scripts/python.exe D:\1\1\1Tasks\zcode\shixiproj\.tooling\agentgate-bench\semantic_pg_bench.py
# preflight 自带清场(6391 端口 + agentgate-bench pgdata 残留进程);产物 bench_output.json
```
