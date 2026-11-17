# pgvector 0.8 升级路径实验(2026-11-18,[1B4-AG] 任务一)

**结论先行:维持 `semantic_backend=redis` 默认不变;若启用 pgvector,升级 ≥0.8 优于查询形态改造——0.8.7 在同机同 PG 16.2 下无需改一行查询,优化器即改走精确路径(LIMIT 200 恰好填满),并附带距离内核/写入 ~1.2-3.4x 提速。同时本轮修正了 11-09 挂账基准的一个关键归因:0.6.2 的"Seq Scan"不是版本必然,是统计信息陈旧的计划 regime;0.6.2 真实的问题是 generic plan 下租户过滤选 HNSW 后过滤,LIMIT 200 只填 7-17%(截断),top-1 口径侥幸无损但 top-K 召回结构性破。产线决策不热改(本批零生产代码变更),启用 pgvector 的决策留主人批。**

## 装置(与 11-11 基准的关键差异:真 A/B 双实例)

- **OLD :61555** = 仓库 venv pginstall 常驻实例(PG 16.2 + **vector 0.6.2**,ECO 轮遗留,复用);
- **NEW :61556** = pginstall 副本(`.tooling/agentgate-pg08/pginstall`,不动 venv 原件——61555 与测试套件共用同一 pginstall,原位升级会波及测试口径)+ **源码编译 pgvector 0.8.7**:GitHub v0.8.7 tarball,gcc 8.1.0(MinGW-W64,与构建该 PG 的 12.2.0 同族工具链,纯 C 扩展 ABI 兼容),19 个 .c 编译链接 `libpostgres.a` 得 vector.dll(274KB),SQL/control 装入副本 share/extension;独立 initdb pgdata。
- 同机双实例、同 PG 16.2、同 initdb 缺省配置,唯一变量 = pgvector 版本。`CREATE EXTENSION` 后 NEW 报 `extversion=0.8.7`,`hnsw.iterative_scan` GUC 在位(0.6.2 无此 GUC,SET 直接报 `reserved prefix`——版本判别点)。
- 工作负载沿用 11-11 口径:HashEmbedder(dim=128) mock embedder(禁真模型红线)、条目语料同生成器、种子 42、K=60、阈值 0.75、命中后校验答案防答非所问;多租户探针 = 目标租户 t-a + 4 个同池伪词重排的异租户(词面距离贴近,挤占 top-K 的真实形态)。
- 脚本 `.tooling/agentgate-pg08/pgv08_experiment.py / pgv08_followup.py / pgv08_probe2.py`(装置不入库,chaos_matrix 先例),产物 `exp_output.json / followup_output.json / probe2_output.json / generic_plan_output.json` 同目录。

## 发现一:11-11 的"Seq Scan"归因修正——那是统计信息 regime,不是 0.6.2 的版本必然

11-11 基准的 N=100/1000/5000 延迟 4.84/33.2/158.9ms 严格线性于 N(O(N) 全表扫描特征),本轮在 OLD 实例**复现了同一 regime**(N=1000 p50=36.8ms ≈ 33.2ms;N=100 p50=4.46ms ≈ 4.84ms)。但 N=5000 本轮只测得 **1.26ms**(差 100 倍)——EXPLAIN (ANALYZE, BUFFERS) 三态定位:

| 统计信息状态 | 计划(0.6.2 与 0.8.7 同) | N=5000 延迟 |
|---|---|---|
| 陈旧(TRUNCATE→灌数→立刻查,autovacuum/ANALYZE 未及跑) | **Seq Scan + Sort**(O(N)) | ~159ms(11-11 实测)/ 线性外推 |
| 新鲜(ANALYZE 后),过滤弱选择性(单租户全表匹配) | btree(tenant,model,ctx) Index Scan + top-N Sort | 0.6-2.8ms |
| 新鲜,过滤选择性中等/高 | 见发现二(两版本分叉) | 0.4-0.9ms |

机制:11-11 的实验形态是"TRUNCATE→store_pg 灌 N 条(~11s)→立刻查",N≤1000 时 autovacuum(默认 naptime 60s)来不及 analyze,优化器按陈旧统计(近似空表)选 Seq Scan,实际扫 N 行;N=5000 灌数期 autovacuum 已跑 → 计划翻转 → 快。**11-11 的绝对数字是真实的,但"出厂查询形态必然 Seq Scan、HNSW 形同虚设"的版本归因不成立——换 ANALYZE 时机结论即变。** 另:11-11 的 EXPLAIN 用了 `ORDER BY embedding <=> embedding`(列对列),排序键不匹配索引序,结构上就不可能出 HNSW 计划——EXPLAIN 方法本身也失真。本轮 EXPLAIN 全部带真实参数向量。

## 发现二:generic plan(生产真实 regime)下两版本计划分叉——这才是升级的实际收益

生产 `search_pg` 走 SQLAlchemy→asyncpg 预编译语句,**第 6 次执行起切 generic plan**——这才是产线常驻 regime。ANALYZE 新鲜统计 + 5 租户探针(异租户条目词面贴近),SQL 级 PREPARE→6×EXECUTE→EXPLAIN EXECUTE(取到的就是 generic plan):

| 场景 | 0.6.2 generic plan | 0.8.7 generic plan |
|---|---|---|
| t-a 占 2%(100/5000) | **HNSW Index Scan + 后过滤**:LIMIT 200 实际返回 **14.2 行**(均值,截断 93%);p50 0.72ms;60 问 top-1 59/60(**观测到 1 例截断致 top-1 丢失**) | **btree Bitmap Heap Scan + Sort**:返回 **100/100 恰满**;p50 0.59ms;top-1 60/60 |
| t-a 占 20%(1000/5000) | **HNSW 后过滤**:返回 **34.6 行**(截断 83%);p50 0.59ms;top-1 60/60 | **btree Bitmap Heap Scan**:返回 **200/200 恰满**;p50 0.85ms;top-1 60/60 |

- 0.6.2 优化器低估 HNSW+后过滤的代价(0.6.x 无迭代扫描概念,代价模型以为 LIMIT 200 很快),选了 HNSW 却填不满 LIMIT——`ef_search=40` 的访问预算内能通过租户过滤的行就那么多。**top-1 口径(生产 search_pg 只取 rows[0] 阈值判)在本语料基本无损(59-60/60),但这是"目标条目词面相似度高、天然排访问窗内"的侥幸;弱相似目标/top-K 召回下 0.6.2 的截断是结构性破洞。**
- 0.6.2 的两个缓解实测:①`SET hnsw.ef_search=400` → 填满 200 行,p50 2.05ms(可用的应急旋钮);②partial HNSW 索引(`WHERE tenant_id=...`,官方 pre-0.8 推荐)→ 优化器选择性场景肯走,但同样受 ef 截断(27 行)。
- **0.8.7 不需要任何查询改动**:0.8 的代价估计修正后,优化器在这种选择性下自己改走 btree 精确路径,行数恰满。**`hnsw.iterative_scan` 在本实验的表规模/选择性组合下没有被选中(btree 更优),未测出独立收益——如实记录;它的收益场景(必须走 HNSW 且选择性极高)留待更大规模验证。**

## 发现三:逐形态 EXPLAIN + 延迟矩阵(N=5000 单租户,新鲜统计)

| 形态 | 0.6.2 计划 | 0.6.2 p50 | 0.8.7 计划 | 0.8.7 p50 |
|---|---|---|---|---|
| S0 出厂生产形态(过滤+created_at+ORDER BY<=>+LIMIT 200) | btree+Sort | 2.11ms | btree+Sort | 2.11ms |
| S1 纯 KNN 无过滤 | HNSW 索引 | 0.68ms | HNSW 索引 | 0.51ms |
| S2 降 LIMIT(1/10/50) | btree+Sort | 1.87-2.36ms | btree+Sort | 2.23-2.35ms |
| S3 过滤外提(子查询内层纯 KNN LIMIT k→外层过滤) | **HNSW 索引** | **k200:0.57 / k1000:0.87ms** | **HNSW 索引** | k200:0.54 / k1000:3.54ms |
| S4 partial HNSW 索引(单租户全匹配态) | btree+Sort(不用) | 2.14ms | **partial_hnsw 扫描** | 2.02ms |
| S5/S6 iterative_scan relaxed/strict(0.8 only) | — | — | 同 S0(btree) | 1.95-2.24ms |
| 同计划(btree+Sort)纯内核差 | | 0.61-0.73ms | | **0.17-0.23ms(~3.4x)** |

- **S3(过滤外提)是 0.6.2 上唯一能让 HNSW 真正走的查询改造**,k=200 时最快(0.54-0.57ms);但语义代价=召回依赖"内层 k 覆盖",跨租户探针实测 60 问 0 答非所问(目标条目天然近邻)——该保证是语料性的,非结构性的,启用需独立论证。
- 同 plan 同数据下 0.8.7 的距离计算/排序内核快 ~3.4x(btree+Sort 全匹配态 0.17 vs 0.61ms),写入侧 store_pg p50 1.24-1.81ms vs 1.76-2.18ms(~1.2-1.4x)。

## 发现四:生产形态 N=100/1000/5000(与 11-11 同口径直接对比,含 regime 注记)

| N | 11-11 基准(0.6.2,stale 态) | 本轮 OLD 0.6.2 | 本轮 NEW 0.8.7 |
|---|---|---|---|
| 100 | 4.84ms(命中 1.00) | 4.46ms(命中 1.00) | 4.15ms(命中 1.00) |
| 1000 | 33.22ms(命中 1.00) | 36.85ms* | 1.72ms(命中 1.00) |
| 5000 | 158.89ms(命中 1.00) | 1.26ms* | 1.26ms(命中 1.00) |

\* = regime 产物,非版本差:N=1000 的 36.85 是 stale 态 Seq Scan(与 11-11 同款),N=5000 的 1.26 是灌数期 autovacuum 已跑、计划已翻转到索引态。同状态复刻实验(store_pg 灌 1000 条后立刻查,间隔足够让 autovacuum 跑完)0.6.2 稳定测得 **2.29ms**(HNSW 后过滤计划)。诚实声明:逐 N 的计划 regime 受 autovacuum 时机影响存在 run-to-run 抖动,跨版本同 N 对比只在同 plan 同统计态下有效(发现二/三的表格);redis 暴力后端对照口径见 11-11 基准(9-10ms,N>200 命中率崩塌 200/N)。

## 决策建议(三选一,产线本轮零改动)

**兼容性回归:生产代码本体在 0.8.7 实例上全绿**——`test_m5_semantic_pg.py + test_m5_cache.py + test_m5_semantic.py` 18 passed(AGENTGATE_TEST_PG_URL 指 61556 实例,含 GATE-5 cleanup_expired/DLQ 回归),升级零代码改动的结论有测试背书。

1. **维持 `semantic_backend=redis`(现行,建议维持)**:N≤200 且按租户天然分片时暴力扫描全面占优(延迟持平、写入便宜、零 PG 依赖、无计划 regime 抖动)——11-11 结论不变,本轮新证据(两版本计划都受统计信息时机影响)反而加强了"别为检索段引入 PG"的判断。
2. **若启用 pgvector:升级 ≥0.8,不改查询**(本轮推荐路径):0.8.7 无需一行查询改动即消除 0.6.2 的截断破洞(优化器自动改精确路径),内核/写入附带提速;升余量:实验实例已验证 0.8.7 在本机 PG 16.2 源码编译可行(mingw,装置留 `.tooling/agentgate-pg08/`)。上线形态建议 Docker 官方镜像 pgvector/pgvector(带 -march 优化的发布构建),不推荐自编产物进产线。
3. **查询改造(不推荐作为长期方案)**:S3 过滤外提在 0.6.2 上让 HNSW 可用且最快,但召回保证是语料性的;ef_search=400 应急可用;均不如升级一次到位。

与 11-09"基准日不改产线"的差别:本轮是受控实验(独立实例、实验轮可自由改),**产线决策仍不热改**——`semantic_backend=pgvector` 的启用与 0.8 升级属主人批的产线决策,本实验提供决策依据。

## 诚实边界

- 本机 0.8.7 为源码编译(gcc 8.1.0,无 -march=native,发布镜像有 SIMD 编译优化)——内核差 3.4x 在产线镜像上可能更大,方向可信、倍数保守;
- iterative_scan 未在"必须走 HNSW"的极端选择性下测出独立收益(本实验选择性最低 2% 时 btree 已胜)——0.8 README 声称的收益场景未在本轮复现,不做超实验外推;
- 词面 mock embedder:命中率/top-1 是词面口径,真语义相似度下截断的实际影响需真 embedding 复测(踩 mock 红线,不做);
- 装置遗留:61555/61556 实验后已停/已清(见 PROGRESS 当轮登记);`agentgate-pg08/pginstall` 副本与 pgdata 为实验装置,不入库不入产线。

## 复现

```
# 实例:61555=venv pginstall 常驻(0.6.2);61556=.tooling/agentgate-pg08/pginstall 副本(pgvector 0.8.7)
# 编译:curl -L github.com/pgvector/pgvector/archive/refs/tags/v0.8.7.tar.gz | tar xz
#   gcc -O2 -ftree-vectorize -fassociative-math -fno-signed-zeros -fno-trapping-math -ffp-contract=fast \
#     -I<pginstall>/include/postgresql/server -I<pginstall>/include/postgresql/server/port/win32 -Isrc -c src/*.c
#   gcc -shared -o vector.dll objs/*.o <pginstall>/lib/libpostgres.a
.venv/Scripts/python.exe D:\1\1\1Tasks\zcode\shixiproj\.tooling\agentgate-pg08\pgv08_experiment.py
.venv/Scripts/python.exe D:\1\1\1Tasks\zcode\shixiproj\.tooling\agentgate-pg08\pgv08_followup.py
.venv/Scripts/python.exe D:\1\1\1Tasks\zcode\shixiproj\.tooling\agentgate-pg08\pgv08_probe2.py
```
