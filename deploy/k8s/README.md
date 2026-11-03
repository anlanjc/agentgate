# agentgate · Kubernetes 清单(P4-3)

> **诚实边界:本清单未在真实集群实跑**(本机无 k8s;Docker 也未就绪)。
> YAML 经语法校验 + 字段对照 k8s v1.29 API 书写;首次 apply 前按集群实际版本核对。

## 文件

| 文件 | 内容 |
|---|---|
| `configmap.yaml` | 非敏感 env(上游地址/OTel 开关等) |
| `secret.yaml` | 敏感 env 模板(admin token/渠道加密主密钥/上游 key)——**生产应换 ExternalSecrets/Vault,明文 Secret 仅演示** |
| `deployment.yaml` | 网关 2 副本:/health 存活 + /ready 就绪探针、资源请求/限制、非敏感配置从 ConfigMap 注入 |
| `service.yaml` | ClusterIP 8100 |
| `hpa.yaml` | HPA v2:CPU 70% 目标,2~10 副本(共享状态全在 Redis,扩副本安全——README 硬点 #2) |

## 未包含(有意,非遗漏)

- **PostgreSQL / Redis**:生产用托管服务(RDS/云 Redis);若要 in-cluster 自建,
  参考 `deploy/docker-compose.yml` 的参数(AOF/健康检查)转 StatefulSet——
  账本/任务流的持久化语义必须在存储层兜底,模板给错了比不给更危险。
- Ingress/TLS:集群网关形态各异(nginx/云 LB),按环境补。
- PodDisruptionBudget/NetworkPolicy:PRODUCTION_GAP B 档,按集群成熟度触发。

## apply 顺序

```bash
kubectl apply -f secret.yaml -f configmap.yaml
kubectl apply -f deployment.yaml -f service.yaml -f hpa.yaml
kubectl rollout status deployment/agentgate
```

## 迁移

容器 CMD 自带 `alembic upgrade head`(见 Dockerfile)——多副本并发跑迁移由
Alembic 事务性 DDL + 版本表兜底;更严谨的多副本锁是 backlog(PG advisory lock)。
