# 生态实接:agentgate 管理 PolicyCompass(真实多进程)

- 时间:2026-11-02 01:51:17
- 链路:OpenAI 格式请求 → agentgate(鉴权/Lua 准入/记账)→ shim(协议映射)→ PolicyCompass /chat(CRAG,mock LLM)

## Q: 上海公积金贷款额度是多少?
- 状态 200 · 渠道 policy-compass · 延迟 937ms · usage={'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}
- A(截断):根据现行政策文件，为您整理如下：

一、最高贷款额度
（一）首套住房：个人公积金最高贷款额度调整为100万元，家庭公积金最高贷款额度调整为200万元；缴交补充公积金的，个人公积金最高贷款额度增加20万元，家庭公积金最高贷款额度增加40万元，…
## Q: 公积金断缴有什么影响?
- 状态 200 · 渠道 policy-compass · 延迟 889ms · usage={'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0}
- A(截断):根据现行政策文件，为您整理如下：
第十一条 住房公积金管理中心履行下列职责：
(一)编制、执行住房公积金的归集、使用计划；
(二)负责记载职工住房公积金的缴存、提取、使用等情况；
(三)负责住房公积金的核算；
(四)审批住房公积金的提取、使…
## 账本对账:2 行 · 总 token 0(请求路径零 DB 写,后台异步落库)

**判定:自家网关管理自家 Agent 成立** —— 租户/key/限流/计费/监控统一收口,
Agent 应用(AskBI/PolicyCompass)经 ~60 行 shim 即可接入,应用零改动。