"""OpenAI 兼容协议模型(网关的地基:统一入站协议,协议转换发生在渠道层)。

对齐 OpenAI Chat Completions API 1.x;extra="allow" 保留未知字段原样透传,
避免网关因上游新增参数而升级——这是代理类产品的基本纪律。
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "tool", "developer"]
    content: str | list[dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None


class ChatCompletionRequest(BaseModel):
    model_config = ConfigDict(extra="allow", protected_namespaces=())

    model: str
    messages: list[ChatMessage] = Field(min_length=1)
    stream: bool = False
    stream_options: dict[str, Any] | None = None
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    tools: list[dict[str, Any]] | None = None
    user: str | None = None

    def upstream_payload(self) -> dict[str, Any]:
        """还原为上游 payload:已知字段 + 未知 extras,丢弃 None(不改变上游默认语义)。"""
        data = self.model_dump(exclude_none=True)
        return data


class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChatCompletionChoice(BaseModel):
    index: int = 0
    message: ChatMessage | None = None
    delta: dict[str, Any] | None = None
    finish_reason: str | None = None


class ChatCompletionResponse(BaseModel):
    id: str
    object: Literal["chat.completion"] = "chat.completion"
    created: int
    model: str
    choices: list[ChatCompletionChoice]
    usage: Usage | None = None


class ErrorEnvelope(BaseModel):
    """网关错误信封(所有非透传错误统一此结构,含 request_id 便于排障)。"""

    message: str
    type: str
    code: str | None = None
    request_id: str | None = None


class GatewayErrorBody(BaseModel):
    error: ErrorEnvelope
