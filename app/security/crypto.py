"""渠道密钥静态加密(Fernet 对称加密,主密钥来自环境变量)。

设计(P2 hardening:`channels.api_key` 原为明文入库,是安全拷打第一戳):
- 主密钥 `AGENTGATE_CHANNEL_ENC_KEY`(Fernet key,`cryptography.fernet.Fernet.generate_key()`
  生成);设置后写入即加密、读出即解密;
- 未设置(dev 口径):明文存储 + 启动告警 —— 不假装安全;prod 环境由 Settings
  校验器强制要求(与 admin_token 同款 fail-closed);
- 密文带版本前缀 `enc:v1:`,旧明文行(加密功能上线前的存量)读取时原样返回
  —— 向后兼容,不做破坏性迁移;下次经 admin 重新注册即落密文;
- 主密钥轮换/解密失败:宁可让该渠道路由失败(状态机冷却),不静默用错误密钥打上游。

日志纪律:明文密钥不落任何日志(本模块不 logger 密钥,调用方同样禁止)。
"""

from cryptography.fernet import Fernet, InvalidToken

_PREFIX = "enc:v1:"
_cipher: Fernet | None = None


def init_cipher(key: str | None) -> None:
    """进程启动时注入主密钥(app 工厂调用;None=明文口径)。"""
    global _cipher
    _cipher = Fernet(key.encode()) if key else None


def encrypt_secret(plain: str) -> str:
    """入库前调用:有主密钥→密文(带前缀);无→原样(dev 口径)。"""
    if not plain:
        return plain
    if _cipher is None:
        return plain
    return _PREFIX + _cipher.encrypt(plain.encode()).decode()


def decrypt_secret(stored: str) -> str:
    """读库后调用:带前缀→解密;无前缀→原样(存量明文兼容)。

    解密失败(主密钥轮换/密文损坏)抛 InvalidToken —— 让该渠道注册失败或
    路由失败,不返回垃圾密钥。
    """
    if not stored:
        return stored
    if not stored.startswith(_PREFIX):
        return stored
    if _cipher is None:
        raise InvalidToken("channel secret is encrypted but no master key configured")
    return _cipher.decrypt(stored[len(_PREFIX):].encode()).decode()


def is_encrypted(stored: str) -> bool:
    return bool(stored) and stored.startswith(_PREFIX)


def generate_key() -> str:
    """生成新主密钥(运维辅助:AGENTGATE_CHANNEL_ENC_KEY 用)。"""
    return Fernet.generate_key().decode()


__all__ = [
    "InvalidToken",
    "decrypt_secret",
    "encrypt_secret",
    "generate_key",
    "init_cipher",
    "is_encrypted",
]
