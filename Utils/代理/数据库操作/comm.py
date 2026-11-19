import ipaddress
import re

from CONFIG import CONFIG
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab

#: 每条代理都要跑一次正则，编译一次即可（原先 2000 条/批里会 compile 2000 次）
#: IPv6 必须是方括号形式（``[2001:db8::1]:8080``），裸 IPv6 与端口无法区分，不接受。
_PROXY_IP_PATTERN = re.compile(
    r"^(?:(?P<scheme>http|https|socks[45]|sock[45])://)?"  # 协议部分（可选）
    r"(?P<host>\[[0-9a-fA-F:.]+\]|(?:[0-9]{1,3}\.){3}[0-9]{1,3})"  # IPv4 / 方括号 IPv6
    r"(?::(?P<port>[0-9]{1,5}))?$"  # 端口号部分（可选）
)


def _allow_ipv6_pool() -> bool:
    """IPv6 代理池开关（默认关闭，保持历史行为）。"""
    return bool(CONFIG.proxy_health.allow_ipv6_pool)


def get_scheme_ip_port_form_proxy_dict(proxy_info_dict: ProxyTab.proxy) -> str | None:
    if not proxy_info_dict:
        return None
    return list(proxy_info_dict.values())[0]


def format_proxy(
    proxy_str, protocol: str = None, allow_ipv6: bool | None = None
) -> dict | None:
    """
    将传入的代理字符串标准化为 {'protocol1': 'address1', 'protocol2': 'address2'} 的形式。

    :param protocol: http https sock4 sock5 socks4 socks5
    :param proxy_str: 代理字符串，可能是IP地址或带有协议前缀的完整URL
    :param allow_ipv6: 是否接受方括号 IPv6（None = 跟随 CONFIG.proxy_health.allow_ipv6_pool）
    :return: 标准化的代理字典或None（如果输入不符合预期格式）
    """
    if type(proxy_str) is str:
        proxy_str = proxy_str.strip()

    match = _PROXY_IP_PATTERN.match(proxy_str)
    if not match:
        return None
    protocol_in_input = match.group("scheme")  # 输入中的协议部分，可能为None
    host = match.group("host")
    port = match.group("port")
    if host.startswith("["):
        if allow_ipv6 is None:
            allow_ipv6 = _allow_ipv6_pool()
        if not allow_ipv6:
            return None
        try:
            # 正则只做形状匹配，这里再校验一次地址合法性（避免 [999::gg] 之类入库）
            ipaddress.IPv6Address(host[1:-1])
        except ValueError:
            return None
    port_suffix = f":{port}" if port else ":80"  # 如果没有提供端口，默认使用80
    # 构建基础的代理地址
    base_protocol = protocol_in_input or protocol or "http"
    url = f"{base_protocol}://{host}{port_suffix}"
    # 创建代理字典
    if base_protocol in ["http", "https"]:
        return {"http": url, "https": url}
    if base_protocol in ["sock5", "socks5"]:
        return {"sock5": url, "socks5": url}
    if base_protocol in ["sock4", "socks4"]:
        return {"sock4": url, "socks4": url}
    return None
