from typing import List, Any
from dataclasses import dataclass, field
import json

from pydantic import Field, ConfigDict

from Models.base.custom_pydantic import CustomBaseModel
from Utils.加密.utils import GenWebCookieParams


class BiliWebCookie(CustomBaseModel):
    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_alias=True, validate_by_name=True
    )

    gen_web_cookie_params: GenWebCookieParams = Field(...)
    buvid3: str | None = Field(
        None
    )  # 第一次通过访问网页获取 _render_data_的access_id 做一个jwt解密,获取buvid键名,之后就一直不动了
    b_nut: str | None = Field(None)
    uuid: str | None = Field(None, alias="_uuid")
    at_once: str | None = Field(None, alias="__at_once")
    hit_dyn_v2: str | None = Field(None, alias="hit-dyn-v2")
    enable_web_push: str = Field("DISABLE")
    home_feed_column: str = Field("4")
    browser_resolution: str = Field("")

    buvid4: str | None = Field(None)  # 通过访问spi获取buvid4,响应里的buvid3不用管
    bili_ticket: str | None = Field(None)
    bili_ticket_expires: int | None = Field(None)

    b_lsid: str | None = Field(None)
    buvid_fp: str | None = Field(None)

    def model_post_init(self, context: Any):
        self.buvid_fp = self.gen_web_cookie_params.buvid_fp
        self.uuid = self.gen_web_cookie_params.uuid
        self.browser_resolution = (
            f"{self.gen_web_cookie_params.avail_w}-{self.gen_web_cookie_params.avail_h}"
        )

    def to_str(
        self, include_keys: List["BiliWebCookie.model_fields"] | None = None
    ) -> str:
        """
        确保按照顺序合成cookie字符串
        """
        cookie_dict = self.model_dump(by_alias=True, exclude_none=True)
        order = list(cookie_dict.keys())
        if include_keys is None:
            include_keys = order
        exclude_keys = ["gen_web_cookie_params", "extra_fields"]
        order[:] = [x for x in order if x not in exclude_keys]
        ret = ""
        for key in order:
            if key in include_keys and cookie_dict.get(key):
                ret += f"{key}={cookie_dict.get(key)}; "
        return ret.strip("; ")


@dataclass
class CookieWrapper:
    ck: BiliWebCookie
    ua: str
    expire_ts: int
    #: 命中 -352 的**不同出口前缀个数**（不是命中次数，见 :meth:`note_352`）
    times_352: int = 0
    #: 多少个不同出口前缀被 -352 后判定该 cookie 不可用（可配，见 CONFIG.bili_cookie）
    max_352_prefixes: int = 3
    #: 每个出口前缀命中 -352 的次数（前缀 -> 次数）
    times_352_by_prefix: dict[str, int] = field(default_factory=dict)

    def note_352(self, prefix: str | None) -> int:
        """记录一次 -352，返回累计的「不同前缀数」。

        为什么按前缀去重：-352 是**出口风控**信号，同一个坏出口会让所有经过它的
        cookie 一起被记次数。若按次数累加，一个坏出口连续命中就能把池子里 50 个
        cookie 全部推到 ``times_352 > 10``，池子被整锅废掉；按「不同前缀数」计数后，
        只有**多个不同出口**都对这个 cookie 报 -352 才判定它本身有问题。
        """
        key = prefix or "unknown"
        count = self.times_352_by_prefix.get(key, 0) + 1
        self.times_352_by_prefix[key] = count
        if count == 1:
            self.times_352 += 1
        return self.times_352

    @property
    def able(self) -> bool:
        return self.times_352 < max(1, self.max_352_prefixes)

    # ---- Redis 持久化序列化（跨进程 / 重启复用） ----
    def to_record(self) -> dict:
        """转成可 JSON 序列化的记录（cookie 字段 + 风控计数）。"""
        return {
            "ck": json.loads(self.ck.model_dump_json(by_alias=True)),
            "ua": self.ua,
            "expire_ts": self.expire_ts,
            "times_352": self.times_352,
            "max_352_prefixes": self.max_352_prefixes,
            "times_352_by_prefix": dict(self.times_352_by_prefix),
        }

    @classmethod
    def from_record(cls, data: dict) -> "CookieWrapper":
        """从 Redis 记录还原。

        ``GenWebCookieParams.model_post_init`` 会重新生成 payload / buvid_fp
        （内部含随机 canvas 串），因此这里用序列化时保存的 ``buvid_fp`` 覆盖回去，
        保证「同一个 cookie 反序列化后指纹不变」，否则服务端会看到同一 buvid
        对应两套 fp。
        """
        ck_payload = dict(data["ck"])
        params_payload = dict(ck_payload.get("gen_web_cookie_params") or {})
        saved_buvid_fp = params_payload.get("buvid_fp")
        # 兼容旧记录：历史指纹库可能把 int/str 字段写成 null
        for key, default in (
            ("deviceMemory", 8),
            ("CPUCoreNum", 8),
            ("renderer", ""),
            ("vendor", ""),
            ("renderer_id", ""),
        ):
            if params_payload.get(key) is None:
                params_payload[key] = default
        ck_payload["gen_web_cookie_params"] = params_payload
        ck = BiliWebCookie.model_validate(ck_payload)
        if saved_buvid_fp:
            ck.gen_web_cookie_params.buvid_fp = saved_buvid_fp
            ck.buvid_fp = saved_buvid_fp
        return cls(
            ck=ck,
            ua=data.get("ua") or ck.gen_web_cookie_params.ua,
            expire_ts=int(data.get("expire_ts") or 0),
            times_352=int(data.get("times_352") or 0),
            max_352_prefixes=int(data.get("max_352_prefixes") or 3),
            times_352_by_prefix=dict(data.get("times_352_by_prefix") or {}),
        )


if __name__ == "__main__":
    ck = BiliWebCookie(
        gen_web_cookie_params=GenWebCookieParams(
            buvid_fp="buvid_fp",
            uuid="uuid",
            avail_w=1920,
            avail_h=1080,
            window_h=1080,
            window_w=1920,
            ua="test-ua",
        ),
        buvid3="buvid3",
        b_nut="1",
        at_once="at_once",
    )
    print(ck.to_str())
    print(ck.to_str())
