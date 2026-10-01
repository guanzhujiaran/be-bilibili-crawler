# -*- coding: utf-8 -*-
"""设备池：把「一台设备」（:class:`~.device_env.DeviceEnv`）持久化到 Redis，实现复用。

策略：

- **复用**：设备（buvid / 指纹 / UA / region 缓存）落 Redis，下次优先取出来接着用，
  而不是每次都造一台新设备；
- **上限**：池子最多保留 ``max_devices`` 台，超出时按 LRU（最久未使用）淘汰；
- **剔除**：服务端返回 -352 / -412 之类的异常时，把该设备从池里删掉，不再复用。

存储结构（Redis db 取 ``CONFIG.database.commStorageRedis``）：

- ``bili:device:{buvid}``  string，设备 JSON；
- ``bili:devices:lru``     zset，member=buvid，score=最后使用时间戳（淘汰依据）。
"""

import json
import random
import time

from CONFIG import CONFIG
from log.base_log import BiliGrpcApi_logger
from Utils.GrpcUtils.metadata.device_env import DeviceEnv, new_device_env
from Utils.redisTool.RedisManager import RedisManagerBase

DEVICE_KEY_PREFIX = "bili:device:"
DEVICE_LRU_KEY = "bili:devices:lru"
# 池子上限：超过就按 LRU 淘汰最久未使用的设备
DEFAULT_MAX_DEVICES = 500


class DevicePool(RedisManagerBase):
    """设备池（Redis 后端）。"""

    def __init__(self, max_devices: int = DEFAULT_MAX_DEVICES, **kwargs):
        redis_conf = CONFIG.database.commStorageRedis
        kwargs.setdefault("host", redis_conf.host)
        kwargs.setdefault("port", redis_conf.port)
        kwargs.setdefault("db", redis_conf.db)
        kwargs.setdefault("pwd", redis_conf.pwd)
        super().__init__(**kwargs)
        self.max_devices = max_devices

    # 基础读写 -----------------------------------------------------------
    @staticmethod
    def _key(buvid: str) -> str:
        return f"{DEVICE_KEY_PREFIX}{buvid}"

    async def save(self, device: DeviceEnv, trim: bool = True) -> None:
        """保存设备并更新其"最后使用时间"；必要时按上限淘汰。"""
        await self._set(self._key(device.buvid), json.dumps(device.to_dict()))
        await self._zadd(DEVICE_LRU_KEY, {device.buvid: int(time.time())})
        if trim:
            await self.trim()

    async def get(self, buvid: str) -> DeviceEnv | None:
        """按 buvid 取设备；不存在或数据损坏时返回 None（损坏的顺手删掉）。"""
        raw = await self._get(self._key(buvid))
        if not raw:
            return None
        try:
            return DeviceEnv.from_dict(json.loads(raw))
        except (ValueError, TypeError) as e:
            BiliGrpcApi_logger.error(f"设备池数据损坏，已剔除 {buvid}：{type(e).__name__} {e}")
            await self.drop(buvid)
            return None

    async def touch(self, device: DeviceEnv) -> None:
        """更新设备的"最后使用时间"（LRU 依据）。"""
        await self._zadd(DEVICE_LRU_KEY, {device.buvid: int(time.time())})

    async def drop(self, device: DeviceEnv | str) -> None:
        """把设备从池里剔除（服务端报 -352 之类异常时调用）。"""
        buvid = device.buvid if isinstance(device, DeviceEnv) else device
        await self._delete(self._key(buvid))
        await self._zrem(DEVICE_LRU_KEY, buvid)

    async def size(self) -> int:
        return int(await self._zcard(DEVICE_LRU_KEY) or 0)

    # 池子维护 -----------------------------------------------------------
    async def trim(self) -> int:
        """按上限淘汰最久未使用的设备，返回淘汰数量。"""
        removed = 0
        while await self.size() > self.max_devices:
            oldest = await self._zget_range(DEVICE_LRU_KEY, 0, 0)
            if not oldest:
                break
            await self.drop(oldest[0])
            removed += 1
        if removed:
            BiliGrpcApi_logger.info(
                f"设备池超过上限 {self.max_devices}，按 LRU 淘汰 {removed} 台"
            )
        return removed

    async def clear(self) -> int:
        """清空设备池（调试用）。"""
        buvids = await self._zget_range(DEVICE_LRU_KEY, 0, -1)
        for buvid in buvids:
            await self.drop(buvid)
        return len(buvids)

    # 取设备 -------------------------------------------------------------
    async def acquire(
        self,
        brand: str = "OnePlus",
        Dalvik: str | None = None,
        version_name: str = "9.13.0",
        build: int = 9130500,
        channel: str | None = None,
    ) -> DeviceEnv:
        """优先从池里随机复用一台；池空（或取到的数据坏了）则新建一台并入池。"""
        while True:
            buvid = await self._zrandmember(DEVICE_LRU_KEY, count=1)
            if isinstance(buvid, (list, tuple)):
                buvid = buvid[0] if buvid else None
            if not buvid:
                break
            device = await self.get(buvid)
            if device:
                await self.touch(device)
                return device
        device = new_device_env(
            Dalvik=Dalvik,
            version_name=version_name,
            build=build,
            channel=channel,
            brand=brand,
        )
        await self.save(device)
        BiliGrpcApi_logger.debug(
            f"设备池新建设备 {device.buvid}（当前 {await self.size()} 台）"
        )
        return device


# 全局单例（懒加载，避免 import 期就连 Redis）
_pool: DevicePool | None = None


def get_pool() -> DevicePool:
    global _pool
    if _pool is None:
        _pool = DevicePool()
    return _pool
