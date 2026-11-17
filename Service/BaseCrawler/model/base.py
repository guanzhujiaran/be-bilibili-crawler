from bili_common.core import IntEnumAutoDoc
from typing import TypeVar, Generic
from datetime import datetime
from pydantic import Field
from Models.base.custom_pydantic import CustomGenericModel, CustomBaseModelHashable

ParamsType = TypeVar("ParamsType", bound=CustomBaseModelHashable)


class WorkerStatus(IntEnumAutoDoc):
    # region 成功的代码
    complete = 1
    nullData = 2
    # endregion

    pending = 3
    fail = 4
    timeoutError = 5


class WorkerModel(CustomGenericModel, Generic[ParamsType]):
    params: ParamsType | None = None
    seqId: int = Field(..., description="任务序号（自增）从0开始")
    fetchStatus: WorkerStatus = Field(WorkerStatus.pending)
    created_at: datetime = Field(default_factory=datetime.now, description="创建时间")
    updated_at: datetime = Field(default_factory=datetime.now, description="更新时间")
    retry_count: int = Field(0, description="重试次数")
    # 最近一次取数失败是否属于「代理池整体不可用」（判定见 resilience.is_pool_level_error）：
    # 池级失败要按「至少一次」重入队等待代理恢复，不能被当成业务失败丢弃。
    pool_level_fail: bool = Field(
        default=False, description="最近一次取数失败是否属于代理池级失败"
    )
    # 本任务因熔断等待代理恢复的次数（轮次级等待上限用，见
    # CrawlerConfig.pool_unavailable_max_waits）。
    pool_wait_count: int = Field(
        default=0, description="本任务因代理池不可用已等待的次数"
    )

    @property
    def fetchStatusStr(self) -> str:
        """将 WorkerStatus 枚举转换为字符串"""
        for i in WorkerStatus:
            if i.value == self.fetchStatus:
                return str(i)
        return "unknown"

    def __hash__(self) -> int:
        return hash((self.seqId or 0, str(self.params)))

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        if name != "updated_at" and name in self.__class__.model_fields:
            super().__setattr__("updated_at", datetime.now())
