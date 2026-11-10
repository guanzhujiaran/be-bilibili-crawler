"""轻量健康检查路由（crawler 角色与 query 角色共用）。

背景：网关 ``be-gateway`` 的启动探活原先直接 ``GET <crawler 根路径>``，而
crawler / query 都没有根路由，FastAPI 每次都回 404 —— 网关那边「有 HTTP 响应
即视为存活」照样通过，但 crawler 日志里会周期性刷
``WARNING ... 404 Not Found [GET /]`` 与访问日志，掩盖真正的告警。

本路由只回答「进程还在、事件循环还在转」：

- 不查数据库、不连 MQ、不碰 Milvus；
- 依赖挂了它依然返回 200（探活的目的只是「容器有没有活着」，
  业务可用性由各自的接口自己报错）；
- 响应体极简（``{"status": "ok"}``），不走 ``{code, msg, data}`` 业务封装：
  ``bili_common.middlewares.error_status`` 已把 ``/health`` 加进跳过列表。
"""

from fastapi import APIRouter

from ApiRoutes import RouterTags

#: 探活路径（网关上游探活与容器健康检查都指向它）
HEALTH_PATH = "/health"

router = APIRouter(tags=[RouterTags.COMMON])


@router.get(
    HEALTH_PATH,
    name="health_check",
    description="轻量健康检查：不查库、不连 MQ，仅表示进程存活（200 + 极简 JSON）",
)
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
