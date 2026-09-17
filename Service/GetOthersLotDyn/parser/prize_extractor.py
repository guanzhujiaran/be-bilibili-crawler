"""基于云端 LLM + LangChain 的抽奖信息提取器

使用 ChatOpenAI.with_structured_output() 进行结构化信息提取，LLM 直接返回 Pydantic 模型。

提供两个入口函数，分别对应 biliopusdb 和 dyndetail 两个数据库的 t_lot_extra_info 需求:
- extract_prize_info_for_biliopusdb() → 适用于普通/预约抽奖 (ref_id + lot_type)
- extract_prize_info_for_lotdata()     → 适用于官方/充电抽奖 (lottery_id)

核心特性：
- with_structured_output 驱动 LLM 调用，无需 agent 层
- 仅使用云端 LLM（get_all_free_llms），不再使用本地大模型进行抽奖判断
- 无回退：依次尝试所有已配置的免费(云端) LLM，全部失败时直接抛错，
  不进行任何正则/本地回退；由调用方跳过保存（留空），等待手动脚本
  judge_grand_prize 回填大奖判断结果
- 采样参数通过 get_all_free_llms() 关键字参数传入
"""

from typing import TypeVar, Generic

from langchain_openai import ChatOpenAI
import asyncio
import json
import time
import re
import traceback
from datetime import datetime
import opencc
from loguru import logger
from pydantic import BaseModel, Field, ValidationError
from Models.MQ.PrizeExtractResult import PrizeExtractResult, OfficialPrizeExtractResult
from Service.llm_service import (
    AllLLMsDisabledError,
    SamplingPreset,
    get_all_free_llms,
    get_llm_stats,
)
from Utils.推送.PushMe import a_push_error

T = TypeVar("T")
# _do_extract 的结果模型类型：随调用入口在两种结果模型间切换
_TResult = TypeVar(
    "_TResult", bound=PrizeExtractResult | OfficialPrizeExtractResult
)
# 繁体转简体转换器（线程安全，可全局复用）
_t2s_converter = opencc.OpenCC("t2s.json")

# 全部 LLM 均失败时的等比退避重试参数
# 等待时间按等比数列（公比 _RETRY_DELAY_FACTOR）递增，到 _RETRY_MAX_DELAY 后维持上限，
# 持续重试直至某一次 LLM 调用成功为止，不再“放弃并跳过保存”。
_RETRY_BASE_DELAY = 10  # 首次重试等待（秒）
_RETRY_DELAY_FACTOR = 2  # 等比数列公比
_RETRY_MAX_DELAY = 600  # 重试等待上限（秒）


class PrizeExtractResp(BaseModel, Generic[T]):
    """抽奖信息提取返回内容（result 的类型随目标数据库不同而不同）"""

    dyn_content: str = Field(description="原始文本内容")
    consume_time: float = Field(description="处理耗时，单位秒")
    result: T = Field(description="抽奖信息提取结果")

    def __post_init__(self):
        self.dyn_content = json.dumps(self.dyn_content)


# ============ System Prompt ============

_COMMON_LOTTERY_SYSTEM_PROMPT = """从文本中提取抽奖信息。
规则：
1. prize_names: 奖品名称列表，没有则为空列表
2. lottery_time: 开奖时间，格式YYYY-MM-DD，没有则为null
3. is_lot: 是否抽奖，true/false
4. need_repost: 是否需要转发，true/false
5. required_topic_text: 需要携带的话题文本，如 #抽奖#，无则为空字符串
6. is_grand_prize: 是否大奖，奖品价值高/数量多/知名品牌即为大奖，true/false"""


# 官方/充电抽奖专用：抽奖方式已由 lottery_type 固定，无需判断 is_lot / need_comment /
# need_repost，也无需落库，故只让大模型判断大奖标记。
_OFFICIAL_SYSTEM_PROMPT = """官方/充电抽奖，抽奖方式由 lottery_type 固定。只判断 is_grand_prize：奖品价值高/数量多/知名品牌即为大奖，返回 true/false。"""


def _build_system_prompt(pub_time: datetime | None) -> str:
    """构建系统提示词，可选附加动态发布时间作为时间参考"""
    if pub_time:
        return (
            _COMMON_LOTTERY_SYSTEM_PROMPT
            + f"\n\n发布时间：{pub_time.strftime('%Y-%m-%d')}，开奖时间应不早于此时间。"
        )
    return _COMMON_LOTTERY_SYSTEM_PROMPT


def _preprocess_text(dyn_content: str) -> str:
    """文本预处理：去除链接、繁体转简体"""
    text = re.sub(r"https?://[^\s\u4e00-\u9fff]*", "", dyn_content)
    return _t2s_converter.convert(text)


# ================================================================
# 结构化输出容错解析
# ================================================================
# 背景：模型（尤其是不支持 json_schema/tool-calling 的免费上游）经常返回
# 「非标准 JSON」，被 langchain 反序列化后由 pydantic 抛 ValidationError，
# 导致一次本可挽救的调用被整体判失败。生产日志中出现过的形态：
#   1. ```json {...} ``` 代码围栏包裹的合法 JSON
#   2. 直接返回裸布尔（false / **false**），而不是对象
#   3. thinking 模型的 <think>...</think> 思考过程与 JSON 混排
#   4. 纯自然语言（无法修复，仍交由上层切换下一个 LLM）
# 这里只做「本地文本修复」，不额外消耗 token，也不改变原有调用方式。

# 完整的思考块（含 closing tag）
_THINK_BLOCK_RE = re.compile(r"<think(?:ing)?>.*?</think(?:ing)?>", re.S | re.I)
# 只有 closing tag 的截断形态（开头的思考内容整体丢弃）
_OPEN_THINK_RE = re.compile(r"^.*?</think(?:ing)?>", re.S | re.I)
# Markdown 代码围栏
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.S | re.I)
# 强调形态的布尔（如 **false** / `true` / *false*）
_EMPH_BOOL_RE = re.compile(r"(?:\*\*|__|`)\s*(true|false)\s*(?:\*\*|__|`)", re.I)
# 整段就是一个布尔（允许前后有引号/标点等装饰）
_STRICT_BOOL_RE = re.compile(
    r'^[\s"`*_.。，,!！?？]*(true|false)[\s"`*_.。，,!！?？]*$', re.I
)


def _extract_json_object(text: str) -> str | None:
    """截取文本中第一个花括号配对的 JSON 对象片段（跳过字符串内的花括号）"""
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = False
    escaped = False
    for idx in range(start, len(text)):
        char = text[idx]
        if in_str:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_str = False
            continue
        if char == '"':
            in_str = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return None


def _build_model_from_scalar_bool(value: bool, result_model: type[_TResult]):
    """把裸布尔映射到模型的唯一布尔字段。

    仅当模型只有一个字段且该字段是 bool 时才认为可安全映射
    （如 OfficialPrizeExtractResult 只有 is_grand_prize），
    多字段模型（如 PrizeExtractResult）直接放弃，避免猜错语义。
    """
    fields = result_model.model_fields
    if len(fields) != 1:
        return None
    (name, field_info), = fields.items()
    if field_info.annotation is not bool:
        return None
    try:
        return result_model.model_validate({name: value})
    except ValidationError:
        return None


def _repair_structured_output(
    exc: BaseException, result_model: type[_TResult]
) -> _TResult | None:
    """尽力把模型返回的非标准输出修复成 result_model 实例；无法修复返回 None。

    仅处理 ValidationError，且只处理作用在模型根上的校验错误（loc 为空），
    因为这类错误才携带「模型原始返回」本身。
    """
    if not isinstance(exc, ValidationError):
        return None
    errors = exc.errors()
    if not errors or errors[0].get("loc"):
        return None
    raw = errors[0].get("input")

    # 形态 2：模型直接把单布尔字段的结果返回成 true/false
    if isinstance(raw, bool):
        return _build_model_from_scalar_bool(raw, result_model)
    if not isinstance(raw, str):
        return None

    # 形态 3：剥离思考块
    text = _THINK_BLOCK_RE.sub(" ", raw)
    if "</think" in text.lower():
        text = _OPEN_THINK_RE.sub(" ", text)

    # 形态 1：剥离代码围栏
    fenced = _CODE_FENCE_RE.search(text)
    if fenced:
        text = fenced.group(1)

    for candidate in (text, _extract_json_object(text)):
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(payload, bool):
            return _build_model_from_scalar_bool(payload, result_model)
        if isinstance(payload, dict):
            try:
                return result_model.model_validate(payload)
            except ValidationError:
                continue

    # 形态 2 的变体：模型以强调/独立成句的方式给出布尔（如 **false**）。
    # 这里刻意不做「全文扫描 true/false」的宽松兜底：自然语言/代码里出现的
    # true/false 未必是判定结论，宽松匹配可能悄悄写入错误结论。
    for pattern in (_EMPH_BOOL_RE, _STRICT_BOOL_RE):
        bool_match = pattern.search(text)
        if bool_match:
            return _build_model_from_scalar_bool(
                bool_match.group(1).lower() == "true", result_model
            )
    return None


# ================================================================
# 核心提取逻辑（共享）
# ================================================================


async def _push_cloud_unavailable_error(exc: Exception) -> None:
    """云端 LLM 全部不可用/失败时推送错误告警（由 PushMe 内部限流/去重）"""
    try:
        await a_push_error(
            subject="云端LLM抽奖判断不可用（重试中）",
            content=(
                "云端 LLM 当前全部不可用，抽奖判断（含大奖判断）将持续等比退避重试，"
                f"等待上限 {_RETRY_MAX_DELAY}s，不会跳过保存。请检查云端 LLM 配置或可用性。\n"
                f"错误类型：{type(exc).__name__}\n"
                f"错误信息：{exc}\n"
                f"错误堆栈：\n{traceback.format_exc()}"
            ),
        )
    except Exception as push_err:
        logger.exception(f"推送云端不可用告警失败: {push_err}")


def _describe_disabled_llms() -> str:
    """列出当前已熔断的模型清单（模型名 + 熔断原因），用于告警内容"""
    try:
        stats = get_llm_stats()
    except Exception as e:  # 统计不可用不应影响告警本身
        return f"（获取熔断明细失败：{type(e).__name__}: {e}）"
    disabled = [item for item in stats if item.get("disabled")]
    if not disabled:
        return "（当前未记录到熔断实例）"
    lines = [
        f"- {item.get('model')} @ {item.get('base_url')}：{item.get('disabled_reason')}"
        for item in disabled
    ]
    return "已熔断模型：\n" + "\n".join(lines)


async def _push_all_llms_disabled_error(exc: Exception) -> None:
    """全部云端 LLM 均已熔断时推送告警（经 message-service 推到 pushplus）。

    与「本轮全部失败、仍在等比退避重试」不同：熔断是不可恢复状态
    （模型下线 / 鉴权失败等 HTTP 400/401/403/404），本次不会再重试，
    只能等服务重启或热更新 llm_apis 配置后恢复，因此单独告警。
    """
    try:
        await a_push_error(
            subject="云端LLM全部熔断（抽奖判断已中断）",
            content=(
                "所有云端 LLM 均已因不可恢复错误（模型下线 / 鉴权失败等 400/401/403/404）"
                "被熔断，抽奖判断（含大奖判断）本次不再重试，"
                "需重启服务或热更新 llm_apis 配置后才能恢复。\n"
                f"{_describe_disabled_llms()}\n"
                f"错误信息：{exc}"
            ),
        )
    except Exception as push_err:
        logger.exception(f"推送云端 LLM 全部熔断告警失败: {push_err}")


async def _do_extract(
    *,
    dyn_content: str,
    dyn_publish_time: datetime | None = None,
    chat_openai_client: ChatOpenAI | None = None,
    result_model: type[_TResult],
    system_prompt: str | None = None,
) -> PrizeExtractResp[_TResult]:
    """一次性提取抽奖相关信息（内部共享实现）

    仅使用云端 LLM 进行抽奖判断，不再使用本地大模型，也不做任何回退
    （正则/SVM 等）。当所有云端 LLM 调用均失败时直接抛出 RuntimeError，
    由调用方决定是否跳过保存（留空），等待手动脚本 judge_grand_prize 回填。

    result_model / system_prompt 由调用方决定：
      - 普通/预约抽奖 → PrizeExtractResult + 完整提示词
      - 官方/充电抽奖 → OfficialPrizeExtractResult + 仅大奖提示词
    """
    start_ts = time.time()
    if not dyn_content or not dyn_content.strip():
        return PrizeExtractResp(
            dyn_content=dyn_content,
            consume_time=time.time() - start_ts,
            result=result_model(),
        )

    text = _preprocess_text(dyn_content)
    if not text:
        return PrizeExtractResp(
            dyn_content=text,
            consume_time=time.time() - start_ts,
            result=result_model(),
        )
    # 全部 LLM 均失败时采用等比退避持续重试：等待时间按等比数列递增，
    # 到 _RETRY_MAX_DELAY 后维持上限，直至某一次调用成功为止，不“放弃并跳过保存”。
    # （示例序列：10s → 20s → 40s → ... → 300s → 300s → ...）
    last_err: Exception | None = None
    retry_delay = _RETRY_BASE_DELAY
    alerted = False
    while True:
        if chat_openai_client:
            all_llms = [chat_openai_client]
        else:
            try:
                # 每轮都重新取实例列表：被熔断（不可恢复错误）的模型会立即被剔除，
                # 全部熔断时立刻告警并抛出，而不是拿着旧快照无限退避重试。
                all_llms = get_all_free_llms()
            except AllLLMsDisabledError as e:
                # 全部模型熔断：不可恢复，发送告警（pushplus）后直接抛出
                await _push_all_llms_disabled_error(e)
                raise
            except RuntimeError as e:
                # 未配置任何云端 LLM：不再回退，直接抛错
                await _push_cloud_unavailable_error(e)
                raise
        for idx, llm in enumerate(all_llms):
            llm = llm.bind(
                **SamplingPreset.TEXT_NON_THINKING.to_kwargs(num_predict=256)
            )
            try:
                msg_content = system_prompt or _build_system_prompt(dyn_publish_time)
                structured_llm = llm.with_structured_output(result_model)
                messages = [
                    {"role": "system", "content": msg_content},
                    {"role": "user", "content": text},
                ]
                result = await structured_llm.ainvoke(
                    messages,
                    extra_body={
                        "chat_template_kwargs": {"enable_thinking": False},
                        "thinking": {"type": "disabled"},
                    },
                )
                logger.info(
                    f"免费 LLM [{idx + 1}/{len(all_llms)}] 提取抽奖信息结果: {result}"
                )
                return PrizeExtractResp(
                    dyn_content=text,
                    consume_time=time.time() - start_ts,
                    result=result,
                )
            except Exception as e:
                # 结构化解析失败时先尝试本地修复（围栏 / 裸布尔 / 思考块等），
                # 修复成功则直接采用，避免一次本可挽救的调用被整体判失败
                repaired = _repair_structured_output(e, result_model)
                if repaired is not None:
                    logger.warning(
                        f"免费 LLM [{idx + 1}/{len(all_llms)}] 输出格式不合规"
                        f"（{type(e).__name__}），已本地修复后采用: {repaired}"
                    )
                    return PrizeExtractResp(
                        dyn_content=text,
                        consume_time=time.time() - start_ts,
                        result=repaired,
                    )
                logger.error(
                    f"免费 LLM [{idx + 1}/{len(all_llms)}] 抽奖判断失败"
                    f"（{type(e).__name__}: {e}），尝试下一个"
                )
                last_err = e
                continue

        # 本轮所有 LLM 均失败：仅告警一次，然后按等比退避等待并继续重试
        if not alerted:
            await _push_cloud_unavailable_error(
                last_err or RuntimeError("全部免费 LLM 均调用失败")
            )
            alerted = True
        logger.warning(
            f"本轮 {len(all_llms)} 个免费 LLM 均失败了，"
            f"{retry_delay}s 后重试（等比退避，上限 {_RETRY_MAX_DELAY}s），"
            f"最近错误：{type(last_err).__name__}: {last_err}"
        )
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * _RETRY_DELAY_FACTOR, _RETRY_MAX_DELAY)


# ================================================================
# 公开入口 — 分别对应 biliopusdb / dyndetail 的 t_lot_extra_info 需求
# ================================================================


async def extract_prize_info_for_biliopusdb(
    *,
    dyn_content: str,
    dyn_publish_time: datetime | None = None,
    chat_openai_client: ChatOpenAI | None = None,
) -> PrizeExtractResp[PrizeExtractResult]:
    """
    面向 biliopusdb (普通抽奖动态) 的抽奖信息提取。

    返回的 PrizeExtractResult 包含完整字段:
      - prize_names, lottery_time → 用于 t_lot_extra_info 表缓存
      - is_lot, need_repost, required_topic_text → 用于抽奖判断
      - is_grand_prize → 用于 t_lot_extra_info (ref_id + lot_type='common')
      - chat_openai_client -> 支持传入自定义的客户端来执行操作
    调用方通常进一步通过 SqlHelper.save_extra_info() 统一入库（含 prize_names / lottery_time）。
    仅使用云端 LLM；当所有云端 LLM 均失败时直接抛出 RuntimeError，
    不提供回退，由调用方跳过保存（留空待手动脚本 judge_grand_prize 回填）。
    """
    return await _do_extract(
        dyn_content=dyn_content,
        dyn_publish_time=dyn_publish_time,
        chat_openai_client=chat_openai_client,
        result_model=PrizeExtractResult,
    )


async def extract_prize_info_for_lotdata(
    *,
    dyn_content: str,
    chat_openai_client: ChatOpenAI | None = None,
) -> PrizeExtractResp[OfficialPrizeExtractResult]:
    """
    面向 dyndetail (官方/充电抽奖) 的抽奖信息提取。

    官方抽奖的抽奖方式已由 lottery_type 固定，无需用大模型提取 is_lot /
    need_comment / need_repost，也无需落库，因此 result 仅含 is_grand_prize，
    用于 t_lot_extra_info (lottery_id 关联 lotdata)。

    调用方通常通过 grpc_sql_helper._upsert_extra_info() / batch_save_extra_info() 入库。
    仅使用云端 LLM；当所有云端 LLM 均失败时直接抛出 RuntimeError，
    不提供回退，由调用方跳过保存（留空待手动脚本 judge_grand_prize 回填）。
    """
    return await _do_extract(
        dyn_content=dyn_content,
        dyn_publish_time=None,  # 官方抽奖不传发布时间
        chat_openai_client=chat_openai_client,
        result_model=OfficialPrizeExtractResult,
        system_prompt=_OFFICIAL_SYSTEM_PROMPT,
    )


if __name__ == "__main__":

    async def _test():
        text = """准备开韩国文学的坑了！大家有没有好看的韩国小说推荐？视频来源：
书：《明亮的夜晚》（译者：叶蕾；出版社：台海出版社；出品方: 磨铁·大鱼读品）；《对我无害之人》（译者：徐丽红；出版社：中国友谊出版公司；出品方: 磨铁·大鱼读品）；《福柯读本》（作者/译者：米歇尔·福柯 / 汪民安；出版社：北京大学出版社；丛书: 培文读本丛书）；《第二性》（译者：郑克鲁；出版社：上海译文出版社；丛书: 西蒙娜·德·波伏瓦系列）。部分文案由译林出版社编辑老师提供；图片来自于网络，部分图片来自于上述参考书目。

电影：《82年生的金智英 82년생 김지영 (2019)》、《悲伤崔恩荣：消解父权，如何祛魅、僭越、割裂观念歧视？自我存在感与可能性，痛苦与新生…《明亮的夜晚》（首先我没看过这边书）观看时脑子里一直是歌德曾经说过的那句“我可以确保正直，但不能保证没有偏见。”文中主人公的遭遇是否就是正直的离席导致偏见的过度纵横。从三代人的视角来看，作为受害者的女性始终受到社会偏见的影响，都处在一种不正直的权力约束下，也即因社会关系网中每一个人的“失智”导致了受害者的人生悲剧。up于视频3min左右处抛出的一系列性别疑惑，主观上我认为是对“正直”问题的思索以及随后对自我存在意义和自性的探寻。4min处则是up认为的本文对“正直”作出的两大呼唤（另一个在下面）：对男性祛魅，对人际关系祛魅。只有祛魅之后才有正直可言，而正直的指引能将所谓偏见归束在一个不那么僭越乃至伤害他人的位置。
    让我意想不到的是，在最后还有一组更深的向内探寻自性的过程，更多地从羞耻感着笔，从对自己的欺瞒着笔，把自己的天真和幻象剖开，认真地和自己的困厄相联系。这一点和《没有人给他写信的上校》中那个上校完全不同，然而最开始的欺骗是相通的：一个选择抓住缥缈的斗鸡大赛作为活下去的新希望，一个选择投奔本就不是真实的爱着的新家庭。欺骗引发的必然结果是二者同样惨淡的终局：“那我们这些天吃什么呢？”“吃屎！”；“他对我来说真的是有意义又有分量的人吗？”“在知道他有外遇之前，我眞的像一直以来坚信的那样没有那么痛苦，也没有那么病态吗? 我想通过和他结婚逃避自己存在的问题和具有的可能性。”
    “不能保证没有偏见”，就是个人对自己的部分行为没有正确认知，或者已经意识到问题所在却有意逃避，都将在随后引发“欺瞒”，因此不能保证个人无偏见。无论是意识到还是没意识到，无意义的希望本身就是一种欺瞒，两个人都因为羞耻感，因为面子，失去了方方面面而言都更加重要的东西。这一点上，“正直”不能解决羞耻感的问题，因为正直能解决是更加广泛的社会性问题，而不能处理好自性这一更加内化的问题。而作者给出了自己的答复：对羞耻感除魅。在最后以一种托马斯泰式的自我心理剖析，直接点出韩国人在自性方面，尤其是羞耻感方面的问题，解决正直无法解决的问题。
    而根据韩国情况，“正直”能做到的东西需要更大的力量，需要政府能力的全方位进步，这除了（）基本难以实现。然而本文重在提供了一种新出路，完善自性，自我接纳，独立成长，在更加小处，抚青萍而后起微澜，我认为这才是这本书好的地方（第一部分）

非常感谢小伙伴的长评，很棒很棒！！！这期视频会因为你的长评而更有意义（更重要的是你看到了视频最后，发现了我提出羞耻感祛魅这个观点，而不是只看到了视频里性别处境、男女地位等浮于表面的话题）。对了，先说一下《明亮的夜晚》这本书在豆瓣上的评分：9.0，超过2万5千人的给出了评价，还能有9分这样的高分，相当了不起，所以你说你还没看这本书——可以看一下，很值得！

不得不说，你看完你的评论我就知道你基本理解了本期视频及文案想要表达的内容，那么就就着视频尾部的“羞耻感祛魅”这个话题延伸再分享一下看完《明亮的夜晚》时，为什么突然有了这个理解，崔恩荣在小说里几乎没有提到“羞耻感”，但那一段——“我不想经历真心实意地深爱一个人的那种撕心裂肺的痛苦。我想远离这种感情上的可能性，在不冷不热的关系中安全地生活。还有比欺骗自己更容易的事吗？离婚后我经历的痛苦时光不只是因为丈夫的欺骗，也是我欺骗自己的结果。扪心自问，其中更让我痛苦的正是我对自己的欺骗。”要留意这一段，明明女主经历了那么多不公平的事情，读者的情绪和同理心也都站到了女主那边去，这一段的出现却着实在引导读者思考女主的问题，跟着她的内心一同回顾结婚动机，也就是造成离婚局面的最初决定，然后得出“不只是丈夫的欺骗，也是我欺骗自己的结果”，这个“自省”的逻辑是很有深意的，崔恩荣为什么要这么做？为什么女主会有一个这样的表态？"""
        result = await extract_prize_info_for_biliopusdb(dyn_content=text)
        print(f"biliopusdb 提取结果: {result}")

        result2 = await extract_prize_info_for_lotdata(dyn_content=text)
        print(f"lotdata 提取结果: {result2}")

    async def _to_csv():
        import csv
        from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
        from Service.GetOthersLotDyn.Sql.models import TLotdyninfo
        from pandas import DataFrame
        from sqlalchemy import select, func

        async with SqlHelper.async_session() as session:
            sql = (
                select(TLotdyninfo)
                .where(TLotdyninfo.officialLotType.isnot(None))
                .order_by(func.char_length(TLotdyninfo.dynContent).desc())
                .limit(10)
            )
            res = await session.execute(sql)
            da: list[TLotdyninfo] = res.scalars().all()
        prize_extract_results = []
        for d in da:
            result = await extract_prize_info_for_biliopusdb(
                dyn_content=d.dynContent,
                dyn_publish_time=d.pubTime,
            )
            prize_extract_results.append(result)
        pd = DataFrame([r.model_dump() for r in prize_extract_results])
        pd.to_csv(
            "dyn_content_result.csv",
            index=False,
            encoding="utf-8",
            quoting=csv.QUOTE_NONNUMERIC,
        )

    asyncio.run(_test())
