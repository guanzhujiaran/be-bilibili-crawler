"""云端 LLM 保存时「按来源回填密钥」的单测（`Service/llm_service/pool._resolve_tokens`）。

口径（2026-11-12 修订）：**密钥属于源（base_url），不属于某一行**。
管理页的一张卡片会勾多个模型、保存时展开成多条，行序整体后移，
所以不能按下标判断「新实例」或认领密钥。覆盖：

1. 同一地址下新增 / 删除 / 重排模型都能沿用原密钥；
2. 同源多把密钥时按模型名认领自己的那把，认不出来就报错（绝不乱发别人的钥匙）；
3. 新来源的空 token 保留空串（免鉴权上游 / 由调用方自行校验）；
4. 脱敏值只在「同索引 + 同地址」时回填，越界 / 跨源一律拒绝。

运行:
    uv run python -m pytest test/test_llm_pool_tokens.py -v
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import LLMApiConfig
from Service.llm_service import pool

_A = "sk-source-a-plain-0001"
_B = "sk-source-b-plain-0002"
_URL_A = "https://a.example.com/v1"
_URL_B = "https://b.example.com/v1"


def _cfg(
    model: str,
    *,
    base_url: str = _URL_A,
    token: str = _A,
    rps: float = 0.5,
) -> LLMApiConfig:
    return LLMApiConfig(
        base_url=base_url, model_name=model, token=token, requests_per_second=rps
    )


def _resolve(apis: list[LLMApiConfig], current: list[LLMApiConfig]) -> list[str]:
    return [cfg.token for cfg in pool._resolve_tokens(apis, current)]


# ================================================================
# 1. 同源共用一把密钥
# ================================================================


def test_insert_model_in_middle_keeps_source_token() -> None:
    """在已有来源中间插一个模型（行序后移），新行留空也能沿用原密钥。"""
    current = [_cfg("m1"), _cfg("m2")]

    tokens = _resolve(
        [_cfg("m1", token=""), _cfg("m3", token=""), _cfg("m2", token="")], current
    )

    assert tokens == [_A, _A, _A]


def test_reorder_across_sources_keeps_each_token() -> None:
    """不同来源的卡片换顺序时，各自仍然拿到自己的密钥。"""
    current = [_cfg("m1", token=_A), _cfg("c1", base_url=_URL_B, token=_B)]

    tokens = _resolve(
        [
            _cfg("c1", base_url=_URL_B, token=""),
            _cfg("m2", token=""),
            _cfg("m1", token=""),
        ],
        current,
    )

    assert tokens == [_B, _A, _A]


def test_rename_model_reuses_single_source_token() -> None:
    """同源只有一把密钥时，改模型名（等于新行）也能沿用。"""
    current = [_cfg("old-name")]

    assert _resolve([_cfg("new-name", token="")], current) == [_A]


def test_trailing_slash_is_same_source() -> None:
    """`…/v1` 与 `…/v1/` 是同一个源，不能因为尾斜杠丢密钥。"""
    current = [_cfg("m1", base_url="https://a.example.com/v1/")]

    assert _resolve([_cfg("m1", token="")], current) == [_A]


def test_new_source_keeps_empty_token() -> None:
    """全新来源没有原密钥可沿用：保留空串（免鉴权上游，管理端另有一道校验）。"""
    assert _resolve([_cfg("m1", base_url="https://new.example.com/v1", token="")], []) == [
        ""
    ]


def test_empty_source_list_keeps_empty_token() -> None:
    assert _resolve([_cfg("m1", token="")], []) == [""]


# ================================================================
# 2. 同源多把密钥
# ================================================================


def test_multiple_tokens_same_source_claimed_by_model_name() -> None:
    """同址两把密钥：按模型名各认领自己的那把，不要串钥匙。"""
    current = [
        _cfg("k1", token=_A),
        _cfg("k2", token=_B),
    ]

    assert _resolve([_cfg("k1", token=""), _cfg("k2", token="")], current) == [_A, _B]


def test_ambiguous_source_raises_instead_of_guessing() -> None:
    """同址多把密钥且认不出模型：必须报错，绝不随便挑一把发出去。"""
    current = [_cfg("k1", token=_A), _cfg("k2", token=_B)]

    with pytest.raises(ValueError, match="多把不同密钥"):
        _resolve(
            [_cfg("k1", token=""), _cfg("k2", token=""), _cfg("brand-new", token="")],
            current,
        )


# ================================================================
# 3. 脱敏值
# ================================================================


def test_masked_token_filled_from_same_slot() -> None:
    current = [_cfg("m1", token=_A)]
    mask = pool._mask_token(_A)

    assert _resolve([_cfg("m1", token=mask)], current) == [_A]


def test_masked_token_survives_row_shift() -> None:
    """脱敏值是「沿用这一把」的引用：插入新模型后行序后移也能对上。"""
    current = [_cfg("m1", token=_A), _cfg("m2", token=_B)]
    apis = [
        _cfg("new-model", token="sk-plain-brand-new"),
        _cfg("m1", token=pool._mask_token(_A)),
        _cfg("m2", token=pool._mask_token(_B)),
    ]

    assert _resolve(apis, current) == ["sk-plain-brand-new", _A, _B]


def test_masked_token_picks_right_key_on_multi_key_source() -> None:
    """同一地址两把密钥：各自回传自己的脱敏值，就各自拿到自己的明文。"""
    current = [_cfg("k1", token=_A), _cfg("k2", token=_B)]

    assert _resolve(
        [
            _cfg("k1", token=pool._mask_token(_B)),
            _cfg("k2", token=pool._mask_token(_A)),
        ],
        current,
    ) == [_B, _A]


def test_masked_token_unknown_rejected() -> None:
    current = [_cfg("m1", token=_A)]

    with pytest.raises(ValueError, match="脱敏值"):
        _resolve([_cfg("m1", token=_A), _cfg("m2", token="sk-b****0002")], current)


def test_masked_token_from_other_source_rejected() -> None:
    """脱敏值不跨源回填：换了地址就对不上，宁可报错也不猜。"""
    current = [_cfg("m1", base_url=_URL_A, token=_A)]

    with pytest.raises(ValueError, match="脱敏值"):
        _resolve([_cfg("m1", base_url=_URL_B, token=pool._mask_token(_A))], current)


def test_short_tokens_with_same_mask_rejected() -> None:
    """短密钥都脱敏成 `****`，撞车时不能替用户猜，要求填明文。"""
    current = [_cfg("k1", token="short-1"), _cfg("k2", token="short-2")]

    with pytest.raises(ValueError, match="脱敏值相同"):
        _resolve([_cfg("brand-new", token="****")], current)
