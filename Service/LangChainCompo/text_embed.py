import asyncio
from typing import List

from langchain_openai import OpenAIEmbeddings

import Models.lottery_database.milvusModel.biliMilvusModel as biliMilvusModel
import Service.LangChainCompo.lottery_data_vec_sql.sql_helper as sql_helper
import Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer as DynDetailSqlHelperMysqlVer
import Service.GrpcModule.GrpcSrc.SQLObject.models as models
from CONFIG import CONFIG, ModelName
from Utils.通用.Common import log_max_count_retry_wrapper

_embeddings = OpenAIEmbeddings(
    model=ModelName.TEXT_EMBEDDING_MULTILINGUAL_E5_BASE,
    openai_api_base=CONFIG.llama_url,
    openai_api_key="1",
)

_embedding_lock = asyncio.Lock()  # 防止并发请求，服务器性能太弱了


async def _create_embedding(
    text: str | None,
) -> list[float] | None:
    async with _embedding_lock:
        if type(text) is not str:
            return None
        if not text.strip():
            return None
        return await _embeddings.aembed_query(text)


@log_max_count_retry_wrapper()
async def save_bili_lot_data_embeddings(
    data_ls: List[biliMilvusModel.BiliLotData],
) -> list[list[float]]:
    return await sql_helper.milvus_sql_helper.upsert_bili_lot_data(
        [x for x in data_ls if x.prize_vec]
    )  # 保存的时候确保vec是存在的


@log_max_count_retry_wrapper()
async def lot_data_2_bili_lot_data_ls(
    x: models.Lotdata,
) -> List[biliMilvusModel.BiliLotData]:
    """
    sqlalchemy的Lotdata转换成milvusdb的biliMilvusModel.BiliLotData模型
    返回1-3个数据
    :return:
    """
    lottery_id = x.lottery_id
    first_prize_cmt = x.first_prize_cmt
    second_prize_cmt = x.second_prize_cmt
    third_prize_cmt = x.third_prize_cmt
    lottery_time = x.lottery_time
    embeddings = await asyncio.gather(
        _create_embedding(first_prize_cmt),
        _create_embedding(second_prize_cmt),
        _create_embedding(third_prize_cmt),
    )
    first_prize_vec, second_prize_vec, third_prize_vec = embeddings
    ret_list = [
        biliMilvusModel.BiliLotData(
            pk=lottery_id * 10,
            lottery_id=lottery_id,
            prize_vec=first_prize_vec,
            prize_cmt=first_prize_cmt,
            lottery_time=lottery_time,
        )
    ]
    if second_prize_vec is not None:
        ret_list.append(
            biliMilvusModel.BiliLotData(
                pk=lottery_id * 20,
                lottery_id=lottery_id,
                prize_vec=second_prize_vec,
                prize_cmt=second_prize_cmt,
                lottery_time=lottery_time,
            )
        )
    if third_prize_vec is not None:
        ret_list.append(
            biliMilvusModel.BiliLotData(
                pk=lottery_id * 30,
                lottery_id=lottery_id,
                prize_vec=third_prize_vec,
                prize_cmt=third_prize_cmt,
                lottery_time=lottery_time,
            )
        )
    return ret_list


#: 关键词/向量召回的候选池大小。进行中/未来抽奖约 425 条，500 足够覆盖；
#: 两路召回后统一做 RRF 融合与分页，避免把分页压力下推到两套索引。
_SEARCH_CANDIDATE_LIMIT = 500
#: 最少相似补充条数：在关键词精确命中之外，至少再补 20 条向量近邻。
#: 这样「显卡」这类只有 2 条精确命中的查询也能看到更多相关内容；
#: 关键词完全不存在时则返回 20 条语义最相近的进行中抽奖。
_MIN_SIMILAR_RESULT_COUNT = 20
#: RRF 融合参数：k 越大，排名靠后的结果权重衰减越慢
_RRF_K = 60
#: 关键词权重高于向量：当前 e5 短中文排序偏弱，向量只做语义补充
_KEYWORD_WEIGHT = 1.0
_VECTOR_WEIGHT = 0.35


def _prize_text(row: models.Lotdata) -> str:
    return " ".join(
        filter(
            None,
            [row.first_prize_cmt, row.second_prize_cmt, row.third_prize_cmt],
        )
    ).lower()


def _shares_query_fragment(row: models.Lotdata, query_text: str) -> bool:
    """向量结果是否与查询共享字符片段（用于过滤纯向量噪声）。

    当前 e5 模型对短中文关键词区分度有限，纯向量召回会把「购物卡」排到
    「显卡」前面。对**只在向量列表里出现**的结果要求至少共享一个查询
    2-gram（或直接包含短查询），保证展示内容与搜索词有可解释的关联；
    两路都命中的结果不做限制。后续换用更强的 embedding 模型可放宽此闸门。
    """
    query = query_text.strip().lower()
    if not query:
        return False
    text = _prize_text(row)
    if not text:
        return False
    if len(query) <= 2:
        return query in text
    grams = {
        query[i : i + 2]
        for i in range(len(query) - 1)
        if query[i : i + 2].strip()
    }
    return any(gram in text for gram in grams)


async def _search_lottery_vector(
    query_text: str, limit: int, offset: int = 0
) -> List[models.Lotdata]:
    """Milvus 向量召回（按相关度顺序返回 MySQL 行）。"""
    query_vec = await _create_embedding(query_text)
    if not query_vec:
        return []
    res = await sql_helper.milvus_sql_helper.search_bili_lot_data(
        query_vec=query_vec, limit=limit, offset=offset
    )
    # Milvus 返回顺序 = 相似度顺序；lottery_id 可能因多个奖品档位重复，先去重。
    lottery_id_ls = list(
        dict.fromkeys(
            x.get("entity").get("lottery_id")
            for x in res[0]
            if x.get("entity") and x.get("entity").get("lottery_id") is not None
        )
    )
    lot_data_ls = await DynDetailSqlHelperMysqlVer.grpc_sql_helper.get_lotDetail_ls_by_lot_ids(
        lottery_id_ls
    )
    # MySQL 的 IN 查询不保证顺序，这里按 Milvus 的相关度顺序重排。
    lot_data_map = {row.lottery_id: row for row in lot_data_ls}
    return [lot_data_map[lid] for lid in lottery_id_ls if lid in lot_data_map]


async def search_lottery_text(
    query_text: str, limit: int = 10, offset=0
) -> List[models.Lotdata]:
    """纯向量语义检索（Milvus），保留给需要单独语义召回的调用方。"""
    return await _search_lottery_vector(query_text.strip(), limit, offset)


def _rrf_fuse(
    keyword_rows: list[models.Lotdata],
    vector_rows: list[models.Lotdata],
    query_text: str,
) -> list[models.Lotdata]:
    """Reciprocal Rank Fusion：关键词 1.0 / 向量 0.35。

    - 两路都命中的 lottery_id 分数相加，排名更高；
    - 只在向量列表出现的结果要求与查询共享字符片段，过滤 e5 噪声；
    - 关键词列表靠前的结果始终优先于纯向量结果。
    """
    keyword_ids = {row.lottery_id for row in keyword_rows}
    scores: dict[int, float] = {}
    rows: dict[int, models.Lotdata] = {}
    for weight, items in (
        (_KEYWORD_WEIGHT, keyword_rows),
        (_VECTOR_WEIGHT, vector_rows),
    ):
        for rank, row in enumerate(items):
            lid = row.lottery_id
            if lid not in keyword_ids and weight == _VECTOR_WEIGHT:
                if not _shares_query_fragment(row, query_text):
                    continue
            scores[lid] = scores.get(lid, 0.0) + weight / (_RRF_K + rank + 1)
            rows.setdefault(lid, row)
    # sorted 是稳定排序：同分时保持首次出现顺序（关键词列表在前）
    return [rows[lid] for lid in sorted(scores, key=lambda lid: -scores[lid])]


async def search_lottery_by_keyword(
    query_text: str, limit: int = 10, offset: int = 0
) -> tuple[List[models.Lotdata], int]:
    """关键词搜索主入口：FULLTEXT(ngram) + Milvus 向量，RRF 融合。

    召回策略：
    1. MySQL FULLTEXT ngram 负责关键词召回（多关键词/中文子串，BM25 排序）；
       单字或全文无命中时回退到 LIKE/2-gram；
    2. Milvus dense 向量负责语义召回（「苹果手机」这类同义/近义表达）；
    3. RRF 融合两路结果：关键词权重 1.0、向量 0.35，关键词命中始终排在
       纯向量结果之前；纯向量结果还要求与查询共享字符片段，过滤 e5 噪声；
    4. 无论精确命中多少，都在其后至少补 ``_MIN_SIMILAR_RESULT_COUNT``（20）
       条相似结果，保证结果页内容充足；关键词完全不存在时即为 20 条语义近邻。

    检索范围仍只覆盖 ``status=0`` 且未开奖的抽奖（约数百条），
    候选池 500 条足够覆盖，融合后再分页。

    Returns:
        (当前页数据, 候选并集总数)。
    """
    query_text = (query_text or "").strip()
    if not query_text:
        return [], 0

    helper = DynDetailSqlHelperMysqlVer.grpc_sql_helper
    # 1) 关键词召回：优先 FULLTEXT；单字/无命中回退 LIKE + 2-gram
    keyword_rows, _ = await helper.search_lottery_fulltext(
        keyword=query_text, limit=_SEARCH_CANDIDATE_LIMIT, offset=0
    )
    if not keyword_rows:
        keyword_rows, _ = await helper.search_lottery_by_keyword(
            keyword=query_text, limit=_SEARCH_CANDIDATE_LIMIT, offset=0
        )

    # 2) 语义召回（Milvus 只索引进行中/未来抽奖，与检索范围一致）
    vector_rows = await _search_lottery_vector(
        query_text, limit=_SEARCH_CANDIDATE_LIMIT
    )

    # 3) RRF 融合；无论精确命中多少，都至少补 _MIN_SIMILAR_RESULT_COUNT 条相似结果。
    #    相似结果 = 不在关键词命中里的向量近邻，排在精确命中之后；
    #    关键词完全不存在时就是 20 条语义近邻，页面不会空。
    keyword_ids = {row.lottery_id for row in keyword_rows}
    fused = _rrf_fuse(list(keyword_rows), vector_rows, query_text)
    seen = {row.lottery_id for row in fused}
    similar_count = sum(1 for row in fused if row.lottery_id not in keyword_ids)
    if similar_count < _MIN_SIMILAR_RESULT_COUNT:
        for row in vector_rows:
            if row.lottery_id in keyword_ids or row.lottery_id in seen:
                continue
            fused.append(row)
            seen.add(row.lottery_id)
            similar_count += 1
            if similar_count >= _MIN_SIMILAR_RESULT_COUNT:
                break
    total = len(fused)
    return fused[offset : offset + limit], total


async def get_lottery_entity_num() -> int:
    if res := await sql_helper.milvus_sql_helper.get_bili_lot_data_collection_stats():
        return res.get("row_count") or 0
    return 0


if __name__ == "__main__":
    result = asyncio.run(_create_embedding("你好"))
    print(result)
