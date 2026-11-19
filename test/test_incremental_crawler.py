"""IncrementalIdCrawler 的自增游标 / 重试参数还原语义。"""

import pytest

from Models.base.custom_pydantic import CustomBaseModelHashable
from Service.BaseCrawler.CrawlerType import IncrementalIdCrawler
from Service.BaseCrawler.config import CrawlerConfig
from Service.BaseCrawler.model.base import WorkerStatus


class _Params(CustomBaseModelHashable):
    id: int

    def __hash__(self) -> int:
        return hash(self.id)


class _WithSeedCrawler(IncrementalIdCrawler[_Params]):
    Config = CrawlerConfig
    id_field = "id"

    def make_params(self, business_id: int) -> _Params:
        return _Params(id=business_id)

    async def is_stop(self) -> bool:
        return False

    async def handle_fetch(self, params: _Params) -> WorkerStatus:
        return WorkerStatus.complete

    async def main(self):
        await self.run()


class _NoSeedCrawler(_WithSeedCrawler):
    include_seed_in_key_params = False


@pytest.mark.asyncio
async def test_key_params_gen_includes_seed_by_default():
    crawler = _WithSeedCrawler()
    gen = crawler.key_params_gen(_Params(id=5))
    got = [(await anext(gen)).id for _ in range(3)]
    await gen.aclose()
    assert got == [5, 6, 7]


@pytest.mark.asyncio
async def test_key_params_gen_can_skip_seed():
    crawler = _NoSeedCrawler()
    gen = crawler.key_params_gen(_Params(id=5))
    got = [(await anext(gen)).id for _ in range(3)]
    await gen.aclose()
    assert got == [6, 7, 8]


def test_retry_params_are_derived_from_make_params():
    crawler = _WithSeedCrawler()
    assert crawler.retry_business_id(_Params(id=9)) == 9
    assert crawler.retry_business_id(None) is None
    assert crawler.retry_params(9) == _Params(id=9)
