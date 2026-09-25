"""骨架冒烟测试：导入与类型可构造。"""

from app.models import AggregatedResult, Metrics, SourceResult, SourceStatus


def test_models_constructible():
    r = AggregatedResult(
        merged={},
        sources=[SourceResult(source="a", status=SourceStatus.OK, value={"v": 1})],
        degraded=False,
        elapsed_ms=0.0,
    )
    assert r.merged == {}
    assert Metrics().as_dict()["cache_hit"] == 0
