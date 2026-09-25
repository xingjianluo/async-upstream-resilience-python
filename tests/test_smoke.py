async def test_skeleton_imports() -> None:
    # 骨架冒烟：保证所有模块可导入、类型可构造
    from app.config import ResilienceConfig
    from app.models import CacheState, Priority, SourceState

    cfg = ResilienceConfig()
    assert cfg.aggregate_timeout > 0
    assert Priority.HIGH.value > Priority.LOW.value
    assert SourceState.OK.value == "ok"
    assert CacheState.MISS.value == "miss"
