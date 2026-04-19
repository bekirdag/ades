from types import SimpleNamespace

from ades.packs import runtime as runtime_module


def test_load_pack_runtime_reuses_cache_until_cleared(monkeypatch, tmp_path) -> None:
    calls: list[str] = []
    runtime_module.clear_pack_runtime_cache()

    def _fake_build(pack_id: str, *, registry):
        calls.append(pack_id)
        return {"pack_id": pack_id, "root": str(tmp_path)}

    fake_registry = SimpleNamespace(
        runtime_target=SimpleNamespace(value="local"),
        metadata_backend=SimpleNamespace(value="sqlite"),
        store=SimpleNamespace(database_url=None),
    )

    monkeypatch.setattr(runtime_module, "_build_pack_runtime", _fake_build)

    first = runtime_module.load_pack_runtime(tmp_path, "general-en", registry=fake_registry)
    second = runtime_module.load_pack_runtime(tmp_path, "general-en", registry=fake_registry)

    assert first == second
    assert calls == ["general-en"]

    runtime_module.clear_pack_runtime_cache()
    third = runtime_module.load_pack_runtime(tmp_path, "general-en", registry=fake_registry)

    assert third == first
    assert calls == ["general-en", "general-en"]
