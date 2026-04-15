from contextlib import contextmanager
from pathlib import Path
import socket
import threading
import time
import urllib.request

import uvicorn

from ades.config import Settings
from ades.packs.installer import PackInstaller
from ades.service.app import create_app
from ades.service.client import tag_file_via_local_service, tag_via_local_service
from ades.storage.backend import MetadataBackend, RuntimeTarget


def _reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


@contextmanager
def _running_service(storage_root: Path, *, port: int):
    app = create_app(storage_root=storage_root)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=0.25):
                break
        except Exception:
            time.sleep(0.1)
    else:
        server.should_exit = True
        thread.join(timeout=5.0)
        raise AssertionError("local test service did not become ready")

    try:
        yield
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)


def test_local_service_client_hits_live_tag_endpoints(tmp_path: Path) -> None:
    PackInstaller(tmp_path).install("finance-en")
    input_path = tmp_path / "report.html"
    input_path.write_text("<p>Org Beta said TICKA traded on EXCHX.</p>", encoding="utf-8")
    port = _reserve_loopback_port()
    settings = Settings(
        host="127.0.0.1",
        port=port,
        storage_root=tmp_path,
        default_pack="finance-en",
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
        database_url="postgresql://local/test",
    )

    with _running_service(tmp_path, port=port):
        text_response = tag_via_local_service(
            "Org Beta said TICKA traded on EXCHX after USD 12.5 guidance.",
            pack="finance-en",
            settings=settings,
        )
        file_response = tag_file_via_local_service(
            input_path,
            pack="finance-en",
            settings=settings,
        )

    text_labels = {entity.label for entity in text_response.entities}
    file_labels = {entity.label for entity in file_response.entities}

    assert {"organization", "ticker", "exchange", "currency_amount"} <= text_labels
    assert {"organization", "ticker", "exchange"} <= file_labels
    assert file_response.source_path == str(input_path.resolve())
