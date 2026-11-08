from pathlib import Path

import pytest

from fault_injection.runner import prepare_baseline


@pytest.fixture(scope="session")
def baseline(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """January to May loaded and built once (~100 s), shared by every test here."""
    path = tmp_path_factory.mktemp("baseline")
    prepare_baseline(path)
    return path
