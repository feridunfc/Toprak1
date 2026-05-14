
import os
from unittest.mock import patch

from hfa_control import auth


def test_auth_runtime_env_changes_are_observed():
    with patch.dict(os.environ, {"APP_ENV": "development"}, clear=True):
        assert auth.validate_shared_secret_header("anything") is True

    with patch.dict(os.environ, {"APP_ENV": "production"}, clear=True):
        assert auth.validate_shared_secret_header("anything") is False


def test_auth_status_is_runtime_resolved():
    with patch.dict(os.environ, {"APP_ENV": "staging", "CP_AUTH_SECRET": "x"}, clear=True):
        status = auth.auth_status()
        assert status.strict is True
        assert status.enabled is True
        assert status.mode == "shared-secret-header"
