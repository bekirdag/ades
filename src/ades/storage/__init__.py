"""Storage helpers and backend selection for ades."""

from .backend import (
    BackendPlan,
    ExactExtractionBackend,
    MetadataBackend,
    OperatorLookupBackend,
    RuntimeTarget,
    UnsupportedRuntimeConfigurationError,
    build_metadata_store,
    normalize_metadata_backend,
    normalize_runtime_target,
    resolve_backend_plan,
)

__all__ = [
    "BackendPlan",
    "ExactExtractionBackend",
    "MetadataBackend",
    "OperatorLookupBackend",
    "RuntimeTarget",
    "UnsupportedRuntimeConfigurationError",
    "build_metadata_store",
    "normalize_metadata_backend",
    "normalize_runtime_target",
    "resolve_backend_plan",
]
