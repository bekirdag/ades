from ades.storage import (
    ExactExtractionBackend,
    MetadataBackend,
    OperatorLookupBackend,
    RuntimeTarget,
    resolve_backend_plan,
)


def test_local_sqlite_backend_plan_exposes_current_role_split() -> None:
    plan = resolve_backend_plan(
        runtime_target=RuntimeTarget.LOCAL,
        metadata_backend=MetadataBackend.SQLITE,
    )

    assert plan.runtime_target is RuntimeTarget.LOCAL
    assert plan.metadata_persistence_backend is MetadataBackend.SQLITE
    assert plan.exact_extraction_backend is ExactExtractionBackend.COMPILED_MATCHER
    assert plan.operator_lookup_backend is OperatorLookupBackend.SQLITE_SEARCH_INDEX


def test_production_postgresql_backend_plan_exposes_current_role_split() -> None:
    plan = resolve_backend_plan(
        runtime_target=RuntimeTarget.PRODUCTION_SERVER,
        metadata_backend=MetadataBackend.POSTGRESQL,
    )

    assert plan.runtime_target is RuntimeTarget.PRODUCTION_SERVER
    assert plan.metadata_persistence_backend is MetadataBackend.POSTGRESQL
    assert plan.exact_extraction_backend is ExactExtractionBackend.COMPILED_MATCHER
    assert plan.operator_lookup_backend is OperatorLookupBackend.POSTGRESQL_SEARCH
