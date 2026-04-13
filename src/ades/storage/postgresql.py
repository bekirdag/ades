"""PostgreSQL-backed metadata store for the production server tool."""

from __future__ import annotations

import atexit
from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from threading import Lock

from ..packs.manifest import PackManifest
from ..text_processing import normalize_lookup_text
from .paths import StorageLayout, iter_pack_manifest_paths

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError as exc:  # pragma: no cover - exercised only without the server extra.
    psycopg = None
    dict_row = None
    _PSYCOPG_IMPORT_ERROR = exc
else:
    _PSYCOPG_IMPORT_ERROR = None

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - exercised when pool extra is absent.
    ConnectionPool = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


SEARCH_TERM_RE = re.compile(r"[a-z0-9]+")
_POOL_MIN_SIZE = 2
_POOL_MAX_SIZE = 10
_POOL_TIMEOUT_SECONDS = 30.0
_POOL_NAME = "ades-metadata-store"
_POOL_CACHE_LOCK = Lock()
_POOL_CACHE: dict[str, object] = {}
_PG_TRGM_ALIAS_THRESHOLD = 0.2
_PG_TRGM_RULE_THRESHOLD = 0.15
_SCHEMA_LOCK_KEY = 48324001
_REQUIRED_TABLE_NAMES = frozenset(
    {
        "installed_packs",
        "pack_dependencies",
        "pack_labels",
        "pack_rules",
        "pack_aliases",
    }
)
_REQUIRED_PACK_ALIASES_COLUMNS = frozenset(
    {
        "normalized_alias_text",
        "canonical_text",
        "alias_score",
        "generated",
        "source_name",
        "entity_id",
        "source_priority",
        "popularity_weight",
    }
)
_REQUIRED_EXACT_INDEX_NAMES = frozenset(
    {
        "idx_installed_packs_active",
        "idx_pack_aliases_lookup",
        "idx_pack_rules_lookup",
    }
)


def _close_shared_pools() -> None:
    with _POOL_CACHE_LOCK:
        pools = list(_POOL_CACHE.values())
        _POOL_CACHE.clear()
    for pool in pools:
        close = getattr(pool, "close", None)
        if callable(close):
            close()


atexit.register(_close_shared_pools)


def _build_shared_connection_pool(database_url: str) -> object | None:
    if ConnectionPool is None:
        return None
    pool = ConnectionPool(
        conninfo=database_url,
        kwargs={"row_factory": dict_row},
        min_size=_POOL_MIN_SIZE,
        max_size=_POOL_MAX_SIZE,
        open=True,
        timeout=_POOL_TIMEOUT_SECONDS,
        name=_POOL_NAME,
    )
    pool.wait(timeout=_POOL_TIMEOUT_SECONDS)
    return pool


def _get_shared_connection_pool(database_url: str) -> object | None:
    if ConnectionPool is None:
        return None
    with _POOL_CACHE_LOCK:
        pool = _POOL_CACHE.get(database_url)
        if pool is not None:
            return pool
        pool = _build_shared_connection_pool(database_url)
        if pool is not None:
            _POOL_CACHE[database_url] = pool
        return pool


class PostgreSQLMetadataStore:
    """Persist installed-pack metadata and deterministic lookup content in PostgreSQL."""

    def __init__(self, layout: StorageLayout, *, database_url: str | None) -> None:
        if psycopg is None:
            raise RuntimeError(
                "The PostgreSQL metadata store requires the `ades[server]` extra "
                "with psycopg installed."
            ) from _PSYCOPG_IMPORT_ERROR
        if not database_url:
            raise RuntimeError(
                "The PostgreSQL metadata store requires ADES_DATABASE_URL or config.database_url."
            )
        self.layout = layout
        self.database_url = database_url
        self._pool = _get_shared_connection_pool(database_url)
        self._pg_trgm_enabled = False
        self.ensure_schema()

    def ensure_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS installed_packs (
                pack_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                version TEXT NOT NULL,
                language TEXT NOT NULL,
                domain TEXT NOT NULL,
                tier TEXT NOT NULL,
                min_ades_version TEXT NOT NULL,
                sha256 TEXT,
                install_path TEXT NOT NULL,
                manifest_path TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                installed_at TIMESTAMPTZ NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pack_dependencies (
                pack_id TEXT NOT NULL REFERENCES installed_packs(pack_id) ON DELETE CASCADE,
                dependency_pack_id TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (pack_id, dependency_pack_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pack_labels (
                pack_id TEXT NOT NULL REFERENCES installed_packs(pack_id) ON DELETE CASCADE,
                label TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (pack_id, label)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pack_rules (
                pack_id TEXT NOT NULL REFERENCES installed_packs(pack_id) ON DELETE CASCADE,
                rule_name TEXT NOT NULL,
                label TEXT NOT NULL,
                pattern TEXT NOT NULL,
                source_domain TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (pack_id, rule_name, pattern)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS pack_aliases (
                pack_id TEXT NOT NULL REFERENCES installed_packs(pack_id) ON DELETE CASCADE,
                alias_text TEXT NOT NULL,
                label TEXT NOT NULL,
                normalized_alias_text TEXT NOT NULL DEFAULT '',
                canonical_text TEXT NOT NULL DEFAULT '',
                alias_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
                generated BOOLEAN NOT NULL DEFAULT FALSE,
                source_name TEXT NOT NULL DEFAULT '',
                entity_id TEXT NOT NULL DEFAULT '',
                source_priority DOUBLE PRECISION NOT NULL DEFAULT 0.6,
                popularity_weight DOUBLE PRECISION NOT NULL DEFAULT 0.5,
                source_domain TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (pack_id, alias_text, label)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_installed_packs_active
            ON installed_packs(active, pack_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_aliases_lookup
            ON pack_aliases(normalized_alias_text, label)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_rules_lookup
            ON pack_rules(rule_name, label)
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS normalized_alias_text TEXT NOT NULL DEFAULT ''
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS canonical_text TEXT NOT NULL DEFAULT ''
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS alias_score DOUBLE PRECISION NOT NULL DEFAULT 1.0
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS generated BOOLEAN NOT NULL DEFAULT FALSE
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS source_name TEXT NOT NULL DEFAULT ''
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS entity_id TEXT NOT NULL DEFAULT ''
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS source_priority DOUBLE PRECISION NOT NULL DEFAULT 0.6
            """,
            """
            ALTER TABLE pack_aliases
            ADD COLUMN IF NOT EXISTS popularity_weight DOUBLE PRECISION NOT NULL DEFAULT 0.5
            """,
        ]
        with self._connect() as connection:
            schema_current, pg_trgm_enabled = self._schema_state(connection)
            if schema_current:
                self._pg_trgm_enabled = pg_trgm_enabled
                return

            connection.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_LOCK_KEY,))
            try:
                schema_current, pg_trgm_enabled = self._schema_state(connection)
                if not schema_current:
                    for statement in statements:
                        connection.execute(statement)
                    pg_trgm_enabled = self._enable_optional_pg_trgm(connection)
                self._pg_trgm_enabled = pg_trgm_enabled
            finally:
                connection.execute("SELECT pg_advisory_unlock(%s)", (_SCHEMA_LOCK_KEY,))

    def _schema_state(self, connection) -> tuple[bool, bool]:
        table_rows = connection.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
              AND tablename = ANY(%s)
            """,
            (list(_REQUIRED_TABLE_NAMES),),
        ).fetchall()
        table_names = {str(row["tablename"]) for row in table_rows}
        if table_names != _REQUIRED_TABLE_NAMES:
            return False, False

        alias_column_rows = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'pack_aliases'
              AND column_name = ANY(%s)
            """,
            (list(_REQUIRED_PACK_ALIASES_COLUMNS),),
        ).fetchall()
        alias_columns = {str(row["column_name"]) for row in alias_column_rows}
        if alias_columns != _REQUIRED_PACK_ALIASES_COLUMNS:
            return False, False

        index_rows = connection.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND indexname = ANY(%s)
            """,
            (list(_REQUIRED_EXACT_INDEX_NAMES),),
        ).fetchall()
        index_names = {str(row["indexname"]) for row in index_rows}
        if index_names != _REQUIRED_EXACT_INDEX_NAMES:
            return False, False

        return True, self._is_pg_trgm_enabled(connection)

    def count_installed_packs(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM installed_packs"
            ).fetchone()
        if row is None:
            return 0
        return int(row["count"])

    def sync_pack_from_dir(
        self,
        pack_dir: Path,
        *,
        active: bool | None = None,
    ) -> PackManifest | None:
        manifest_path = pack_dir / "manifest.json"
        if not manifest_path.exists():
            return None
        manifest = PackManifest.load(manifest_path)
        if active is not None:
            manifest = replace(manifest, active=active)
        self.sync_pack(manifest, pack_dir)
        return manifest

    def sync_pack(self, manifest: PackManifest, pack_dir: Path) -> None:
        manifest_path = pack_dir / "manifest.json"
        installed_at = manifest.installed_at or _utc_now()
        labels = self._load_labels(pack_dir)
        rules = self._load_rules(pack_dir, manifest.domain)
        aliases = self._load_aliases(pack_dir, manifest.domain)

        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO installed_packs (
                    pack_id,
                    schema_version,
                    version,
                    language,
                    domain,
                    tier,
                    min_ades_version,
                    sha256,
                    install_path,
                    manifest_path,
                    active,
                    installed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pack_id) DO UPDATE SET
                    schema_version = EXCLUDED.schema_version,
                    version = EXCLUDED.version,
                    language = EXCLUDED.language,
                    domain = EXCLUDED.domain,
                    tier = EXCLUDED.tier,
                    min_ades_version = EXCLUDED.min_ades_version,
                    sha256 = EXCLUDED.sha256,
                    install_path = EXCLUDED.install_path,
                    manifest_path = EXCLUDED.manifest_path,
                    active = EXCLUDED.active,
                    installed_at = EXCLUDED.installed_at
                """,
                (
                    manifest.pack_id,
                    manifest.schema_version,
                    manifest.version,
                    manifest.language,
                    manifest.domain,
                    manifest.tier,
                    manifest.min_ades_version,
                    manifest.sha256,
                    str(pack_dir),
                    str(manifest_path),
                    manifest.active,
                    installed_at,
                ),
            )
            self._replace_dependencies(connection, manifest)
            self._replace_labels(connection, manifest.pack_id, labels)
            self._replace_rules(connection, manifest.pack_id, rules)
            self._replace_aliases(connection, manifest.pack_id, aliases)

    def sync_from_filesystem(self, packs_dir: Path | None = None) -> None:
        root = packs_dir or self.layout.packs_dir
        if not root.exists():
            return
        for manifest_path in iter_pack_manifest_paths(root):
            self.sync_pack_from_dir(manifest_path.parent)

    def list_installed_packs(self, *, active_only: bool = False) -> list[PackManifest]:
        query = """
            SELECT *
            FROM installed_packs
        """
        params: list[object] = []
        if active_only:
            query += " WHERE active = TRUE"
        query += " ORDER BY pack_id ASC"

        stale_ids: list[str] = []
        packs: list[PackManifest] = []
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
            for row in rows:
                manifest = self._manifest_from_row(connection, row)
                if manifest is None:
                    stale_ids.append(str(row["pack_id"]))
                    continue
                packs.append(manifest)

        if stale_ids:
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "DELETE FROM installed_packs WHERE pack_id = %s",
                        [(pack_id,) for pack_id in stale_ids],
                    )
        return packs

    def get_pack(self, pack_id: str, *, active_only: bool = False) -> PackManifest | None:
        query = """
            SELECT *
            FROM installed_packs
            WHERE pack_id = %s
        """
        params: list[object] = [pack_id]
        if active_only:
            query += " AND active = TRUE"
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
            if row is None:
                return None
            manifest = self._manifest_from_row(connection, row)
        if manifest is not None:
            return manifest
        with self._connect() as connection:
            connection.execute("DELETE FROM installed_packs WHERE pack_id = %s", (pack_id,))
        return None

    def set_pack_active(self, pack_id: str, active: bool) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE installed_packs SET active = %s WHERE pack_id = %s",
                (active, pack_id),
            )
        return cursor.rowcount > 0

    def delete_pack(self, pack_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM installed_packs WHERE pack_id = %s",
                (pack_id,),
            )
        return cursor.rowcount > 0

    def list_pack_labels(self, pack_id: str) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT label
                FROM pack_labels
                WHERE pack_id = %s
                ORDER BY position ASC, label ASC
                """,
                (pack_id,),
            ).fetchall()
        return [str(row["label"]) for row in rows]

    def list_pack_rules(self, pack_id: str) -> list[dict[str, str]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT rule_name, label, pattern, source_domain
                FROM pack_rules
                WHERE pack_id = %s
                ORDER BY position ASC, rule_name ASC
                """,
                (pack_id,),
            ).fetchall()
        return [
            {
                "name": str(row["rule_name"]),
                "label": str(row["label"]),
                "pattern": str(row["pattern"]),
                "source_domain": str(row["source_domain"]),
            }
            for row in rows
        ]

    def list_pack_aliases(self, pack_id: str) -> list[dict[str, str | float | bool]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT alias_text, label, normalized_alias_text, canonical_text, alias_score,
                       generated, source_name, entity_id, source_priority, popularity_weight,
                       source_domain
                FROM pack_aliases
                WHERE pack_id = %s
                ORDER BY position ASC, alias_text ASC
                """,
                (pack_id,),
            ).fetchall()
        return [
            {
                "text": str(row["alias_text"]),
                "label": str(row["label"]),
                "normalized_text": str(row["normalized_alias_text"]),
                "canonical_text": str(row["canonical_text"]),
                "alias_score": float(row["alias_score"]),
                "generated": bool(row["generated"]),
                "source_name": str(row["source_name"]),
                "entity_id": str(row["entity_id"]),
                "source_priority": float(row["source_priority"]),
                "popularity_weight": float(row["popularity_weight"]),
                "source_domain": str(row["source_domain"]),
            }
            for row in rows
        ]

    def lookup_candidates(
        self,
        query: str,
        *,
        pack_id: str | None = None,
        exact_alias: bool = False,
        fuzzy: bool = False,
        active_only: bool = True,
        limit: int = 20,
    ) -> list[dict[str, str | float | bool | None]]:
        normalized = query.strip()
        if not normalized:
            return []
        if exact_alias and fuzzy:
            raise ValueError("Lookup cannot request both exact_alias and fuzzy mode.")

        alias_candidates = self._lookup_alias_candidates(
            normalized,
            pack_id=pack_id,
            exact_alias=exact_alias,
            fuzzy=fuzzy,
            active_only=active_only,
            limit=limit,
        )
        if exact_alias:
            return alias_candidates[:limit]

        rule_candidates = self._lookup_rule_candidates(
            normalized,
            pack_id=pack_id,
            fuzzy=fuzzy,
            active_only=active_only,
            limit=limit,
        )
        combined = alias_candidates + rule_candidates
        combined.sort(
            key=lambda item: (
                -float(item["score"]),
                str(item["kind"]),
                str(item["pack_id"]),
                str(item["value"]).lower(),
            )
        )
        return combined[:limit]

    def _manifest_from_row(self, connection, row: dict[str, object]) -> PackManifest | None:
        manifest_path = Path(str(row["manifest_path"]))
        if not manifest_path.exists():
            return None
        if manifest_path.parent.name.startswith("."):
            return None

        source_manifest = PackManifest.load(manifest_path)
        return PackManifest(
            schema_version=source_manifest.schema_version,
            pack_id=source_manifest.pack_id,
            version=source_manifest.version,
            language=source_manifest.language,
            domain=source_manifest.domain,
            tier=source_manifest.tier,
            description=source_manifest.description,
            tags=list(source_manifest.tags),
            dependencies=list(source_manifest.dependencies),
            artifacts=list(source_manifest.artifacts),
            models=list(source_manifest.models),
            rules=list(source_manifest.rules),
            labels=list(source_manifest.labels),
            matcher=source_manifest.matcher,
            min_ades_version=source_manifest.min_ades_version,
            min_entities_per_100_tokens_warning=source_manifest.min_entities_per_100_tokens_warning,
            sha256=row["sha256"],
            active=bool(row["active"]),
            install_path=str(row["install_path"]),
            manifest_path=str(row["manifest_path"]),
            installed_at=str(row["installed_at"]),
        )

    def _replace_dependencies(self, connection, manifest: PackManifest) -> None:
        connection.execute("DELETE FROM pack_dependencies WHERE pack_id = %s", (manifest.pack_id,))
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO pack_dependencies (pack_id, dependency_pack_id, position)
                VALUES (%s, %s, %s)
                """,
                [
                    (manifest.pack_id, dependency, position)
                    for position, dependency in enumerate(manifest.dependencies)
                ],
            )

    def _replace_labels(self, connection, pack_id: str, labels: list[str]) -> None:
        connection.execute("DELETE FROM pack_labels WHERE pack_id = %s", (pack_id,))
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO pack_labels (pack_id, label, position)
                VALUES (%s, %s, %s)
                """,
                [(pack_id, label, position) for position, label in enumerate(labels)],
            )

    def _replace_rules(self, connection, pack_id: str, rules: list[dict[str, str]]) -> None:
        connection.execute("DELETE FROM pack_rules WHERE pack_id = %s", (pack_id,))
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO pack_rules (
                    pack_id,
                    rule_name,
                    label,
                    pattern,
                    source_domain,
                    position
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        pack_id,
                        rule["name"],
                        rule["label"],
                        rule["pattern"],
                        rule["source_domain"],
                        position,
                    )
                    for position, rule in enumerate(rules)
                ],
            )

    def _replace_aliases(
        self,
        connection,
        pack_id: str,
        aliases: list[dict[str, str | float | bool]],
    ) -> None:
        connection.execute("DELETE FROM pack_aliases WHERE pack_id = %s", (pack_id,))
        rows = [
            (
                pack_id,
                alias["text"],
                alias["label"],
                alias["normalized_text"],
                alias["canonical_text"],
                alias["alias_score"],
                bool(alias["generated"]),
                alias["source_name"],
                alias["entity_id"],
                alias["source_priority"],
                alias["popularity_weight"],
                alias["source_domain"],
                position,
            )
            for position, alias in enumerate(aliases)
        ]
        if not rows:
            return
        insert_sql = """
            INSERT INTO pack_aliases (
                pack_id,
                alias_text,
                label,
                normalized_alias_text,
                canonical_text,
                alias_score,
                generated,
                source_name,
                entity_id,
                source_priority,
                popularity_weight,
                source_domain,
                position
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        copy_sql = """
            COPY pack_aliases (
                pack_id,
                alias_text,
                label,
                normalized_alias_text,
                canonical_text,
                alias_score,
                generated,
                source_name,
                entity_id,
                source_priority,
                popularity_weight,
                source_domain,
                position
            ) FROM STDIN
        """
        with connection.cursor() as cursor:
            copy_method = getattr(cursor, "copy", None)
            if callable(copy_method):
                with copy_method(copy_sql) as copy:
                    for row in rows:
                        copy.write_row(row)
                return
            cursor.executemany(insert_sql, rows)

    def _lookup_alias_candidates(
        self,
        query: str,
        *,
        pack_id: str | None,
        exact_alias: bool,
        fuzzy: bool,
        active_only: bool,
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        filters = []
        params: list[object] = []
        if active_only:
            filters.append("p.active = TRUE")
        if pack_id is not None:
            filters.append("a.pack_id = %s")
            params.append(pack_id)

        if exact_alias:
            filters.append("a.normalized_alias_text = %s")
            params.append(normalize_lookup_text(query))
        elif fuzzy and getattr(self, "_pg_trgm_enabled", False):
            return self._lookup_alias_candidates_pg_trgm(
                query,
                filters=filters,
                params=params,
                limit=limit,
            )
        else:
            like_query = f"%{query.lower()}%"
            filters.append(
                "(LOWER(a.alias_text) LIKE %s OR LOWER(a.label) LIKE %s OR LOWER(a.source_domain) LIKE %s)"
            )
            params.extend([like_query, like_query, like_query])

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    a.pack_id,
                    a.alias_text,
                    a.label,
                    a.canonical_text,
                    a.alias_score,
                    a.generated,
                    a.source_name,
                    a.entity_id,
                    a.source_priority,
                    a.popularity_weight,
                    a.source_domain,
                    p.active
                FROM pack_aliases AS a
                JOIN installed_packs AS p ON p.pack_id = a.pack_id
                {where_clause}
                ORDER BY a.alias_text ASC, a.label ASC
                LIMIT %s
                """,
                params,
            ).fetchall()
        return [
            {
                "kind": "alias",
                "pack_id": str(row["pack_id"]),
                "label": str(row["label"]),
                "value": str(row["alias_text"]),
                "canonical_text": str(row["canonical_text"]),
                "generated": bool(row["generated"]),
                "source_name": str(row["source_name"]),
                "entity_id": str(row["entity_id"]),
                "entity_prior": float(row["alias_score"]),
                "source_priority": float(row["source_priority"]),
                "popularity_weight": float(row["popularity_weight"]),
                "pattern": None,
                "domain": str(row["source_domain"]),
                "active": bool(row["active"]),
                "score": self._alias_score(
                    query,
                    alias_text=str(row["alias_text"]),
                    label=str(row["label"]),
                    source_domain=str(row["source_domain"]),
                    alias_prior=float(row["alias_score"]),
                    exact_alias=exact_alias,
                    fuzzy=fuzzy,
                    match_score=None,
                ),
            }
            for row in rows
        ]

    def _lookup_rule_candidates(
        self,
        query: str,
        *,
        pack_id: str | None,
        fuzzy: bool,
        active_only: bool,
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        filters = []
        params: list[object] = []
        if active_only:
            filters.append("p.active = TRUE")
        if pack_id is not None:
            filters.append("r.pack_id = %s")
            params.append(pack_id)

        if fuzzy and getattr(self, "_pg_trgm_enabled", False):
            return self._lookup_rule_candidates_pg_trgm(
                query,
                filters=filters,
                params=params,
                limit=limit,
            )
        like_query = f"%{query.lower()}%"
        filters.append(
            "(LOWER(r.rule_name) LIKE %s OR LOWER(r.label) LIKE %s OR LOWER(r.pattern) LIKE %s OR LOWER(r.source_domain) LIKE %s)"
        )
        params.extend([like_query, like_query, like_query, like_query])

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    r.pack_id,
                    r.rule_name,
                    r.label,
                    r.pattern,
                    r.source_domain,
                    p.active
                FROM pack_rules AS r
                JOIN installed_packs AS p ON p.pack_id = r.pack_id
                {where_clause}
                ORDER BY r.rule_name ASC, r.label ASC
                LIMIT %s
                """,
                params,
            ).fetchall()
        return [
            {
                "kind": "rule",
                "pack_id": str(row["pack_id"]),
                "label": str(row["label"]),
                "value": str(row["rule_name"]),
                "pattern": str(row["pattern"]),
                "domain": str(row["source_domain"]),
                "active": bool(row["active"]),
                "score": self._rule_score(
                    query,
                    rule_name=str(row["rule_name"]),
                    label=str(row["label"]),
                    pattern=str(row["pattern"]),
                    source_domain=str(row["source_domain"]),
                    fuzzy=fuzzy,
                    match_score=None,
                ),
            }
            for row in rows
        ]

    def _lookup_alias_candidates_pg_trgm(
        self,
        query: str,
        *,
        filters: list[str],
        params: list[object],
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        trigram_params = [query, query, query]
        query_params = trigram_params + params + [_PG_TRGM_ALIAS_THRESHOLD, limit]
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM (
                    SELECT
                        a.pack_id,
                        a.alias_text,
                        a.label,
                        a.canonical_text,
                        a.alias_score,
                        a.generated,
                        a.source_name,
                        a.entity_id,
                        a.source_priority,
                        a.popularity_weight,
                        a.source_domain,
                        p.active,
                        GREATEST(
                            similarity(a.alias_text, %s),
                            similarity(a.label, %s),
                            similarity(a.source_domain, %s)
                        ) AS match_score
                    FROM pack_aliases AS a
                    JOIN installed_packs AS p ON p.pack_id = a.pack_id
                    {where_clause}
                ) AS ranked
                WHERE ranked.match_score >= %s
                ORDER BY ranked.match_score DESC, ranked.alias_score DESC, ranked.alias_text ASC, ranked.label ASC
                LIMIT %s
                """,
                query_params,
            ).fetchall()
        return [
            {
                "kind": "alias",
                "pack_id": str(row["pack_id"]),
                "label": str(row["label"]),
                "value": str(row["alias_text"]),
                "canonical_text": str(row["canonical_text"]),
                "generated": bool(row["generated"]),
                "source_name": str(row["source_name"]),
                "entity_id": str(row["entity_id"]),
                "entity_prior": float(row["alias_score"]),
                "source_priority": float(row["source_priority"]),
                "popularity_weight": float(row["popularity_weight"]),
                "pattern": None,
                "domain": str(row["source_domain"]),
                "active": bool(row["active"]),
                "score": self._alias_score(
                    query,
                    alias_text=str(row["alias_text"]),
                    label=str(row["label"]),
                    source_domain=str(row["source_domain"]),
                    alias_prior=float(row["alias_score"]),
                    exact_alias=False,
                    fuzzy=True,
                    match_score=float(row["match_score"]),
                ),
            }
            for row in rows
        ]

    def _lookup_rule_candidates_pg_trgm(
        self,
        query: str,
        *,
        filters: list[str],
        params: list[object],
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        trigram_params = [query, query, query, query]
        query_params = trigram_params + params + [_PG_TRGM_RULE_THRESHOLD, limit]
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM (
                    SELECT
                        r.pack_id,
                        r.rule_name,
                        r.label,
                        r.pattern,
                        r.source_domain,
                        p.active,
                        GREATEST(
                            similarity(r.rule_name, %s),
                            similarity(r.label, %s),
                            similarity(r.pattern, %s),
                            similarity(r.source_domain, %s)
                        ) AS match_score
                    FROM pack_rules AS r
                    JOIN installed_packs AS p ON p.pack_id = r.pack_id
                    {where_clause}
                ) AS ranked
                WHERE ranked.match_score >= %s
                ORDER BY ranked.match_score DESC, ranked.rule_name ASC, ranked.label ASC
                LIMIT %s
                """,
                query_params,
            ).fetchall()
        return [
            {
                "kind": "rule",
                "pack_id": str(row["pack_id"]),
                "label": str(row["label"]),
                "value": str(row["rule_name"]),
                "pattern": str(row["pattern"]),
                "domain": str(row["source_domain"]),
                "active": bool(row["active"]),
                "score": self._rule_score(
                    query,
                    rule_name=str(row["rule_name"]),
                    label=str(row["label"]),
                    pattern=str(row["pattern"]),
                    source_domain=str(row["source_domain"]),
                    fuzzy=True,
                    match_score=float(row["match_score"]),
                ),
            }
            for row in rows
        ]

    def _connect(self):
        if getattr(self, "_pool", None) is not None:
            return self._pool.connection()
        return psycopg.connect(self.database_url, row_factory=dict_row)

    def _is_pg_trgm_enabled(self, connection) -> bool:
        row = connection.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_extension
                WHERE extname = 'pg_trgm'
            ) AS enabled
            """
        ).fetchone()
        return bool(row and row["enabled"])

    def _enable_optional_pg_trgm(self, connection) -> bool:
        statements = [
            "CREATE EXTENSION IF NOT EXISTS pg_trgm",
            """
            CREATE INDEX IF NOT EXISTS idx_pack_aliases_alias_text_trgm
            ON pack_aliases USING GIN (alias_text gin_trgm_ops)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_aliases_label_trgm
            ON pack_aliases USING GIN (label gin_trgm_ops)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_aliases_source_domain_trgm
            ON pack_aliases USING GIN (source_domain gin_trgm_ops)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_rules_rule_name_trgm
            ON pack_rules USING GIN (rule_name gin_trgm_ops)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_rules_label_trgm
            ON pack_rules USING GIN (label gin_trgm_ops)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_rules_pattern_trgm
            ON pack_rules USING GIN (pattern gin_trgm_ops)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_pack_rules_source_domain_trgm
            ON pack_rules USING GIN (source_domain gin_trgm_ops)
            """,
        ]
        try:
            for statement in statements:
                connection.execute(statement)
        except Exception:
            return False
        return True

    @staticmethod
    def _alias_score(
        query: str,
        *,
        alias_text: str,
        label: str,
        source_domain: str,
        alias_prior: float,
        exact_alias: bool,
        fuzzy: bool,
        match_score: float | None,
    ) -> float:
        normalized_query = query.lower()
        normalized_alias = alias_text.lower()
        normalized_label = label.lower()
        normalized_domain = source_domain.lower()
        coverage = PostgreSQLMetadataStore._field_coverage(
            query,
            alias_text,
            label,
            source_domain,
        )
        if exact_alias or normalized_alias == normalized_query:
            return round(min(1.0, 0.7 + (alias_prior * 0.3)), 4)
        if fuzzy and match_score is not None:
            score = 0.35 + (min(1.0, max(0.0, match_score)) * 0.4)
            score += coverage * 0.1
            score = (score * 0.75) + (alias_prior * 0.25)
            return round(min(0.97, score), 4)
        if coverage == 0.0:
            return 0.0
        if normalized_alias.startswith(normalized_query):
            return round(min(0.98, 0.75 + (alias_prior * 0.23)), 4)
        if normalized_query in normalized_alias:
            return round(min(0.94, 0.65 + (alias_prior * 0.24)), 4)
        score = 0.45 + (coverage * 0.35)
        if normalized_query and normalized_query in normalized_label:
            score += 0.04
        if normalized_query and normalized_query in normalized_domain:
            score += 0.03
        if coverage == 1.0 and len(PostgreSQLMetadataStore._query_terms(query)) > 1:
            score += 0.05
        score = (score * 0.75) + (alias_prior * 0.25)
        return round(min(0.95, score), 4)

    @staticmethod
    def _rule_score(
        query: str,
        *,
        rule_name: str,
        label: str,
        pattern: str,
        source_domain: str,
        fuzzy: bool,
        match_score: float | None,
    ) -> float:
        normalized_query = query.lower()
        coverage = PostgreSQLMetadataStore._field_coverage(
            query,
            rule_name,
            label,
            pattern,
            source_domain,
        )
        if rule_name.lower() == normalized_query:
            return 0.9
        if fuzzy and match_score is not None:
            score = 0.3 + (min(1.0, max(0.0, match_score)) * 0.45) + (coverage * 0.1)
            return round(min(0.9, score), 4)
        if coverage == 0.0:
            return 0.0
        if normalized_query in rule_name.lower():
            return 0.75
        if normalized_query in pattern.lower():
            return 0.6
        score = 0.35 + (coverage * 0.35)
        if coverage == 1.0 and len(PostgreSQLMetadataStore._query_terms(query)) > 1:
            score += 0.05
        return round(min(0.85, score), 4)

    @staticmethod
    def _query_terms(query: str) -> list[str]:
        terms = [match.group(0) for match in SEARCH_TERM_RE.finditer(query.casefold())]
        deduped: list[str] = []
        seen: set[str] = set()
        for term in terms:
            if term in seen:
                continue
            seen.add(term)
            deduped.append(term)
        return deduped

    @staticmethod
    def _field_coverage(query: str, *fields: str) -> float:
        terms = PostgreSQLMetadataStore._query_terms(query)
        if not terms:
            return 0.0
        normalized_fields = [field.casefold() for field in fields]
        matched = sum(
            1
            for term in terms
            if any(term in field for field in normalized_fields)
        )
        return matched / len(terms)

    @staticmethod
    def _load_labels(pack_dir: Path) -> list[str]:
        path = pack_dir / "labels.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        return [str(item) for item in data]

    @staticmethod
    def _load_rules(pack_dir: Path, domain: str) -> list[dict[str, str]]:
        path = pack_dir / "rules.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        patterns = data.get("patterns", [])
        return [
            {
                "name": str(item["name"]),
                "label": str(item.get("label", item["name"])),
                "pattern": str(item["pattern"]),
                "source_domain": domain,
            }
            for item in patterns
        ]

    @staticmethod
    def _load_aliases(pack_dir: Path, domain: str) -> list[dict[str, str | float | bool]]:
        path = pack_dir / "aliases.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text(encoding="utf-8"))
        aliases = data.get("aliases", [])
        loaded: list[dict[str, str | float | bool]] = []
        for item in aliases:
            if isinstance(item, str):
                loaded.append(
                    {
                        "text": item,
                        "label": "alias",
                        "normalized_text": normalize_lookup_text(item),
                        "canonical_text": str(item),
                        "alias_score": 1.0,
                        "generated": False,
                        "source_name": "",
                        "entity_id": "",
                        "source_priority": 0.6,
                        "popularity_weight": 0.5,
                        "source_domain": domain,
                    }
                )
                continue
            loaded.append(
                {
                    "text": str(item["text"]),
                    "label": str(item.get("label", "alias")),
                    "normalized_text": str(
                        item.get("normalized_text") or normalize_lookup_text(str(item["text"]))
                    ),
                    "canonical_text": str(
                        item.get("canonical_text") or item.get("text") or ""
                    ),
                    "alias_score": float(item.get("score", item.get("alias_score", 1.0))),
                    "generated": bool(item.get("generated", False)),
                    "source_name": str(item.get("source_name", "")),
                    "entity_id": str(item.get("entity_id", "")),
                    "source_priority": float(item.get("source_priority", 0.6)),
                    "popularity_weight": float(item.get("popularity_weight", 0.5)),
                    "source_domain": domain,
                }
            )
        return loaded
