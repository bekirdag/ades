"""SQLite-backed metadata store for installed packs."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Iterator

from ..packs.manifest import PackManifest
from ..text_processing import normalize_lookup_text
from .paths import StorageLayout, iter_pack_manifest_paths


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


SEARCH_TERM_RE = re.compile(r"[a-z0-9]+")
ALIAS_SEARCH_TABLE = "pack_alias_search"
RULE_SEARCH_TABLE = "pack_rule_search"
ALIAS_SYNC_BATCH_SIZE = 50000
ALIAS_JSON_READ_CHUNK_SIZE = 1024 * 1024
MAX_ALIAS_SEARCH_INDEX_ROWS = 5_000_000
SQLITE_CONNECT_TIMEOUT_SECONDS = 60.0
SQLITE_BUSY_TIMEOUT_MS = 60_000


class PackMetadataStore:
    """Persist installed-pack metadata and deterministic lookup content."""

    def __init__(self, layout: StorageLayout) -> None:
        self.layout = layout
        self._search_index_supported = False
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
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
                    active INTEGER NOT NULL DEFAULT 1,
                    installed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pack_dependencies (
                    pack_id TEXT NOT NULL,
                    dependency_pack_id TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (pack_id, dependency_pack_id),
                    FOREIGN KEY (pack_id) REFERENCES installed_packs(pack_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS pack_labels (
                    pack_id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (pack_id, label),
                    FOREIGN KEY (pack_id) REFERENCES installed_packs(pack_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS pack_rules (
                    pack_id TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    label TEXT NOT NULL,
                    pattern TEXT NOT NULL,
                    source_domain TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (pack_id, rule_name, pattern),
                    FOREIGN KEY (pack_id) REFERENCES installed_packs(pack_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS pack_aliases (
                    pack_id TEXT NOT NULL,
                    alias_text TEXT NOT NULL,
                    label TEXT NOT NULL,
                    normalized_alias_text TEXT NOT NULL DEFAULT '',
                    canonical_text TEXT NOT NULL DEFAULT '',
                    alias_score REAL NOT NULL DEFAULT 1.0,
                    generated INTEGER NOT NULL DEFAULT 0,
                    source_name TEXT NOT NULL DEFAULT '',
                    entity_id TEXT NOT NULL DEFAULT '',
                    source_priority REAL NOT NULL DEFAULT 0.6,
                    popularity_weight REAL NOT NULL DEFAULT 0.5,
                    source_domain TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (pack_id, alias_text, label),
                    FOREIGN KEY (pack_id) REFERENCES installed_packs(pack_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_installed_packs_active
                ON installed_packs(active, pack_id);

                CREATE INDEX IF NOT EXISTS idx_pack_aliases_lookup
                ON pack_aliases(normalized_alias_text, label);

                CREATE INDEX IF NOT EXISTS idx_pack_aliases_pack_lookup
                ON pack_aliases(pack_id, normalized_alias_text);

                CREATE INDEX IF NOT EXISTS idx_pack_rules_lookup
                ON pack_rules(rule_name, label);
                """
            )
            self._ensure_search_schema(connection)
            self._ensure_pack_alias_columns(connection)
            self._ensure_pack_alias_lookup_indexes(connection)
            self._search_index_supported = self._search_index_enabled(connection)

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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pack_id) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    version = excluded.version,
                    language = excluded.language,
                    domain = excluded.domain,
                    tier = excluded.tier,
                    min_ades_version = excluded.min_ades_version,
                    sha256 = excluded.sha256,
                    install_path = excluded.install_path,
                    manifest_path = excluded.manifest_path,
                    active = excluded.active,
                    installed_at = excluded.installed_at
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
                    1 if manifest.active else 0,
                    installed_at,
                ),
            )
            self._replace_dependencies(connection, manifest)
            self._replace_labels(connection, manifest.pack_id, labels)
            self._replace_rules(connection, manifest.pack_id, rules)
            self._replace_aliases(
                connection,
                manifest.pack_id,
                self._iter_aliases(pack_dir, manifest.domain),
            )

    def sync_from_filesystem(self, packs_dir: Path | None = None) -> None:
        root = packs_dir or self.layout.packs_dir
        if not root.exists():
            return
        for manifest_path in iter_pack_manifest_paths(root):
            self.sync_pack_from_dir(manifest_path.parent)

    def list_installed_packs(self, *, active_only: bool = False) -> list[PackManifest]:
        clause = "WHERE active = 1" if active_only else ""
        stale_ids: list[str] = []
        packs: list[PackManifest] = []
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM installed_packs
                {clause}
                ORDER BY pack_id ASC
                """
            ).fetchall()
            for row in rows:
                manifest = self._manifest_from_row(connection, row)
                if manifest is None:
                    stale_ids.append(str(row["pack_id"]))
                    continue
                packs.append(manifest)

        if stale_ids:
            with self._connect() as connection:
                connection.executemany(
                    "DELETE FROM installed_packs WHERE pack_id = ?",
                    [(pack_id,) for pack_id in stale_ids],
                )
        return packs

    def get_pack(self, pack_id: str, *, active_only: bool = False) -> PackManifest | None:
        query = """
            SELECT *
            FROM installed_packs
            WHERE pack_id = ?
        """
        params: list[object] = [pack_id]
        if active_only:
            query += " AND active = 1"
        with self._connect() as connection:
            row = connection.execute(query, params).fetchone()
            if row is None:
                return None
            manifest = self._manifest_from_row(connection, row)
        if manifest is not None:
            return manifest
        with self._connect() as connection:
            connection.execute("DELETE FROM installed_packs WHERE pack_id = ?", (pack_id,))
        return None

    def pack_exists(self, pack_id: str, *, active_only: bool = False) -> bool:
        return self.get_pack(pack_id, active_only=active_only) is not None

    def set_pack_active(self, pack_id: str, active: bool) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE installed_packs SET active = ? WHERE pack_id = ?",
                (1 if active else 0, pack_id),
            )
        return cursor.rowcount > 0

    def repair_pack_installation_paths(
        self,
        pack_id: str,
        *,
        install_path: str,
        manifest_path: str,
        active: bool | None = None,
    ) -> bool:
        assignments = [
            "install_path = ?",
            "manifest_path = ?",
        ]
        params: list[object] = [install_path, manifest_path]
        if active is not None:
            assignments.append("active = ?")
            params.append(1 if active else 0)
        params.append(pack_id)
        with self._connect() as connection:
            cursor = connection.execute(
                f"UPDATE installed_packs SET {', '.join(assignments)} WHERE pack_id = ?",
                params,
            )
        return cursor.rowcount > 0

    def delete_pack(self, pack_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM installed_packs WHERE pack_id = ?",
                (pack_id,),
            )
        return cursor.rowcount > 0

    def list_pack_labels(self, pack_id: str) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT label
                FROM pack_labels
                WHERE pack_id = ?
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
                WHERE pack_id = ?
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
                WHERE pack_id = ?
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

    def count_pack_aliases(self, pack_id: str) -> int:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM pack_aliases
                WHERE pack_id = ?
                """,
                (pack_id,),
            ).fetchone()
        if row is None:
            return 0
        return int(row["count"])

    def iter_pack_aliases(
        self,
        pack_id: str,
    ) -> Iterator[dict[str, str | float | bool]]:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT alias_text, label, normalized_alias_text, canonical_text, alias_score,
                       generated, source_name, entity_id, source_priority, popularity_weight,
                       source_domain
                FROM pack_aliases
                WHERE pack_id = ?
                ORDER BY alias_text ASC, label ASC
                """,
                (pack_id,),
            )
            while True:
                rows = cursor.fetchmany(10_000)
                if not rows:
                    break
                for row in rows:
                    yield {
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
            active_only=active_only,
            limit=limit,
        )
        if exact_alias:
            return alias_candidates[:limit]

        rule_candidates = self._lookup_rule_candidates(
            normalized,
            pack_id=pack_id,
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

    def _manifest_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> PackManifest | None:
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

    def _replace_dependencies(
        self,
        connection: sqlite3.Connection,
        manifest: PackManifest,
    ) -> None:
        connection.execute("DELETE FROM pack_dependencies WHERE pack_id = ?", (manifest.pack_id,))
        connection.executemany(
            """
            INSERT INTO pack_dependencies (pack_id, dependency_pack_id, position)
            VALUES (?, ?, ?)
            """,
            [
                (manifest.pack_id, dependency, position)
                for position, dependency in enumerate(manifest.dependencies)
            ],
        )

    def _replace_labels(
        self,
        connection: sqlite3.Connection,
        pack_id: str,
        labels: list[str],
    ) -> None:
        connection.execute("DELETE FROM pack_labels WHERE pack_id = ?", (pack_id,))
        connection.executemany(
            """
            INSERT INTO pack_labels (pack_id, label, position)
            VALUES (?, ?, ?)
            """,
            [(pack_id, label, position) for position, label in enumerate(labels)],
        )

    def _replace_rules(
        self,
        connection: sqlite3.Connection,
        pack_id: str,
        rules: list[dict[str, str]],
    ) -> None:
        connection.execute("DELETE FROM pack_rules WHERE pack_id = ?", (pack_id,))
        connection.executemany(
            """
            INSERT INTO pack_rules (
                pack_id,
                rule_name,
                label,
                pattern,
                source_domain,
                position
            ) VALUES (?, ?, ?, ?, ?, ?)
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
        self._replace_rule_search(connection, pack_id, rules)

    def _replace_aliases(
        self,
        connection: sqlite3.Connection,
        pack_id: str,
        aliases: Iterator[dict[str, str | float | bool]],
    ) -> None:
        connection.execute("DELETE FROM pack_aliases WHERE pack_id = ?", (pack_id,))
        search_supported = self._search_index_enabled(connection)
        if search_supported:
            connection.execute(
                f"DELETE FROM {ALIAS_SEARCH_TABLE} WHERE pack_id = ?",
                (pack_id,),
            )
        # Large generated packs can contain millions of aliases. Commit the
        # cleared state up front so the follow-on batch import does not stay
        # trapped behind one giant long-lived WAL transaction.
        connection.commit()

        alias_rows: list[tuple[object, ...]] = []
        emitted_alias_count = 0
        for position, alias in enumerate(aliases):
            emitted_alias_count = position + 1
            alias_rows.append(
                (
                    pack_id,
                    alias["text"],
                    alias["label"],
                    alias["normalized_text"],
                    alias["canonical_text"],
                    alias["alias_score"],
                    1 if bool(alias["generated"]) else 0,
                    alias["source_name"],
                    alias["entity_id"],
                    alias["source_priority"],
                    alias["popularity_weight"],
                    alias["source_domain"],
                    position,
                )
            )
            if len(alias_rows) >= ALIAS_SYNC_BATCH_SIZE:
                self._flush_alias_batch(connection, alias_rows)
                alias_rows = []

        if alias_rows:
            self._flush_alias_batch(connection, alias_rows)
        if search_supported and emitted_alias_count <= MAX_ALIAS_SEARCH_INDEX_ROWS:
            self._replace_alias_search_from_alias_table(connection, pack_id)
        elif emitted_alias_count > MAX_ALIAS_SEARCH_INDEX_ROWS:
            self._search_index_supported = False
        connection.commit()

    def _lookup_alias_candidates(
        self,
        query: str,
        *,
        pack_id: str | None,
        exact_alias: bool,
        active_only: bool,
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        filters = []
        params: list[object] = []
        if active_only:
            filters.append("p.active = 1")
        if pack_id is not None:
            filters.append("a.pack_id = ?")
            params.append(pack_id)

        if exact_alias:
            filters.append("a.normalized_alias_text = ?")
            params.append(normalize_lookup_text(query))
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
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
            candidates = [
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
                        exact_alias=True,
                    ),
                }
                for row in rows
            ]
            candidates.sort(
                key=lambda item: (
                    str(item["value"]).lower(),
                    str(item["label"]).lower(),
                    str(item["pack_id"]).lower(),
                )
            )
            return candidates[:limit]
        else:
            match_query = self._build_search_match_query(query)
            if self._search_index_supported and match_query is not None:
                return self._lookup_alias_candidates_fts(
                    query,
                    match_query=match_query,
                    pack_id=pack_id,
                    active_only=active_only,
                    limit=limit,
                )
            like_query = f"%{query.lower()}%"
            filters.append(
                "(LOWER(a.alias_text) LIKE ? OR LOWER(a.label) LIKE ? OR LOWER(a.source_domain) LIKE ?)"
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
                LIMIT ?
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
                ),
            }
            for row in rows
        ]

    def _lookup_rule_candidates(
        self,
        query: str,
        *,
        pack_id: str | None,
        active_only: bool,
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        filters = []
        params: list[object] = []
        if active_only:
            filters.append("p.active = 1")
        if pack_id is not None:
            filters.append("r.pack_id = ?")
            params.append(pack_id)

        like_query = f"%{query.lower()}%"
        match_query = self._build_search_match_query(query)
        if self._search_index_supported and match_query is not None:
            return self._lookup_rule_candidates_fts(
                query,
                match_query=match_query,
                pack_id=pack_id,
                active_only=active_only,
                limit=limit,
            )
        filters.append(
            "(LOWER(r.rule_name) LIKE ? OR LOWER(r.label) LIKE ? OR LOWER(r.pattern) LIKE ? OR LOWER(r.source_domain) LIKE ?)"
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
                LIMIT ?
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
                ),
            }
            for row in rows
        ]

    def _lookup_alias_candidates_fts(
        self,
        query: str,
        *,
        match_query: str,
        pack_id: str | None,
        active_only: bool,
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        filters = [f"{ALIAS_SEARCH_TABLE} MATCH ?"]
        params: list[object] = [match_query]
        if active_only:
            filters.append("p.active = 1")
        if pack_id is not None:
            filters.append(f"{ALIAS_SEARCH_TABLE}.pack_id = ?")
            params.append(pack_id)
        where_clause = f"WHERE {' AND '.join(filters)}"
        params.append(limit)
        with self._connect() as connection:
            try:
                rows = connection.execute(
                    f"""
                    SELECT
                        {ALIAS_SEARCH_TABLE}.pack_id,
                        {ALIAS_SEARCH_TABLE}.alias_text,
                        {ALIAS_SEARCH_TABLE}.label,
                        a.canonical_text,
                        a.alias_score,
                        a.generated,
                        a.source_name,
                        a.entity_id,
                        a.source_priority,
                        a.popularity_weight,
                        {ALIAS_SEARCH_TABLE}.source_domain,
                        p.active
                    FROM {ALIAS_SEARCH_TABLE}
                    JOIN pack_aliases AS a
                      ON a.pack_id = {ALIAS_SEARCH_TABLE}.pack_id
                     AND a.alias_text = {ALIAS_SEARCH_TABLE}.alias_text
                     AND a.label = {ALIAS_SEARCH_TABLE}.label
                    JOIN installed_packs AS p ON p.pack_id = {ALIAS_SEARCH_TABLE}.pack_id
                    {where_clause}
                    ORDER BY bm25({ALIAS_SEARCH_TABLE}, 3.0, 1.5, 0.75) ASC,
                             {ALIAS_SEARCH_TABLE}.alias_text ASC,
                             {ALIAS_SEARCH_TABLE}.label ASC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
            except sqlite3.OperationalError:
                self._search_index_supported = False
                return self._lookup_alias_candidates(
                    query,
                    pack_id=pack_id,
                    exact_alias=False,
                    active_only=active_only,
                    limit=limit,
                )
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
                ),
            }
            for row in rows
        ]

    def _lookup_rule_candidates_fts(
        self,
        query: str,
        *,
        match_query: str,
        pack_id: str | None,
        active_only: bool,
        limit: int,
    ) -> list[dict[str, str | float | bool | None]]:
        filters = [f"{RULE_SEARCH_TABLE} MATCH ?"]
        params: list[object] = [match_query]
        if active_only:
            filters.append("p.active = 1")
        if pack_id is not None:
            filters.append(f"{RULE_SEARCH_TABLE}.pack_id = ?")
            params.append(pack_id)
        where_clause = f"WHERE {' AND '.join(filters)}"
        params.append(limit)
        with self._connect() as connection:
            try:
                rows = connection.execute(
                    f"""
                    SELECT
                        {RULE_SEARCH_TABLE}.pack_id,
                        {RULE_SEARCH_TABLE}.rule_name,
                        {RULE_SEARCH_TABLE}.label,
                        {RULE_SEARCH_TABLE}.pattern,
                        {RULE_SEARCH_TABLE}.source_domain,
                        p.active
                    FROM {RULE_SEARCH_TABLE}
                    JOIN installed_packs AS p ON p.pack_id = {RULE_SEARCH_TABLE}.pack_id
                    {where_clause}
                    ORDER BY bm25({RULE_SEARCH_TABLE}, 3.0, 1.5, 0.75, 0.5) ASC,
                             {RULE_SEARCH_TABLE}.rule_name ASC,
                             {RULE_SEARCH_TABLE}.label ASC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
            except sqlite3.OperationalError:
                self._search_index_supported = False
                return self._lookup_rule_candidates(
                    query,
                    pack_id=pack_id,
                    active_only=active_only,
                    limit=limit,
                )
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
                ),
            }
            for row in rows
        ]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.layout.registry_db,
            timeout=SQLITE_CONNECT_TIMEOUT_SECONDS,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @staticmethod
    def _alias_score(
        query: str,
        *,
        alias_text: str,
        label: str,
        source_domain: str,
        alias_prior: float,
        exact_alias: bool,
    ) -> float:
        normalized_query = query.lower()
        normalized_alias = alias_text.lower()
        normalized_label = label.lower()
        normalized_domain = source_domain.lower()
        coverage = PackMetadataStore._field_coverage(
            query,
            alias_text,
            label,
            source_domain,
        )
        if exact_alias or normalized_alias == normalized_query:
            return round(min(1.0, 0.7 + (alias_prior * 0.3)), 4)
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
        if coverage == 1.0 and len(PackMetadataStore._query_terms(query)) > 1:
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
    ) -> float:
        normalized_query = query.lower()
        coverage = PackMetadataStore._field_coverage(
            query,
            rule_name,
            label,
            pattern,
            source_domain,
        )
        if rule_name.lower() == normalized_query:
            return 0.9
        if coverage == 0.0:
            return 0.0
        if normalized_query in rule_name.lower():
            return 0.75
        if normalized_query in pattern.lower():
            return 0.6
        score = 0.35 + (coverage * 0.35)
        if coverage == 1.0 and len(PackMetadataStore._query_terms(query)) > 1:
            score += 0.05
        return round(min(0.85, score), 4)

    def _ensure_search_schema(self, connection: sqlite3.Connection) -> None:
        try:
            connection.executescript(
                f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS {ALIAS_SEARCH_TABLE}
                USING fts5(
                    pack_id UNINDEXED,
                    alias_text,
                    label,
                    source_domain,
                    tokenize = 'unicode61 remove_diacritics 2'
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS {RULE_SEARCH_TABLE}
                USING fts5(
                    pack_id UNINDEXED,
                    rule_name,
                    label,
                    pattern,
                    source_domain,
                    tokenize = 'unicode61 remove_diacritics 2'
                );
                """
            )
        except sqlite3.OperationalError:
            self._search_index_supported = False

    def _sync_search_index(self, connection: sqlite3.Connection) -> None:
        if not self._search_index_enabled(connection):
            return
        alias_count = self._table_count(connection, "pack_aliases")
        alias_search_count = self._table_count(connection, ALIAS_SEARCH_TABLE)
        if alias_count > MAX_ALIAS_SEARCH_INDEX_ROWS:
            self._search_index_supported = False
        elif alias_count != alias_search_count:
            connection.execute(f"DELETE FROM {ALIAS_SEARCH_TABLE}")
            connection.execute(
                f"""
                INSERT INTO {ALIAS_SEARCH_TABLE} (pack_id, alias_text, label, source_domain)
                SELECT pack_id, alias_text, label, source_domain
                FROM pack_aliases
                """
            )
        rule_count = self._table_count(connection, "pack_rules")
        rule_search_count = self._table_count(connection, RULE_SEARCH_TABLE)
        if rule_count != rule_search_count:
            connection.execute(f"DELETE FROM {RULE_SEARCH_TABLE}")
            connection.execute(
                f"""
                INSERT INTO {RULE_SEARCH_TABLE} (pack_id, rule_name, label, pattern, source_domain)
                SELECT pack_id, rule_name, label, pattern, source_domain
                FROM pack_rules
                """
            )

    def _replace_alias_search(
        self,
        connection: sqlite3.Connection,
        pack_id: str,
        aliases: list[dict[str, str]],
    ) -> None:
        if not self._search_index_enabled(connection):
            return
        connection.execute(
            f"DELETE FROM {ALIAS_SEARCH_TABLE} WHERE pack_id = ?",
            (pack_id,),
        )
        connection.executemany(
            f"""
            INSERT INTO {ALIAS_SEARCH_TABLE} (pack_id, alias_text, label, source_domain)
            VALUES (?, ?, ?, ?)
            """,
            [
                (pack_id, alias["text"], alias["label"], alias["source_domain"])
                for alias in aliases
            ],
        )

    def _replace_rule_search(
        self,
        connection: sqlite3.Connection,
        pack_id: str,
        rules: list[dict[str, str]],
    ) -> None:
        if not self._search_index_enabled(connection):
            return
        connection.execute(
            f"DELETE FROM {RULE_SEARCH_TABLE} WHERE pack_id = ?",
            (pack_id,),
        )
        connection.executemany(
            f"""
            INSERT INTO {RULE_SEARCH_TABLE} (pack_id, rule_name, label, pattern, source_domain)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    pack_id,
                    rule["name"],
                    rule["label"],
                    rule["pattern"],
                    rule["source_domain"],
                )
                for rule in rules
            ],
        )

    @staticmethod
    def _upsert_alias_batch(
        connection: sqlite3.Connection,
        alias_rows: list[tuple[object, ...]],
    ) -> None:
        connection.executemany(
            """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pack_id, alias_text, label) DO UPDATE SET
                normalized_alias_text = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.normalized_alias_text
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.normalized_alias_text
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.normalized_alias_text
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.normalized_alias_text
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.normalized_alias_text
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.normalized_alias_text
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.normalized_alias_text
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.normalized_alias_text
                    WHEN excluded.position < pack_aliases.position THEN excluded.normalized_alias_text
                    ELSE pack_aliases.normalized_alias_text
                END,
                canonical_text = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.canonical_text
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.canonical_text
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.canonical_text
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.canonical_text
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.canonical_text
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.canonical_text
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.canonical_text
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.canonical_text
                    WHEN excluded.position < pack_aliases.position THEN excluded.canonical_text
                    ELSE pack_aliases.canonical_text
                END,
                alias_score = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.alias_score
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.alias_score
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.alias_score
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.alias_score
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.alias_score
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.alias_score
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.alias_score
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.alias_score
                    WHEN excluded.position < pack_aliases.position THEN excluded.alias_score
                    ELSE pack_aliases.alias_score
                END,
                generated = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.generated
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.generated
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.generated
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.generated
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.generated
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.generated
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.generated
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.generated
                    WHEN excluded.position < pack_aliases.position THEN excluded.generated
                    ELSE pack_aliases.generated
                END,
                source_name = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.source_name
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.source_name
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.source_name
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.source_name
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.source_name
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.source_name
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.source_name
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.source_name
                    WHEN excluded.position < pack_aliases.position THEN excluded.source_name
                    ELSE pack_aliases.source_name
                END,
                entity_id = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.entity_id
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.entity_id
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.entity_id
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.entity_id
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.entity_id
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.entity_id
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.entity_id
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.entity_id
                    WHEN excluded.position < pack_aliases.position THEN excluded.entity_id
                    ELSE pack_aliases.entity_id
                END,
                source_priority = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.source_priority
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.source_priority
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.source_priority
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.source_priority
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.source_priority
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.source_priority
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.source_priority
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.source_priority
                    WHEN excluded.position < pack_aliases.position THEN excluded.source_priority
                    ELSE pack_aliases.source_priority
                END,
                popularity_weight = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.popularity_weight
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.popularity_weight
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.popularity_weight
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.popularity_weight
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.popularity_weight
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.popularity_weight
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.popularity_weight
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.popularity_weight
                    WHEN excluded.position < pack_aliases.position THEN excluded.popularity_weight
                    ELSE pack_aliases.popularity_weight
                END,
                source_domain = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.source_domain
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.source_domain
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.source_domain
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.source_domain
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.source_domain
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.source_domain
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.source_domain
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.source_domain
                    WHEN excluded.position < pack_aliases.position THEN excluded.source_domain
                    ELSE pack_aliases.source_domain
                END,
                position = CASE
                    WHEN excluded.alias_score > pack_aliases.alias_score THEN excluded.position
                    WHEN excluded.alias_score < pack_aliases.alias_score THEN pack_aliases.position
                    WHEN excluded.source_priority > pack_aliases.source_priority THEN excluded.position
                    WHEN excluded.source_priority < pack_aliases.source_priority THEN pack_aliases.position
                    WHEN excluded.popularity_weight > pack_aliases.popularity_weight THEN excluded.position
                    WHEN excluded.popularity_weight < pack_aliases.popularity_weight THEN pack_aliases.position
                    WHEN excluded.generated < pack_aliases.generated THEN excluded.position
                    WHEN excluded.generated > pack_aliases.generated THEN pack_aliases.position
                    WHEN excluded.position < pack_aliases.position THEN excluded.position
                    ELSE pack_aliases.position
                END
            """,
            alias_rows,
        )

    @classmethod
    def _flush_alias_batch(
        cls,
        connection: sqlite3.Connection,
        alias_rows: list[tuple[object, ...]],
    ) -> None:
        cls._upsert_alias_batch(connection, alias_rows)
        connection.commit()

    @staticmethod
    def _replace_alias_search_from_alias_table(
        connection: sqlite3.Connection,
        pack_id: str,
    ) -> None:
        connection.execute(
            f"DELETE FROM {ALIAS_SEARCH_TABLE} WHERE pack_id = ?",
            (pack_id,),
        )
        connection.execute(
            f"""
            INSERT INTO {ALIAS_SEARCH_TABLE} (pack_id, alias_text, label, source_domain)
            SELECT pack_id, alias_text, label, source_domain
            FROM pack_aliases
            WHERE pack_id = ?
            ORDER BY position ASC
            """,
            (pack_id,),
        )

    @staticmethod
    def _table_count(connection: sqlite3.Connection, table_name: str) -> int:
        row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
        if row is None:
            return 0
        return int(row["count"])

    @staticmethod
    def _search_index_enabled(connection: sqlite3.Connection) -> bool:
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name IN (?, ?)
            ORDER BY name ASC
            """,
            (ALIAS_SEARCH_TABLE, RULE_SEARCH_TABLE),
        ).fetchall()
        return {str(row["name"]) for row in rows} == {ALIAS_SEARCH_TABLE, RULE_SEARCH_TABLE}

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
    def _build_search_match_query(query: str) -> str | None:
        terms = PackMetadataStore._query_terms(query)
        if not terms:
            return None
        return " AND ".join(f"{term}*" for term in terms)

    @staticmethod
    def _field_coverage(query: str, *fields: str) -> float:
        terms = PackMetadataStore._query_terms(query)
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
        data = json.loads(path.read_text())
        return [str(item) for item in data]

    @staticmethod
    def _load_rules(pack_dir: Path, domain: str) -> list[dict[str, str]]:
        path = pack_dir / "rules.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text())
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
        data = json.loads(path.read_text())
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

    @classmethod
    def _iter_aliases(
        cls,
        pack_dir: Path,
        domain: str,
    ) -> Iterator[dict[str, str | float | bool]]:
        jsonl_path = pack_dir / "aliases.jsonl"
        if jsonl_path.exists():
            yield from cls._iter_aliases_jsonl(jsonl_path, domain)
            return

        json_path = pack_dir / "aliases.json"
        if not json_path.exists():
            normalized_entities_path = pack_dir / "normalized" / "entities.jsonl"
            if normalized_entities_path.exists():
                yield from cls._iter_entity_bundle_aliases_jsonl(
                    normalized_entities_path,
                    domain,
                )
                return
            flat_entities_path = pack_dir / "entities.jsonl"
            if flat_entities_path.exists():
                yield from cls._iter_entity_bundle_aliases_jsonl(
                    flat_entities_path,
                    domain,
                )
            return
        yield from cls._iter_aliases_json(json_path, domain)

    @classmethod
    def _iter_aliases_json(
        cls,
        path: Path,
        domain: str,
    ) -> Iterator[dict[str, str | float | bool]]:
        for item in cls._iter_alias_json_items(path):
            yield cls._normalize_alias_record(item, domain)

    @classmethod
    def _iter_aliases_jsonl(
        cls,
        path: Path,
        domain: str,
    ) -> Iterator[dict[str, str | float | bool]]:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = line.strip()
                if not payload:
                    continue
                yield cls._normalize_alias_record(json.loads(payload), domain)

    @classmethod
    def _iter_entity_bundle_aliases_jsonl(
        cls,
        path: Path,
        domain: str,
    ) -> Iterator[dict[str, str | float | bool]]:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = line.strip()
                if not payload:
                    continue
                record = json.loads(payload)
                if not isinstance(record, dict):
                    continue
                yield from cls._normalize_entity_bundle_alias_records(record, domain)

    @staticmethod
    def _iter_alias_json_items(path: Path) -> Iterator[object]:
        decoder = json.JSONDecoder()
        buffer = ""
        array_started = False
        eof = False
        with path.open("r", encoding="utf-8") as handle:
            while not array_started:
                chunk = handle.read(ALIAS_JSON_READ_CHUNK_SIZE)
                if not chunk:
                    break
                buffer += chunk
                key_index = buffer.find('"aliases"')
                if key_index == -1:
                    if len(buffer) > ALIAS_JSON_READ_CHUNK_SIZE:
                        buffer = buffer[-ALIAS_JSON_READ_CHUNK_SIZE:]
                    continue
                bracket_index = buffer.find("[", key_index)
                if bracket_index == -1:
                    continue
                buffer = buffer[bracket_index + 1 :]
                array_started = True
            if not array_started:
                return

            while True:
                buffer = buffer.lstrip()
                while buffer.startswith(","):
                    buffer = buffer[1:].lstrip()
                if buffer.startswith("]"):
                    return
                if not buffer and eof:
                    return
                try:
                    item, offset = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    chunk = handle.read(ALIAS_JSON_READ_CHUNK_SIZE)
                    if not chunk:
                        eof = True
                        if buffer.strip():
                            raise ValueError(
                                f"Invalid aliases.json payload in {path}"
                            ) from None
                        return
                    buffer += chunk
                    continue
                yield item
                buffer = buffer[offset:]

    @staticmethod
    def _normalize_alias_record(
        item: object,
        domain: str,
    ) -> dict[str, str | float | bool]:
        if isinstance(item, str):
            return {
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
        if not isinstance(item, dict):
            raise ValueError(f"Unsupported alias record type: {type(item)!r}")
        text = str(item["text"])
        return {
            "text": text,
            "label": str(item.get("label", "alias")),
            "normalized_text": str(
                item.get("normalized_text") or normalize_lookup_text(text)
            ),
            "canonical_text": str(item.get("canonical_text") or text or ""),
            "alias_score": float(item.get("score", item.get("alias_score", 1.0))),
            "generated": bool(item.get("generated", False)),
            "source_name": str(item.get("source_name", "")),
            "entity_id": str(item.get("entity_id", "")),
            "source_priority": float(item.get("source_priority", 0.6)),
            "popularity_weight": float(item.get("popularity_weight", 0.5)),
            "source_domain": domain,
        }

    @classmethod
    def _normalize_entity_bundle_alias_records(
        cls,
        record: dict[str, object],
        domain: str,
    ) -> Iterator[dict[str, str | float | bool]]:
        label = str(record.get("entity_type") or record.get("label") or "alias").strip()
        canonical_text = str(record.get("canonical_text") or "").strip()
        if not canonical_text:
            return

        entity_id = str(record.get("entity_id") or record.get("source_id") or "").strip()
        source_name = str(record.get("source_name") or "").strip()
        popularity_weight = cls._entity_bundle_popularity_weight(record)

        alias_values: list[tuple[str, float, float]] = [(canonical_text, 1.0, 0.8)]
        raw_aliases = record.get("aliases")
        if isinstance(raw_aliases, list):
            for value in raw_aliases:
                alias_text = str(value or "").strip()
                if not alias_text:
                    continue
                alias_values.append((alias_text, 0.95, 0.7))

        seen_keys: set[str] = set()
        for alias_text, alias_score, source_priority in alias_values:
            normalized_text = normalize_lookup_text(alias_text)
            if not normalized_text or normalized_text in seen_keys:
                continue
            seen_keys.add(normalized_text)
            yield {
                "text": alias_text,
                "label": label,
                "normalized_text": normalized_text,
                "canonical_text": canonical_text,
                "alias_score": alias_score,
                "generated": False,
                "source_name": source_name,
                "entity_id": entity_id,
                "source_priority": source_priority,
                "popularity_weight": popularity_weight,
                "source_domain": domain,
            }

    @staticmethod
    def _entity_bundle_popularity_weight(record: dict[str, object]) -> float:
        popularity = record.get("popularity")
        if isinstance(popularity, (int, float)):
            return max(0.0, min(1.0, float(popularity)))
        source_features = record.get("source_features")
        if isinstance(source_features, dict):
            sitelink_count = source_features.get("sitelink_count")
            if isinstance(sitelink_count, (int, float)):
                if float(sitelink_count) >= 50:
                    return 1.0
                if float(sitelink_count) >= 10:
                    return 0.8
                if float(sitelink_count) >= 1:
                    return 0.6
        return 0.5

    @staticmethod
    def _ensure_pack_alias_columns(connection: sqlite3.Connection) -> None:
        rows = connection.execute("PRAGMA table_info(pack_aliases)").fetchall()
        existing = {str(row[1]) for row in rows}
        required = {
            "normalized_alias_text": "TEXT NOT NULL DEFAULT ''",
            "canonical_text": "TEXT NOT NULL DEFAULT ''",
            "alias_score": "REAL NOT NULL DEFAULT 1.0",
            "generated": "INTEGER NOT NULL DEFAULT 0",
            "source_name": "TEXT NOT NULL DEFAULT ''",
            "entity_id": "TEXT NOT NULL DEFAULT ''",
            "source_priority": "REAL NOT NULL DEFAULT 0.6",
            "popularity_weight": "REAL NOT NULL DEFAULT 0.5",
        }
        for column_name, column_definition in required.items():
            if column_name in existing:
                continue
            try:
                connection.execute(
                    f"ALTER TABLE pack_aliases ADD COLUMN {column_name} {column_definition}"
                )
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "duplicate column name" in message and column_name in message:
                    continue
                raise

    @staticmethod
    def _ensure_pack_alias_lookup_indexes(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pack_aliases_pack_lookup
            ON pack_aliases(pack_id, normalized_alias_text)
            """
        )
