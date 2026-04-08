"""SQLite-backed metadata store for installed packs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from ..packs.manifest import PackManifest
from .paths import StorageLayout


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PackMetadataStore:
    """Persist installed-pack metadata and deterministic lookup content."""

    def __init__(self, layout: StorageLayout) -> None:
        self.layout = layout
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
                    source_domain TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    PRIMARY KEY (pack_id, alias_text, label),
                    FOREIGN KEY (pack_id) REFERENCES installed_packs(pack_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_installed_packs_active
                ON installed_packs(active, pack_id);

                CREATE INDEX IF NOT EXISTS idx_pack_aliases_lookup
                ON pack_aliases(alias_text, label);

                CREATE INDEX IF NOT EXISTS idx_pack_rules_lookup
                ON pack_rules(rule_name, label);
                """
            )

    def count_installed_packs(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM installed_packs"
            ).fetchone()
        if row is None:
            return 0
        return int(row["count"])

    def sync_pack_from_dir(self, pack_dir: Path) -> PackManifest | None:
        manifest_path = pack_dir / "manifest.json"
        if not manifest_path.exists():
            return None
        manifest = PackManifest.load(manifest_path)
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
            self._replace_aliases(connection, manifest.pack_id, aliases)

    def sync_from_filesystem(self, packs_dir: Path | None = None) -> None:
        root = packs_dir or self.layout.packs_dir
        if not root.exists():
            return
        for manifest_path in sorted(root.glob("*/manifest.json")):
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

    def list_pack_aliases(self, pack_id: str) -> list[dict[str, str]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT alias_text, label, source_domain
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
        active_only: bool = True,
        limit: int = 20,
    ) -> list[dict[str, str | float | bool | None]]:
        normalized = query.strip()
        if not normalized:
            return []

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

        dependencies = [
            str(item["dependency_pack_id"])
            for item in connection.execute(
                """
                SELECT dependency_pack_id
                FROM pack_dependencies
                WHERE pack_id = ?
                ORDER BY position ASC, dependency_pack_id ASC
                """,
                (row["pack_id"],),
            ).fetchall()
        ]
        rules = [
            str(item["rule_name"])
            for item in connection.execute(
                """
                SELECT rule_name
                FROM pack_rules
                WHERE pack_id = ?
                ORDER BY position ASC, rule_name ASC
                """,
                (row["pack_id"],),
            ).fetchall()
        ]
        labels = [
            str(item["label"])
            for item in connection.execute(
                """
                SELECT label
                FROM pack_labels
                WHERE pack_id = ?
                ORDER BY position ASC, label ASC
                """,
                (row["pack_id"],),
            ).fetchall()
        ]
        return PackManifest(
            schema_version=int(row["schema_version"]),
            pack_id=str(row["pack_id"]),
            version=str(row["version"]),
            language=str(row["language"]),
            domain=str(row["domain"]),
            tier=str(row["tier"]),
            dependencies=dependencies,
            artifacts=[],
            models=[],
            rules=rules,
            labels=labels,
            min_ades_version=str(row["min_ades_version"]),
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

    def _replace_aliases(
        self,
        connection: sqlite3.Connection,
        pack_id: str,
        aliases: list[dict[str, str]],
    ) -> None:
        connection.execute("DELETE FROM pack_aliases WHERE pack_id = ?", (pack_id,))
        connection.executemany(
            """
            INSERT INTO pack_aliases (
                pack_id,
                alias_text,
                label,
                source_domain,
                position
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    pack_id,
                    alias["text"],
                    alias["label"],
                    alias["source_domain"],
                    position,
                )
                for position, alias in enumerate(aliases)
            ],
        )

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
            filters.append("LOWER(a.alias_text) = LOWER(?)")
            params.append(query)
        else:
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
                "pattern": None,
                "domain": str(row["source_domain"]),
                "active": bool(row["active"]),
                "score": self._alias_score(query, str(row["alias_text"]), exact_alias),
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
                "score": self._rule_score(query, str(row["rule_name"]), str(row["pattern"])),
            }
            for row in rows
        ]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.layout.registry_db)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @staticmethod
    def _alias_score(query: str, alias_text: str, exact_alias: bool) -> float:
        normalized_query = query.lower()
        normalized_alias = alias_text.lower()
        if exact_alias or normalized_alias == normalized_query:
            return 1.0
        if normalized_alias.startswith(normalized_query):
            return 0.95
        if normalized_query in normalized_alias:
            return 0.85
        return 0.7

    @staticmethod
    def _rule_score(query: str, rule_name: str, pattern: str) -> float:
        normalized_query = query.lower()
        if rule_name.lower() == normalized_query:
            return 0.9
        if normalized_query in rule_name.lower():
            return 0.75
        if normalized_query in pattern.lower():
            return 0.6
        return 0.5

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
    def _load_aliases(pack_dir: Path, domain: str) -> list[dict[str, str]]:
        path = pack_dir / "aliases.json"
        if not path.exists():
            return []
        data = json.loads(path.read_text())
        aliases = data.get("aliases", [])
        loaded: list[dict[str, str]] = []
        for item in aliases:
            if isinstance(item, str):
                loaded.append(
                    {
                        "text": item,
                        "label": "alias",
                        "source_domain": domain,
                    }
                )
                continue
            loaded.append(
                {
                    "text": str(item["text"]),
                    "label": str(item.get("label", "alias")),
                    "source_domain": domain,
                }
            )
        return loaded
