from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

JsonDict = dict[str, Any]
Normalizer = Callable[[str], str]


class RecordStoreError(Exception):
    """Base exception for RecordStore errors."""


class IdentifierConflictError(RecordStoreError):
    """Raised when identifiers imply conflicting record identities."""


class MetadataConflictError(RecordStoreError):
    """Raised when metadata updates conflict with existing values."""


class UnknownIdentifierTypeError(RecordStoreError):
    """Raised when an identifier type was not declared for this store."""


class UnknownFieldError(RecordStoreError):
    """Raised when a metadata field was not declared for this store."""


@dataclass(frozen=True)
class StoredRecord:
    """A record returned from the store."""

    record_id: int
    store_name: str
    identifiers: dict[str, list[str]]
    data: JsonDict


class RecordStore:
    """
    SQLite-backed record store with multiple identifiers per record.

    Core design:
      - records table stores flexible JSON metadata.
      - identifiers table stores indexed external identifiers.
      - identifier uniqueness is enforced by SQLite.
      - metadata merge/conflict rules are handled in Python.

    A single SQLite database can hold multiple logical stores, such as:
      - documents
      - journals

    Each logical store has its own allowed identifier types and metadata fields.
    """

    VALID_FIELD_POLICIES = {
        "strict",
        "set_union",
        "append",
        "replace",
    }

    def __init__(
        self,
        db_path: str | Path,
        *,
        store_name: str,
        identifier_types: Iterable[str],
        fields: Iterable[str],
        field_policies: Mapping[str, str] | None = None,
        identifier_normalizers: Mapping[str, Normalizer] | None = None,
        allow_unknown_fields: bool = False,
    ) -> None:
        self.db_path = Path(db_path)
        self.store_name = store_name

        self.identifier_types = {self._canonical_identifier_type(t) for t in identifier_types}
        self.fields = set(fields)
        self.allow_unknown_fields = allow_unknown_fields

        self.field_policies = dict(field_policies or {})
        for field, policy in self.field_policies.items():
            if policy not in self.VALID_FIELD_POLICIES:
                raise ValueError(f"Invalid merge policy {policy!r} for field {field!r}. " f"Expected one of {sorted(self.VALID_FIELD_POLICIES)}.")

        unknown_policy_fields = set(self.field_policies) - self.fields
        if unknown_policy_fields and not allow_unknown_fields:
            raise ValueError(f"Field policies were provided for undeclared fields: " f"{sorted(unknown_policy_fields)}")

        self.identifier_normalizers: dict[str, Normalizer] = {self._canonical_identifier_type(k): v for k, v in (identifier_normalizers or {}).items()}

        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()

        # Optional in-memory indexes populated by preload().
        # When these are not None, get()/get_by_record_id()/iteration avoid
        # per-record SQLite reads for records already known to this store.
        self._record_cache_by_id: dict[int, StoredRecord] | None = None
        self._record_cache_by_identifier: dict[tuple[str, str], StoredRecord] | None = None

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> RecordStore:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _create_schema(self) -> None:
        """
        Create the two-table schema.

        records:
            One row per record.

        identifiers:
            One row per external identifier.
            The primary key guarantees that one identifier of a given type
            maps to at most one record within a logical store.
        """
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS records (
                record_id INTEGER PRIMARY KEY,
                store_name TEXT NOT NULL,
                data_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS identifiers (
                store_name TEXT NOT NULL,
                id_type TEXT NOT NULL,
                id_value TEXT NOT NULL,
                record_id INTEGER NOT NULL,
                PRIMARY KEY (store_name, id_type, id_value),
                FOREIGN KEY (record_id) REFERENCES records(record_id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_records_store_name
                ON records (store_name);

            CREATE INDEX IF NOT EXISTS idx_identifiers_record_id
                ON identifiers (record_id);
            """)
        self.conn.commit()

    @staticmethod
    def _canonical_identifier_type(id_type: str) -> str:
        return id_type.strip().upper()

    def _normalize_identifier_value(self, id_type: str, id_value: Any) -> str:
        if id_value is None:
            raise ValueError(f"Identifier value for {id_type!r} cannot be None.")

        value = str(id_value).strip()
        if not value:
            raise ValueError(f"Identifier value for {id_type!r} cannot be empty.")

        normalizer = self.identifier_normalizers.get(id_type)
        if normalizer is not None:
            value = normalizer(value)

        return value

    def _normalize_identifiers(
        self,
        identifiers: Mapping[str, Any],
    ) -> dict[str, set[str]]:
        """
        Normalize input identifiers to:

            {
                "PMID": {"123"},
                "DOI": {"10.123/example"}
            }

        Values may be:
          - scalar, e.g. "123"
          - iterable, e.g. ["123", "456"]

        Strings are treated as scalar values, not iterables.
        """
        normalized: dict[str, set[str]] = {}

        for raw_type, raw_values in identifiers.items():
            id_type = self._canonical_identifier_type(raw_type)

            if id_type not in self.identifier_types:
                raise UnknownIdentifierTypeError(f"Identifier type {raw_type!r} is not allowed for store " f"{self.store_name!r}. Allowed types: {sorted(self.identifier_types)}")

            if raw_values is None:
                continue

            if isinstance(raw_values, str):
                values = [raw_values]
            else:
                try:
                    values = list(raw_values)
                except TypeError:
                    values = [raw_values]

            for raw_value in values:
                id_value = self._normalize_identifier_value(id_type, raw_value)
                normalized.setdefault(id_type, set()).add(id_value)

        if not normalized:
            raise ValueError("At least one identifier is required.")

        return normalized

    def _validate_data_fields(self, data: Mapping[str, Any]) -> None:
        if self.allow_unknown_fields:
            return

        unknown = set(data) - self.fields
        if unknown:
            raise UnknownFieldError(f"Unknown metadata fields for store {self.store_name!r}: " f"{sorted(unknown)}. Allowed fields: {sorted(self.fields)}")

    @staticmethod
    def _json_dumps(data: JsonDict) -> str:
        return json.dumps(data, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _json_loads(data_json: str) -> JsonDict:
        data = json.loads(data_json)
        if not isinstance(data, dict):
            raise ValueError("Stored JSON record data must be a dictionary.")
        return data

    def _find_record_ids_for_identifiers(
        self,
        identifiers: Mapping[str, set[str]],
    ) -> set[int]:
        record_ids: set[int] = set()

        # If the store has been preloaded, identifier resolution can be done
        # entirely in memory. This is useful during read-heavy workflows and
        # also speeds up upsert() calls that add information to cached records.
        if self._record_cache_by_identifier is not None:
            for id_type, values in identifiers.items():
                for id_value in values:
                    record = self._record_cache_by_identifier.get((id_type, id_value))
                    if record is not None:
                        record_ids.add(record.record_id)
            return record_ids

        for id_type, values in identifiers.items():
            for id_value in values:
                row = self.conn.execute(
                    """
                    SELECT record_id
                    FROM identifiers
                    WHERE store_name = ?
                      AND id_type = ?
                      AND id_value = ?
                    """,
                    (self.store_name, id_type, id_value),
                ).fetchone()

                if row is not None:
                    record_ids.add(int(row["record_id"]))

        return record_ids

    def _load_data_json(self, record_id: int) -> JsonDict:
        row = self.conn.execute(
            """
            SELECT data_json
            FROM records
            WHERE store_name = ?
              AND record_id = ?
            """,
            (self.store_name, record_id),
        ).fetchone()

        if row is None:
            raise KeyError(f"No record {record_id} in store {self.store_name!r}.")

        return self._json_loads(row["data_json"])

    def _load_identifiers(self, record_id: int) -> dict[str, list[str]]:
        rows = self.conn.execute(
            """
            SELECT id_type, id_value
            FROM identifiers
            WHERE store_name = ?
              AND record_id = ?
            ORDER BY id_type, id_value
            """,
            (self.store_name, record_id),
        ).fetchall()

        identifiers: dict[str, list[str]] = {}
        for row in rows:
            identifiers.setdefault(row["id_type"], []).append(row["id_value"])

        return identifiers

    def _merge_data(self, existing: JsonDict, incoming: Mapping[str, Any]) -> JsonDict:
        """
        Merge incoming metadata into existing metadata.

        Policies:

        strict:
            Missing existing value -> add.
            Existing None -> replace with incoming non-None.
            Incoming None -> ignored if existing already has a value.
            Same value -> okay.
            Different non-None value -> conflict.

        set_union:
            Treat values as sets/lists and merge unique values.

        append:
            Append incoming value(s) to a list. Does not deduplicate.

        replace:
            Replace existing value with incoming value.
            Use sparingly.
        """
        self._validate_data_fields(incoming)

        merged = dict(existing)

        for key, incoming_value in incoming.items():
            policy = self.field_policies.get(key, "strict")

            if key not in merged:
                merged[key] = incoming_value
                continue

            existing_value = merged[key]

            if policy == "strict":
                if existing_value is None:
                    merged[key] = incoming_value
                elif incoming_value is None:
                    pass
                elif existing_value == incoming_value:
                    pass
                else:
                    raise MetadataConflictError(
                        f"Conflicting value for field {key!r} in store " f"{self.store_name!r}: existing={existing_value!r}, " f"incoming={incoming_value!r}"
                    )

            elif policy == "set_union":
                merged[key] = self._merge_set_union(existing_value, incoming_value)

            elif policy == "append":
                merged[key] = self._merge_append(existing_value, incoming_value)

            elif policy == "replace":
                merged[key] = incoming_value

            else:
                raise AssertionError(f"Unhandled field policy: {policy!r}")

        return merged

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, set):
            return sorted(value)
        return [value]

    @classmethod
    def _merge_set_union(cls, existing_value: Any, incoming_value: Any) -> list[Any]:
        result: list[Any] = []

        for value in cls._as_list(existing_value) + cls._as_list(incoming_value):
            if value not in result:
                result.append(value)

        return result

    @classmethod
    def _merge_append(cls, existing_value: Any, incoming_value: Any) -> list[Any]:
        return cls._as_list(existing_value) + cls._as_list(incoming_value)

    @property
    def is_preloaded(self) -> bool:
        """Return True if this store is currently using in-memory caches."""
        return self._record_cache_by_id is not None and self._record_cache_by_identifier is not None

    def clear_cache(self) -> None:
        """Disable and discard the in-memory preload cache."""
        self._record_cache_by_id = None
        self._record_cache_by_identifier = None

    def load_all(self) -> list[StoredRecord]:
        """
        Load all records in this store using two SQLite queries.

        This avoids the N+1 query pattern of loading each record and its
        identifiers separately. It is the fastest path for bulk-loading a
        store before many lookups.
        """
        record_rows = self.conn.execute(
            """
            SELECT record_id, data_json
            FROM records
            WHERE store_name = ?
            ORDER BY record_id
            """,
            (self.store_name,),
        ).fetchall()

        identifier_rows = self.conn.execute(
            """
            SELECT record_id, id_type, id_value
            FROM identifiers
            WHERE store_name = ?
            ORDER BY record_id, id_type, id_value
            """,
            (self.store_name,),
        ).fetchall()

        identifiers_by_record_id: dict[int, dict[str, list[str]]] = {}
        for row in identifier_rows:
            record_id = int(row["record_id"])
            identifiers_by_record_id.setdefault(record_id, {}).setdefault(row["id_type"], []).append(row["id_value"])

        records: list[StoredRecord] = []
        for row in record_rows:
            record_id = int(row["record_id"])
            records.append(
                StoredRecord(
                    record_id=record_id,
                    store_name=self.store_name,
                    identifiers=identifiers_by_record_id.get(record_id, {}),
                    data=self._json_loads(row["data_json"]),
                )
            )

        return records

    def preload(self) -> None:
        """
        Load all records and identifiers into in-memory indexes.

        After calling this method, get(), get_by_record_id(), values(), and
        items() will avoid SQLite reads for existing records. SQLite remains
        the authoritative backing store for writes; upsert() and delete_record()
        keep the in-memory cache synchronized for changes made through this
        RecordStore instance.

        If another process or RecordStore instance modifies the same database
        after preload(), call preload() again or clear_cache() to avoid stale
        reads.
        """
        records = self.load_all()

        by_id: dict[int, StoredRecord] = {}
        by_identifier: dict[tuple[str, str], StoredRecord] = {}

        for record in records:
            by_id[record.record_id] = record
            for id_type, values in record.identifiers.items():
                for id_value in values:
                    by_identifier[(id_type, id_value)] = record

        self._record_cache_by_id = by_id
        self._record_cache_by_identifier = by_identifier

    def _load_record_from_db(self, record_id: int) -> StoredRecord:
        """Load one record from SQLite, ignoring any in-memory cache."""
        data = self._load_data_json(record_id)
        identifiers = self._load_identifiers(record_id)
        return StoredRecord(
            record_id=record_id,
            store_name=self.store_name,
            identifiers=identifiers,
            data=data,
        )

    def _cache_record(self, record: StoredRecord) -> None:
        """Insert or replace one record in the in-memory caches, if enabled."""
        if not self.is_preloaded:
            return

        assert self._record_cache_by_id is not None
        assert self._record_cache_by_identifier is not None

        # Remove stale identifier mappings for the previous version of this
        # record before adding the refreshed mappings.
        old_record = self._record_cache_by_id.get(record.record_id)
        if old_record is not None:
            for id_type, values in old_record.identifiers.items():
                for id_value in values:
                    self._record_cache_by_identifier.pop((id_type, id_value), None)

        self._record_cache_by_id[record.record_id] = record
        for id_type, values in record.identifiers.items():
            for id_value in values:
                self._record_cache_by_identifier[(id_type, id_value)] = record

    def _remove_cached_record(self, record_id: int) -> None:
        """Remove one record from the in-memory caches, if enabled."""
        if not self.is_preloaded:
            return

        assert self._record_cache_by_id is not None
        assert self._record_cache_by_identifier is not None

        old_record = self._record_cache_by_id.pop(record_id, None)
        if old_record is None:
            return

        for id_type, values in old_record.identifiers.items():
            for id_value in values:
                self._record_cache_by_identifier.pop((id_type, id_value), None)

    def _refresh_cached_record(self, record_id: int) -> StoredRecord:
        """Reload one record from SQLite and update the cache if enabled."""
        record = self._load_record_from_db(record_id)
        self._cache_record(record)
        return record

    def upsert(
        self,
        *,
        identifiers: Mapping[str, Any],
        data: Mapping[str, Any] | None = None,
    ) -> StoredRecord:
        """
        Insert or update a record.

        If none of the incoming identifiers are known:
            create a new record.

        If exactly one existing record is matched:
            merge identifiers and metadata into that record.

        If multiple existing records are matched:
            raise IdentifierConflictError, because the incoming data implies
            that distinct existing records are actually the same thing.
        """
        normalized_ids = self._normalize_identifiers(identifiers)
        incoming_data = dict(data or {})
        self._validate_data_fields(incoming_data)

        with self.conn:
            matching_record_ids = self._find_record_ids_for_identifiers(normalized_ids)

            if len(matching_record_ids) > 1:
                raise IdentifierConflictError(f"Incoming identifiers match multiple records in store " f"{self.store_name!r}: {sorted(matching_record_ids)}")

            if not matching_record_ids:
                merged_data = self._merge_data({}, incoming_data)

                cursor = self.conn.execute(
                    """
                    INSERT INTO records (store_name, data_json)
                    VALUES (?, ?)
                    """,
                    (self.store_name, self._json_dumps(merged_data)),
                )
                record_id = int(cursor.lastrowid)

            else:
                record_id = next(iter(matching_record_ids))
                existing_data = self._load_data_json(record_id)
                merged_data = self._merge_data(existing_data, incoming_data)

                self.conn.execute(
                    """
                    UPDATE records
                    SET data_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE store_name = ?
                      AND record_id = ?
                    """,
                    (self._json_dumps(merged_data), self.store_name, record_id),
                )

            for id_type, values in normalized_ids.items():
                for id_value in values:
                    self._insert_identifier(record_id, id_type, id_value)

        # If the store has been preloaded, get_by_record_id() would otherwise
        # return the stale cached version. Refresh this record from SQLite and
        # update the in-memory indexes.
        if self.is_preloaded:
            return self._refresh_cached_record(record_id)

        return self.get_by_record_id(record_id)

    def _insert_identifier(self, record_id: int, id_type: str, id_value: str) -> None:
        """
        Attach an identifier to a record.

        If the identifier already exists for the same record, this is fine.

        If it exists for another record, SQLite's primary key would block the
        insert, but we check explicitly so the error message is clearer.
        """
        row = self.conn.execute(
            """
            SELECT record_id
            FROM identifiers
            WHERE store_name = ?
              AND id_type = ?
              AND id_value = ?
            """,
            (self.store_name, id_type, id_value),
        ).fetchone()

        if row is not None:
            existing_record_id = int(row["record_id"])
            if existing_record_id == record_id:
                return

            raise IdentifierConflictError(f"Identifier {id_type}:{id_value} already belongs to record " f"{existing_record_id}, not record {record_id}.")

        self.conn.execute(
            """
            INSERT INTO identifiers (store_name, id_type, id_value, record_id)
            VALUES (?, ?, ?, ?)
            """,
            (self.store_name, id_type, id_value, record_id),
        )

    def get(
        self,
        id_type: str,
        id_value: Any,
    ) -> StoredRecord | None:
        """Return a record by one of its identifiers, or None if absent."""
        id_type_norm = self._canonical_identifier_type(id_type)

        if id_type_norm not in self.identifier_types:
            raise UnknownIdentifierTypeError(f"Identifier type {id_type!r} is not allowed for store " f"{self.store_name!r}.")

        id_value_norm = self._normalize_identifier_value(id_type_norm, id_value)

        if self._record_cache_by_identifier is not None:
            return self._record_cache_by_identifier.get((id_type_norm, id_value_norm))

        row = self.conn.execute(
            """
            SELECT record_id
            FROM identifiers
            WHERE store_name = ?
              AND id_type = ?
              AND id_value = ?
            """,
            (self.store_name, id_type_norm, id_value_norm),
        ).fetchone()

        if row is None:
            return None

        return self.get_by_record_id(int(row["record_id"]))

    def get_by_record_id(self, record_id: int) -> StoredRecord:
        """Return a record by its internal SQLite record id."""
        if self._record_cache_by_id is not None:
            try:
                return self._record_cache_by_id[record_id]
            except KeyError:
                raise KeyError(f"No record {record_id} in preloaded store {self.store_name!r}.") from None

        return self._load_record_from_db(record_id)

    def __iter__(self) -> Iterator[StoredRecord]:
        """Iterate over all records in this logical store."""
        if self._record_cache_by_id is not None:
            yield from self._record_cache_by_id.values()
            return

        yield from self.load_all()

    def values(self) -> Iterator[StoredRecord]:
        """Alias for iterating over all known records."""
        return iter(self)

    def items(self) -> Iterator[tuple[int, StoredRecord]]:
        """Iterate over internal record id plus record."""
        for record in self:
            yield record.record_id, record

    def count(self) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM records
            WHERE store_name = ?
            """,
            (self.store_name,),
        ).fetchone()

        return int(row["n"])

    def delete_record(self, record_id: int) -> None:
        """Delete a record and all identifiers attached to it."""
        with self.conn:
            self.conn.execute(
                """
                DELETE FROM records
                WHERE store_name = ?
                  AND record_id = ?
                """,
                (self.store_name, record_id),
            )

        self._remove_cached_record(record_id)
