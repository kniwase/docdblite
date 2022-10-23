from __future__ import annotations
import base64
from datetime import date, datetime
from enum import IntEnum
from itertools import groupby
import time
from typing import Any, Iterable, Iterator, Mapping, NamedTuple, cast
from uuid import UUID

from ulid import ULID


class ValueType(IntEnum):
    NONE = 0
    MAPPING = 1
    ARRAY = 2
    STR = 11
    BYTES = 12
    BOOL = 13
    INT = 14
    FLOAT = 15
    DATETIME = 16
    DATE = 17
    UUID = 18


class DataField(NamedTuple):
    id: UUID
    path: str
    value: str
    value_type: ValueType


def encode(value: Any):
    if value is None:
        return "None", ValueType.NONE
    elif isinstance(value, str):
        return value, ValueType.STR
    elif isinstance(value, bytes):
        return base64.b64encode(value).decode("utf8"), ValueType.BYTES
    elif isinstance(value, bool):
        return str(int(value)), ValueType.BOOL
    elif isinstance(value, int):
        return str(value), ValueType.INT
    elif isinstance(value, float):
        return str(value), ValueType.FLOAT
    elif isinstance(value, datetime):
        return (value.isoformat(), ValueType.DATETIME)
    elif isinstance(value, date):
        return (value.isoformat(), ValueType.DATE)
    elif isinstance(value, UUID):
        return str(value), ValueType.UUID
    else:
        raise ValueError(f"invalid type: {type(value)}")


def decode(value_str: str, value_type: ValueType):
    if value_type == ValueType.NONE:
        return None
    elif value_type == ValueType.MAPPING:
        return dict[str, Any]()
    elif value_type == ValueType.ARRAY:
        return list[Any]()
    elif value_type == ValueType.STR:
        return value_str
    elif value_type == ValueType.BYTES:
        return base64.b64decode(value_str)
    elif value_type == ValueType.BOOL:
        return bool(int(value_str))
    elif value_type == ValueType.INT:
        return int(value_str)
    elif value_type == ValueType.FLOAT:
        return float(value_str)
    elif value_type == ValueType.DATETIME:
        return datetime.fromisoformat(value_str)
    elif value_type == ValueType.DATE:
        return date.fromisoformat(value_str)
    elif value_type == ValueType.UUID:
        return UUID(value_str)
    else:
        raise ValueError(f"invalid type: {value_type}")


def dump(path: str, name: str, value: Any) -> list[tuple[str, str, ValueType]]:
    child_path = f"{path}.{name}" if path else name
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        records: list[tuple[str, str, ValueType]]
        if isinstance(value, Mapping):
            records = [(child_path, "", ValueType.MAPPING)]
            items = cast(dict[str, Any], value).items()
        else:
            records = [(child_path, "", ValueType.ARRAY)]
            items = (
                (str(idx), item) for idx, item in enumerate(cast(Iterable[Any], value))
            )
        records.extend((it for k, v in items for it in dump(child_path, k, v)))
        return records
    else:
        value_str, value_type = encode(value)
        return [(child_path, value_str, value_type)]


def dump_data(data: dict[str, Any]):
    if "_id" in data.keys():
        del data["_id"]
    recid: UUID = ULID.from_timestamp(time.time()).to_uuid()  # type: ignore
    records: list[DataField] = [DataField(recid, *dump("", "_id", recid)[0])]
    records.extend(
        DataField(recid, *it)
        for it in sorted(
            (it for k, v in data.items() for it in dump("", k, v)),
            key=lambda it: it[1],
        )
    )
    return recid, records


def load(data: dict[str, Any], path: list[str], value: Any):
    cursor: dict[str, Any] | list[Any]
    cursor = data
    for key in path[:-1]:
        cursor = cursor[key] if isinstance(cursor, dict) else cursor[int(key)]
    if isinstance(cursor, dict):
        cursor[path[-1]] = value
    else:
        cursor.append(value)


def load_data(record: Iterable[DataField]):
    data: dict[str, Any] = {}
    for path_str, value_str, value_type in sorted(
        (rec[1:] for rec in record), key=lambda rec: rec[0]
    ):
        load(data, path_str.split("."), decode(value_str, value_type))
    return data


def load_many_data(records: Iterable[DataField]) -> Iterator[dict[str, Any]]:
    return iter(load_data(r) for _, r in groupby(records, key=lambda r: r.id))
