from __future__ import annotations
from itertools import chain
from typing import Any, Callable, Generator, Iterable, Literal, overload
from uuid import UUID

from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session, Query
from sqlalchemy import or_, asc
from sqlalchemy.sql import ClauseElement

from .core import stringify
from .core.model import create_model


class DocDBLiteCollection:
    def __init__(
        self, database_name: str, collection_name: str, engine: Engine
    ) -> None:
        self._model = create_model(database_name, collection_name, engine)
        self._sessionmaker = sessionmaker(
            autocommit=False, autoflush=False, bind=engine
        )
        self._scoped_session = scoped_session(self._sessionmaker)
        self._opend = True

    def __close__(self):
        if self._opend:
            self._sessionmaker.close_all()
            self._opend = False

    def __del__(self):
        self.__close__()

    def insert_many(self, records: Iterable[dict[str, Any]]) -> list[UUID]:
        model_type = self._model
        recids: list[UUID] = []
        with self._scoped_session() as session:
            session.add_all(
                model_type(
                    id=field.id,
                    path=field.path,
                    value=field.value,
                    value_type=field.value_type,
                )
                for recid, fields in (stringify.dump_data(rec) for rec in records)
                if not recids.append(recid)
                for field in fields
            )
            session.commit()
        return recids

    def insert_one(self, record: dict[str, Any]) -> UUID:
        return self.insert_many((record,))[0]

    @overload
    def find_many(
        self,
        _filter: dict[str, Any] | None = None,
        *,
        fetchall_at_once: Literal[True] = ...,
    ) -> list[dict[str, Any]]:
        ...

    @overload
    def find_many(
        self,
        _filter: dict[str, Any] | None = None,
        *,
        fetchall_at_once: Literal[False] = ...,
    ) -> Generator[dict[str, Any], None, None]:
        ...

    def find_many(  # type: ignore
        self,
        _filter: dict[str, Any] | None = None,
        *,
        fetchall_at_once: bool = True,
    ):
        model_type = self._model
        with self._scoped_session() as session:
            q: Query[model_type] = session.query(
                model_type.id,
                model_type.path,
                model_type.value,
                model_type.value_type,
            )
            if _filter:
                q = q.filter(model_type.id.in_(self._search(session, _filter)))
            q = q.order_by(asc(model_type.id))
            fetchone = q.statement.execute().fetchone
            data = stringify.load_many_data(
                stringify.DataField(*it) for it in iter(fetchone, None)
            )
            if fetchall_at_once:
                return list(data)
            else:
                yield from data

    def find_one(self, query: dict[str, Any] | None = None):
        return next(self.find_many(query, fetchall_at_once=False))

    def update_many(self, _filter: dict[str, Any] | None, data: dict[str, Any]):
        model_type = self._model
        with self._scoped_session() as session:
            recid_query = self._search(session, _filter)
            recid_filter = model_type.id.in_(recid_query)
            self._update(session, recid_query, recid_filter, data)

    def update_one(self, _filter: dict[str, Any] | None, data: dict[str, Any]):
        model_type = self._model
        with self._scoped_session() as session:
            recid_query = self._search(session, _filter).limit(1)
            record = recid_query.first()
            if record:
                recid_filter = model_type.id == record.id
                self._update(session, recid_query, recid_filter, data)

    def _update(
        self,
        session: Session,
        recid_query: Query[Any],
        recid_filter: ClauseElement | None,
        data: dict[str, Any],
    ):
        model_type = self._model
        fetchone: Callable[
            [], tuple[UUID] | None
        ] = recid_query.statement.execute().fetchone
        ids = tuple(chain.from_iterable(iter(fetchone, None)))
        q = session.query(model_type)
        if recid_filter is not None:
            q = q.filter(recid_filter)
        delete_targets = q.filter(
            or_(
                *tuple(
                    chain.from_iterable(
                        (model_type.path == key, model_type.path.startswith(key + "."))
                        for key in data.keys()
                    )
                )
            )
        ).all()
        for row in delete_targets:
            session.delete(row)
        session.add_all(
            self._model(id=_id, path=path, value=value_str, value_type=value_type)
            for _id in ids
            for key, value in data.items()
            for path, value_str, value_type in stringify.dump("", key, value)
        )
        session.commit()

    def _search(self, session: Session, _filter: dict[str, Any] | None):
        model_type = self._model
        q: Query[model_type] = session.query(model_type.id).order_by(asc(model_type.id))
        if _filter:
            for key, value in _filter.items():
                value_str, _ = stringify.encode(value)
                q = q.filter(model_type.path == key, model_type.value == value_str)
        return q.distinct(model_type.id)
