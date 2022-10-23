from __future__ import annotations

from sqlalchemy.engine import Engine

from .collection import DocDBLiteCollection


class DocDBLiteDatabase:
    def __init__(self, name: str, engine: Engine) -> None:
        self._db_name = name
        self._engine = engine
        self._collection: dict[str, DocDBLiteCollection] = {}
        self._opend = True

    def __getattribute__(self, name: str) -> DocDBLiteCollection:
        if name.startswith("_"):
            return super().__getattribute__(name)
        else:
            if name not in self._collection:
                self._collection[name] = DocDBLiteCollection(
                    self._db_name, name, self._engine
                )
            return self._collection[name]

    def __close__(self):
        if self._opend:
            for name in tuple(self._collection.keys()):
                self._collection[name].__del__()
            self._opend = False

    def __del__(self):
        self.__close__()
