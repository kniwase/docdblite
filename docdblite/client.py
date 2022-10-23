from __future__ import annotations
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine

from .database import DocDBLiteDatabase


class DocDBLiteClient:
    def __init__(
        self,
        databae_url: str | Path | None = None,
        /,
        **create_engine_kwargs: Any,
    ) -> None:
        if databae_url is None:
            url = "sqlite:///:memory:"
        elif isinstance(databae_url, Path):
            url = f"sqlite:///{databae_url.absolute()}"
        else:
            url = databae_url
        self._engine = create_engine(url, **create_engine_kwargs)
        self._database: dict[str, DocDBLiteDatabase] = {}

    def __getattribute__(self, name: str) -> DocDBLiteDatabase:
        if name.startswith("_"):
            return super().__getattribute__(name)
        else:
            if name not in self._database:
                self._database[name] = DocDBLiteDatabase(name, self._engine)
            return self._database[name]

    def __del__(self):
        for name in tuple(self._database.keys()):
            self._database[name].__close__()
        self._engine.dispose()
