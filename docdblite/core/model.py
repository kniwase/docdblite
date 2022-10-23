from __future__ import annotations
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import Column, Integer, Text
from sqlalchemy.types import TypeDecorator
from sqlalchemy.orm import registry
from sqlalchemy.engine import Engine
from sqlalchemy_utils import UUIDType  # type: ignore

from .stringify import ValueType, DataField


class ValueTypeEnum(TypeDecorator[ValueType]):
    impl = Integer
    cache_ok = True

    def __init__(self, *args: Any, **kwargs: Any):
        TypeDecorator.__init__(self, *args, **kwargs)  # type: ignore

    def process_bind_param(self, value: Any, dialect: Any):
        if value is not None:
            if not isinstance(value, ValueType):
                raise TypeError(f"Value must be {ValueType} type")
            return value.value

    def process_result_value(self, value: Any, dialect: Any):
        if value is not None:
            if not isinstance(value, int):
                raise TypeError("value must have int type")
            return ValueType(value)


def create_model(database_name: str, collection_name: str, engine: Engine):
    mapper_registry = registry(_bind=engine)

    @mapper_registry.mapped
    class CollectionTableModel:
        __tablename__ = f"{collection_name}@{database_name}"

        rownum = Column(Integer, primary_key=True)
        id = Column(UUIDType(binary=False), index=True)
        path = Column(Text, index=True)
        value = Column(Text, index=True)
        value_type = Column(ValueTypeEnum)

        if TYPE_CHECKING:

            def __init__(
                self, *, id: UUID, path: str, value: str, value_type: ValueType
            ) -> None:
                ...

        def as_namedtuple(self):
            return DataField(self.id, self.path, self.value, self.value_type)  # type: ignore

        def __getitem__(self, idx: int) -> Any:
            if idx == 0:
                return self.id
            if idx == 1:
                return self.path
            if idx == 2:
                return self.value
            if idx == 3:
                return self.value_type
            else:
                raise IndexError

    mapper_registry.metadata.create_all(engine)
    return CollectionTableModel
