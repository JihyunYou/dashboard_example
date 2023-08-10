from sqlalchemy import Column, Integer, String
from common.constant import TABLE_SPEC_DIMENSIONS
from common.database import Base


class SpecDimensions(Base):
    __tablename__ = TABLE_SPEC_DIMENSIONS

    id = Column(Integer, primary_key=True)
    key = Column(String)
    name = Column(String)

    def __init__(
            self,
            id=None,
            key=None,
            name=None
    ):
        self.id = id
        self.key = key
        self.name = name
