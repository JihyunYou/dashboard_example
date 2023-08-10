from sqlalchemy import Column, Integer, String
from common.constant import TABLE_SPEC_METRICS
from common.database import Base


class SpecMetrics(Base):
    __tablename__ = TABLE_SPEC_METRICS

    id = Column(Integer, primary_key=True)
    key = Column(String)
    name = Column(String)
    calculates = Column(String)

    def __init__(
            self,
            id=None,
            key=None,
            name=None,
            calculates=None,
    ):
        self.id = id
        self.key = key
        self.name = name
        self.calculates = calculates
