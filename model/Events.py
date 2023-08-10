from sqlalchemy import Column, Integer, String, Date
from common.constant import TABLE_EVENTS
from common.database import Base


class Events(Base):
    __tablename__ = TABLE_EVENTS

    id = Column(Integer, primary_key=True)
    browser = Column(String)
    channel = Column(String)
    clicks = Column(Integer)
    event_date = Column(Date)
    impressions = Column(Integer)
    purchase_price = Column(Integer)
    return_price = Column(Integer)

    def __init__(
            self,
            id=None,
            browser=None,
            channel=None,
            clicks=None,
            event_date=None,
            impressions=None,
            purchase_price=None,
            return_price=None
    ):
        self.id = id
        self.browser = browser
        self.channel = channel
        self.clicks = clicks
        self.event_date = event_date
        self.impressions = impressions
        self.purchase_price = purchase_price
        self.return_price = return_price

