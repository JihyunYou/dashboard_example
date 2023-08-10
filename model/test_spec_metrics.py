import unittest

from sqlalchemy import select

from common.database import get_db_session
from model.SpecMetrics import SpecMetrics


class TestSpecMetrics(unittest.TestCase):
    def setUp(self) -> None:
        self.db_session = get_db_session()

    def tearDown(self) -> None:
        self.db_session.close()

    def test_select(self):
        stmt = select(SpecMetrics).where(
            SpecMetrics.id > 0
        )

        test_model_obj = self.db_session.scalars(stmt).first()

        print(test_model_obj)
        print(test_model_obj.name)
