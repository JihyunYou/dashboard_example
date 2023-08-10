import unittest
from unittest import IsolatedAsyncioTestCase
from httpx import AsyncClient
from fastapi import status

from common.constant import TEST_BASE_URL


class TestSpecMetricsGetApi(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    async def asyncSetUp(self) -> None:
        pass

    async def asyncTearDown(self) -> None:
        pass

    async def test_response(self):
        # expected
        expected = [
            {'key': 'clicks', 'name': '클릭 수'},
            {'key': 'impressions', 'name': '노출 수'},
            {'key': 'purchase_price', 'name': '구매 금액'},
            {'key': 'return_price', 'name': '구매 금액'},
            {'key': 'custom_1', 'name': '순 구매 금액'},
            {'key': 'custom_2', 'name': '클릭당 구매 금액'}
        ]

        async with AsyncClient(base_url=TEST_BASE_URL) as ac:
            response = await ac.get("/spec/metrics")
            self.assertEqual(status.HTTP_200_OK, response.status_code)
            self.assertEqual(expected, response.json())


if __name__ == "__main__":
    unittest.main()
