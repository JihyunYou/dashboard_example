import json
import unittest
from unittest import IsolatedAsyncioTestCase
from fastapi import status
from httpx import AsyncClient
from common.constant import TEST_BASE_URL
from common.database import get_db_session


class TestReportPostApi(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.db_session = get_db_session()

    def tearDown(self) -> None:
        self.db_session.close()

    async def asyncSetUp(self) -> None:
        pass

    async def asyncTearDown(self) -> None:
        pass

    async def test_case_1(self):
        # given
        request_body = {
            "from": "2023-01-01",
            "to": "2023-01-07",
            "metrics": ["clicks", "purchase_price", "custom_2"],
            "dimensions": ["channel"]
        }
        # expected
        expected = {
            'rows': [
                {'channel': 'airbridge', 'clicks': 534507, 'purchase_price': 10023300, 'custom_2': 18.752420454736797},
                {'channel': 'ab180', 'clicks': 505211, 'purchase_price': 10849500, 'custom_2': 21.4751856155151},
                {'channel': 'unattributed', 'clicks': 119852, 'purchase_price': 2881500, 'custom_2': 24.04215198745119}
            ],
            'totalCount': 3
        }

        async with AsyncClient(base_url=TEST_BASE_URL) as ac:
            response = await ac.post(
                "/report",
                data=json.dumps(request_body)
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)
            self.assertEqual(expected, response.json())

    async def test_case_2(self):
        # given
        request_body = {
            "from": "2023-01-01",
            "to": "2023-01-14",
            "metrics": ["impressions", "purchase_price", "custom_2"],
            "dimensions": ["channel"],
            "sortByDesc": "custom_2"
        }
        # expected
        expected = {
            "rows": [
                {
                    "channel": "unattributed",
                    "impressions": 26208107,
                    "purchase_price": 5873200,
                    "custom_2": 22.1814336430244
                },
                {
                    "channel": "ab180",
                    "impressions": 101907733,
                    "purchase_price": 21257900,
                    "custom_2": 21.662975987999605
                },
                {
                    "channel": "airbridge",
                    "impressions": 100995985,
                    "purchase_price": 20499500,
                    "custom_2": 20.14916644305461
                }
            ],
            "totalCount": 3
        }

        async with AsyncClient(base_url=TEST_BASE_URL) as ac:
            response = await ac.post(
                "/report",
                data=json.dumps(request_body)
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)
            self.assertEqual(expected, response.json())

    async def test_case_3(self):
        # given
        request_body = {
            "from": "2023-01-01",
            "to": "2023-01-07",
            "metrics": ["impressions"]
        }
        # expected
        expected = {
            "rows": [
                {
                    "impressions": 116227384
                }
            ],
            "totalCount": 1
        }

        async with AsyncClient(base_url=TEST_BASE_URL) as ac:
            response = await ac.post(
                "/report",
                data=json.dumps(request_body)
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)
            self.assertEqual(expected, response.json())

    async def test_case_4(self):
        # given
        request_body = {
            "from": "2023-02-01",
            "to": "2023-02-07",
            "metrics": ["impressions"]
        }
        # expected
        expected = {
            "rows": [
                {
                    "impressions": 0
                }
            ],
            "totalCount": 1
        }

        async with AsyncClient(base_url=TEST_BASE_URL) as ac:
            response = await ac.post(
                "/report",
                data=json.dumps(request_body)
            )
            self.assertEqual(status.HTTP_200_OK, response.status_code)
            self.assertEqual(expected, response.json())


if __name__ == "__main__":
    unittest.main()
