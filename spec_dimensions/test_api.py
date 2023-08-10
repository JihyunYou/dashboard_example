import unittest
from unittest import IsolatedAsyncioTestCase
from httpx import AsyncClient
from fastapi import status

from common.constant import TEST_BASE_URL


class TestSpecDimensionsGetApi(IsolatedAsyncioTestCase):
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
            {'key': 'channel', 'name': '광고 채널 이름'},
            {'key': 'browser', 'name': '브라우저 이름'}
        ]

        async with AsyncClient(base_url=TEST_BASE_URL) as ac:
            response = await ac.get("/spec/dimensions")
            self.assertEqual(status.HTTP_200_OK, response.status_code)
            self.assertEqual(expected, response.json())


if __name__ == "__main__":
    unittest.main()
