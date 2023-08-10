import unittest
from datetime import datetime
from decimal import Decimal

from fastapi import HTTPException, status
from sqlalchemy import delete

from common.constant import ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_FROM, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM, ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_TO, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_TO, ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM_TO, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_METRICS, ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS, DATE_FORMAT
from common.database import get_db_session
from model.SpecMetrics import SpecMetrics
from report.RequestBody import RequestBody
from report.report_post import get_data, set_response, extract_parameter, get_custom_metric_info, \
    cal_custom_metric_value


class TestReportPostUnit(unittest.TestCase):
    def setUp(self) -> None:
        self.db_session = get_db_session()

    def tearDown(self) -> None:
        self.db_session.close()

    def test_extract_parameter_from_not_exist_case(self):
        # given
        request_body = {}

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_FROM, exc.exception.detail)

    def test_extract_parameter_from_invalid_case(self):
        # given
        request_body = {"from": "20231211"}

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM, exc.exception.detail)

    def test_extract_parameter_to_not_exist_case(self):
        # given
        request_body = {
            "from": "2023-01-01"
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_TO, exc.exception.detail)

    def test_extract_parameter_to_invalid_case(self):
        # given
        request_body = {
            "from": "2023-01-01",
            "to": "12541546"
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_TO, exc.exception.detail)

    def test_extract_parameter_from_to_invalid_case(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-04-01"
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM_TO, exc.exception.detail)

    def test_extract_parameter_metrics_not_exist_case(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05"
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_METRICS, exc.exception.detail)

    def test_extract_parameter_metrics_invalid_case_not_list(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": "test"
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS, exc.exception.detail)

    def test_extract_parameter_metrics_invalid_case_lt_1(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": []
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS, exc.exception.detail)

    def test_extract_parameter_metrics_invalid_case_gt_5(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": ["clicks", "impressions", "purchase_price", "return_price", "custom_1", "custom_2"]
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS, exc.exception.detail)

    def test_extract_parameter_metrics_invalid_case_not_in_db(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": ["pay"]
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS, exc.exception.detail)

    def test_extract_parameter_dimensions_invalid_case_not_list(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": ["clicks", "impressions"],
            "dimensions": "test"
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS, exc.exception.detail)

    def test_extract_parameter_dimensions_invalid_case_gt_5(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": ["clicks", "impressions"],
            "dimensions": ["test1", "test2", "test3", "test4", "test5", "test6"]
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS, exc.exception.detail)

    def test_extract_parameter_dimensions_invalid_case_not_in_db(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": ["clicks", "impressions"],
            "dimensions": ["channel", "browser", "app"]
        }

        # when
        with self.assertRaises(HTTPException) as exc:
            extract_parameter(request_body)

        # then
        self.assertEqual(status.HTTP_400_BAD_REQUEST, exc.exception.status_code)
        self.assertEqual(ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS, exc.exception.detail)

    def test_extract_parameter_sort_by_not_exist(self):
        # given
        request_body = {
            "from": "2023-05-01",
            "to": "2023-05-05",
            "metrics": ["clicks", "impressions"],
            "dimensions": ["channel", "browser"]
        }

        # when
        result: RequestBody = extract_parameter(request_body)

        self.assertEqual(request_body.get("metrics")[0], result.sort_by_desc)

    def test_get_custom_metric_info(self):
        # given
        spec_metrics_obj = SpecMetrics(
            key="test_custom_1",
            name="테스트 용 metric",
            calculates="""
                [{"metrics": ["return_price", "buy"], "operator": "^"}]
            """
        )
        self.db_session.add(spec_metrics_obj)
        self.db_session.commit()

        # when
        result = get_custom_metric_info(spec_metrics_obj.key)

        # then
        self.assertEqual(
            {"metrics": ["return_price", "buy"], "operator": "^"},
            result
        )

        # finally
        self.db_session.execute(
            delete(SpecMetrics).where(SpecMetrics.id == spec_metrics_obj.id)
        )
        self.db_session.commit()

    def test_cal_custom_metric_value(self):
        # given
        item = {"value_1": 10, "value_2": 20}
        custom_metric_info = {"metrics": ["value_1", "value_2"], "operator": "+"}

        # when
        result = cal_custom_metric_value(item, custom_metric_info)

        # then
        self.assertEqual(30, result)

    def test_cal_custom_metric_value_zero_case(self):
        # given
        item_1 = {"value_1": 0, "value_2": 20}
        item_2 = {"value_1": 7, "value_2": 0}
        item_3 = {"value_1": 0, "value_2": 0}
        custom_metric_info = {"metrics": ["value_1", "value_2"], "operator": "/"}

        # when
        result = cal_custom_metric_value(item_1, custom_metric_info)

        # then
        self.assertEqual(0, result)

        # when
        result = cal_custom_metric_value(item_2, custom_metric_info)

        # then
        self.assertEqual(0, result)

        # when
        result = cal_custom_metric_value(item_3, custom_metric_info)

        # then
        self.assertEqual(0, result)

    def test_get_data(self):
        # given
        request_body = RequestBody(
            from_date=datetime.strptime("2023-01-01", DATE_FORMAT),
            to_date=datetime.strptime("2023-01-02", DATE_FORMAT),
            metrics=["clicks", "return_price"],
            dimensions=["channel"],
            sort_by_desc="clicks"
        )

        # when
        column_key_list, query_result = get_data(request_body)

        # then
        # print(column_key_list)
        # print(query_result)
        self.assertEqual(request_body.dimensions + request_body.metrics, column_key_list)
        self.assertIsNotNone(query_result)

    def test_set_response_data_not_exist_case(self):
        # given
        request_body = RequestBody(
            from_date=datetime.strptime("2023-01-01", DATE_FORMAT),
            to_date=datetime.strptime("2023-01-02", DATE_FORMAT),
            metrics=["clicks", "return_price"],
            dimensions=["channel"],
            sort_by_desc="clicks"
        )
        column_key_list = request_body.dimensions + request_body.metrics
        query_result_1 = []
        query_result_2 = [(None,)]

        # when
        response = set_response(request_body, column_key_list, query_result_1)

        # then
        # print(response)
        self.assertEqual(1, response.get("totalCount"))
        self.assertEqual(
            [{"clicks": 0, "return_price": 0}],
            response.get("rows")
        )

        # when
        response = set_response(request_body, column_key_list, query_result_2)

        # then
        # print(response)
        self.assertEqual(1, response.get("totalCount"))
        self.assertEqual(
            [{"clicks": 0, "return_price": 0}],
            response.get("rows")
        )

    def test_set_response_ab180_purchase_price_lt_500000_case(self):
        # given
        request_body = RequestBody(
            from_date=datetime.strptime("2023-01-01", DATE_FORMAT),
            to_date=datetime.strptime("2023-01-02", DATE_FORMAT),
            metrics=["clicks", "purchase_price"],
            dimensions=["channel", "browser"],
            sort_by_desc="clicks"
        )
        column_key_list = request_body.dimensions + request_body.metrics
        query_result = [
            ("ab180", "chrome", Decimal(4), Decimal(495000)),
            ("ab180", "fox", Decimal(3), Decimal(720000)),
            ("jihyun", "chrome", Decimal(2), Decimal(350000)),
            ("jihyun", "safari", Decimal(1), Decimal(1200000))
        ]

        # when
        response = set_response(request_body, column_key_list, query_result)

        # then
        # print(response)
        self.assertEqual(0, response.get("rows")[0].get("purchase_price"))
        self.assertEqual(720000, response.get("rows")[1].get("purchase_price"))
        self.assertEqual(350000, response.get("rows")[2].get("purchase_price"))
        self.assertEqual(1200000, response.get("rows")[3].get("purchase_price"))

    def test_set_response_sort(self):
        # given
        request_body = RequestBody(
            from_date=datetime.strptime("2023-01-01", DATE_FORMAT),
            to_date=datetime.strptime("2023-01-02", DATE_FORMAT),
            metrics=["clicks", "purchase_price"],
            dimensions=["channel"],
            sort_by_desc="clicks"
        )
        column_key_list = request_body.dimensions + request_body.metrics
        query_result = [
            ("3", Decimal(2), Decimal(495000)),
            ("2", Decimal(4), Decimal(720000)),
            ("4", Decimal(1), Decimal(350000)),
            ("1", Decimal(7), Decimal(1200000))
        ]

        # when
        response = set_response(request_body, column_key_list, query_result)

        # then
        # print(response)
        self.assertEqual("1", response.get("rows")[0].get("channel"))
        self.assertEqual("2", response.get("rows")[1].get("channel"))
        self.assertEqual("3", response.get("rows")[2].get("channel"))
        self.assertEqual("4", response.get("rows")[3].get("channel"))


if __name__ == "__main__":
    unittest.main()
