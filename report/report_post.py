import ast
from datetime import datetime
from fastapi import APIRouter, Body, status, HTTPException
from sqlalchemy import select, func, and_

from common.constant import ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_FROM, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM, ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_TO, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_TO, ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM_TO, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_METRICS, ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS, \
    ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS, DATE_FORMAT, ERROR_MESSAGE_INTERNAL_SERVER_ERROR
from common.database import get_db_session
from model.Events import Events
from model.SpecDimensions import SpecDimensions
from model.SpecMetrics import SpecMetrics
from report.RequestBody import RequestBody

router = APIRouter()
db_session = get_db_session()
operators = {
    "+": lambda x, y: x + y,
    "-": lambda x, y: x - y,
    "*": lambda x, y: x * y,
    "/": lambda x, y: x / y if x != 0 and y != 0 else 0
}
events_column = {
    "channel": Events.channel,
    "browser": Events.browser,
    "clicks": Events.clicks,
    "impressions": Events.impressions,
    "purchase_price": Events.purchase_price,
    "return_price": Events.return_price
}


@router.post("/report", status_code=status.HTTP_200_OK, name="데이터 조회 API", tags=["report"])
async def report_post(body: dict = Body()):
    request_body: RequestBody = extract_parameter(body)

    try:
        column_key_list, query_result = get_data(request_body)

        response = set_response(request_body, column_key_list, query_result)
    except Exception as exc:
        print(exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ERROR_MESSAGE_INTERNAL_SERVER_ERROR
        )

    return response


def extract_parameter(body: dict):
    # From
    from_date = body.get("from")
    if from_date is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_FROM
        )
    try:
        from_date = datetime.strptime(from_date, DATE_FORMAT)
    except Exception as exc:
        print(exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM
        )

    # To
    to_date = body.get("to")
    if to_date is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_TO
        )
    try:
        to_date = datetime.strptime(to_date, DATE_FORMAT)
    except Exception as exc:
        print(exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_TO
        )

    # From 일자가 To 일자보다 같거나 이전인지 확인
    validate_date(from_date, to_date)

    # Metrics
    metrics = body.get("metrics")
    if metrics is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_NOT_EXIST_METRICS
        )
    validate_metrics(metrics)

    # Dimensions
    dimensions = body.get("dimensions")
    if dimensions is not None:
        validate_dimensions(dimensions)

    sort_by_desc = body.get("sortByDesc")
    if sort_by_desc is None:
        sort_by_desc = metrics[0]

    return RequestBody(
        from_date=from_date,
        to_date=to_date,
        metrics=metrics,
        dimensions=dimensions,
        sort_by_desc=sort_by_desc
    )


def validate_date(from_date, to_date):
    if from_date > to_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_FROM_TO
        )


def validate_metrics(metrics):
    if type(metrics) != list or len(metrics) < 1 or len(metrics) > 5:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS
        )

    # DB에 존재하는 Metric 인지 확인
    valid_metrics = db_session.scalars(select(SpecMetrics.key)).all()
    for metric in metrics:
        if metric not in valid_metrics:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_METRICS
            )


def validate_dimensions(dimensions):
    if type(dimensions) != list or len(dimensions) > 5:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS
        )

    # DB에 존재하는 Dimension 인지 확인
    valid_dimensions = db_session.scalars(select(SpecDimensions.key)).all()
    for dimension in dimensions:
        if dimension not in valid_dimensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ERROR_MESSAGE_INVALID_BODY_CONTENTS_INVALID_VALUE_DIMENSIONS
            )


def get_data(request_body: RequestBody):
    column_key_list = []
    column_value_list = []

    # Request Dimension 정보 -> Add Select Column
    if request_body.dimensions is not None:
        for dimension in request_body.dimensions:
            column_key_list.append(dimension)
            column_value_list.append(events_column[dimension])

    # Request Metric 정보 -> Add Select Column
    temp_metric_list = request_body.metrics[:]
    for metric in temp_metric_list:
        if metric[:6] == "custom":
            # Case: custom metric
            # custom metric 계산에 필요한 metric 을 가져와서 기존 request에 없는 metric 이라면 추가
            custom_metric_info = get_custom_metric_info(metric)

            for custom_metric in custom_metric_info.get("metrics"):
                if custom_metric not in temp_metric_list:
                    temp_metric_list.append(custom_metric)
        else:
            column_key_list.append(metric)
            column_value_list.append(func.sum(events_column[metric]))

    stmt = select(*column_value_list).where(
        and_(
            Events.event_date >= request_body.from_date,
            Events.event_date <= request_body.to_date
        )
    )

    # Request 에 존재하는 dimension 에 대해 group_by 설정
    if request_body.dimensions is not None:
        for dimension in request_body.dimensions:
            stmt = stmt.group_by(dimension)

    result = db_session.execute(stmt).all()

    return column_key_list, result


def set_response(request_body: RequestBody, column_key_list, result):
    rows = []
    for row in result:
        item_dict = {
            column_key_list[idx]:
                column_value if type(column_value) == str else int(column_value)
                for idx, column_value in enumerate(row)
                if column_value is not None
        }
        if not item_dict:
            continue

        # 예외 처리
        if "channel" in item_dict and item_dict["channel"] == "ab180":
            if "purchase_price" in item_dict and item_dict["purchase_price"] < 500000:
                item_dict["purchase_price"] = 0

        for metric in request_body.metrics:
            if metric[:6] == "custom":
                custom_metric_info = get_custom_metric_info(metric)
                item_dict[metric] = cal_custom_metric_value(item_dict, custom_metric_info)

        for item_key in list(item_dict.keys()):
            if request_body.dimensions is not None:
                if item_key not in request_body.dimensions and item_key not in request_body.metrics:
                    item_dict.pop(item_key)
            else:
                if item_key not in request_body.metrics:
                    item_dict.pop(item_key)

        rows.append(item_dict)

    # 정렬: custom metric 의 계산을 쿼리 후 하기 때문에 여기서 정렬
    rows = sorted(rows, key=lambda d: d[request_body.sort_by_desc], reverse=True)

    # 조회 데이터가 없는 경우
    if len(rows) == 0:
        return {
            "rows": [
                {metric: 0 for metric in request_body.metrics}
            ],
            "totalCount": 1
        }

    return {
        "rows": rows,
        "totalCount": len(rows)
    }


def get_custom_metric_info(metric_key: str) -> dict:
    # Request Metrics 에 대해 DB에 존재하는 값들인지 앞에서 체크하기 때문에 DB에 없는 경우는 고려하지 않음
    metric_info: SpecMetrics = db_session.scalars(
        select(SpecMetrics).where(SpecMetrics.key == metric_key)
    ).first()

    return ast.literal_eval(metric_info.calculates)[0]


def cal_custom_metric_value(item: dict, custom_metric_info):
    # Custom Metric 값들에 대해 명세 조건으로 기존에 존재한는 Metric 이라 하였으므로, 없는 경우는 고려하지 않음
    x = item[custom_metric_info.get("metrics")[0]]
    y = item[custom_metric_info.get("metrics")[1]]
    operator = custom_metric_info.get("operator")

    return operators[operator](x, y)
