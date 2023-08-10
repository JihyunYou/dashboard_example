from fastapi import APIRouter, status
from sqlalchemy import select

from common.database import get_db_session
from model.SpecMetrics import SpecMetrics

router = APIRouter()


@router.get("/spec/metrics", status_code=status.HTTP_200_OK, name="metric 조회 API", tags=["spec"])
async def spec_metrics_get():
    db_session = get_db_session()

    stmt = select(SpecMetrics)
    result = db_session.scalars(stmt).all()

    response = [
        {
            "key": item.key,
            "name": item.name
        } for item in result
    ]

    return response
