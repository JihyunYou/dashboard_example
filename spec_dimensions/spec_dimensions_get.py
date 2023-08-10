from fastapi import APIRouter, status
from sqlalchemy import select

from common.database import get_db_session
from model.SpecDimensions import SpecDimensions

router = APIRouter()


@router.get("/spec/dimensions", status_code=status.HTTP_200_OK, name="dimension 조회 API", tags=["spec"])
async def spec_dimensions_get():
    db_session = get_db_session()

    stmt = select(SpecDimensions)

    result = db_session.scalars(stmt).all()

    response = [
        {
            "key": item.key,
            "name": item.name
        } for item in result
    ]

    return response
