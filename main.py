import os

import uvicorn
from fastapi import FastAPI

from spec_metrics import spec_metrics_get
from spec_dimensions import spec_dimensions_get
from report import report_post

app = FastAPI()

app.include_router(spec_metrics_get.router)
app.include_router(spec_dimensions_get.router)
app.include_router(report_post.router)


if __name__ == '__main__':
    os.environ['ENV'] = 'UVICORN'
    uvicorn.run("main:app")
