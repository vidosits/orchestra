import logging

from fastapi import FastAPI

app = FastAPI()

# uvicorn 
for name in logging.root.manager.loggerDict.keys():
    logging.getLogger(name).handlers = []
    logging.getLogger(name).propagate = True


@app.get("/")
def read_root():
    return {"Hello": "World"}
