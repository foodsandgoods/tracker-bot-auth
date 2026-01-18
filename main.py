from fastapi import FastAPI, Request

app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok"}

@app.get("/ping")
async def ping():
    return "pong"

@app.get("/oauth/callback")
async def oauth_callback(request: Request):
    return dict(request.query_params)
