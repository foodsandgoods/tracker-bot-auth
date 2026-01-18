import os
import secrets
import urllib.parse

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse

app = FastAPI()

YANDEX_CLIENT_ID = os.getenv("YANDEX_CLIENT_ID", "")
YANDEX_CLIENT_SECRET = os.getenv("YANDEX_CLIENT_SECRET", "")
BASE_URL = os.getenv("BASE_URL", "")

AUTH_URL = "https://oauth.yandex.ru/authorize"
TOKEN_URL = "https://oauth.yandex.ru/token"


@app.get("/ping")
async def ping():
    return "pong"


@app.get("/")
async def root():
    return {
        "status": "ok",
        "oauth_configured": bool(YANDEX_CLIENT_ID and YANDEX_CLIENT_SECRET and BASE_URL),
        "base_url": BASE_URL,
        "client_id_set": bool(YANDEX_CLIENT_ID),
        "client_secret_set": bool(YANDEX_CLIENT_SECRET),
    }


@app.get("/oauth/start")
async def oauth_start():
    if not (YANDEX_CLIENT_ID and BASE_URL):
        return JSONResponse(
            {"error": "OAuth not configured. Set YANDEX_CLIENT_ID and BASE_URL."},
            status_code=500,
        )

    redirect_uri = f"{BASE_URL}/oauth/callback"
    state = secrets.token_urlsafe(16)

    params = {
        "response_type": "code",
        "client_id": YANDEX_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "state": state,
    }
    url = AUTH_URL + "?" + urllib.parse.urlencode(params)
    return RedirectResponse(url)


@app.get("/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    error = request.query_params.get("error")

    if error:
        return JSONResponse({"ok": False, "error": error, "params": dict(request.query_params)}, status_code=400)
    if not code:
        return JSONResponse({"ok": False, "error": "no_code", "params": dict(request.query_params)}, status_code=400)
    if not (YANDEX_CLIENT_ID and YANDEX_CLIENT_SECRET and BASE_URL):
        return JSONResponse({"ok": False, "error": "OAuth not configured"}, status_code=500)

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": YANDEX_CLIENT_ID,
        "client_secret": YANDEX_CLIENT_SECRET,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            TOKEN_URL,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": resp.text}
@app.get("/tracker/me")
async def tracker_me(token: str):
    """
    Тест: проверяем, что OAuth-токен даёт доступ к Tracker API.
    Временно принимаем token через query-параметр (это небезопасно, потом уберём).
    """
    url = "https://api.tracker.yandex.net/v2/myself"
    headers = {"Authorization": f"OAuth {token}"}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, headers=headers)

    try:
        payload = r.json()
    except Exception:
        payload = {"raw": r.text}

    return {"status_code": r.status_code, "response": payload}
    return JSONResponse(
        {"ok": resp.status_code == 200, "status_code": resp.status_code, "state": state, "token_response": payload},
        status_code=200 if resp.status_code == 200 else 400,
    )
