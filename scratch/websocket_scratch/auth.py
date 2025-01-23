import jwt
from jwt import InvalidTokenError

SECRET_KEY = "C@n'tT0uchThis!"


async def authorize(websocket):
    try:
        token = websocket.request_headers['Authorization']
        if token.startswith("Bearer "):
            token = token.split(" ")[1]
        else:
            return False

        decoded_token = jwt.decode(token, SECRET_KEY, algorithms="HS256")
        return decoded_token
    except (KeyError, InvalidTokenError):
        return False


async def default_handler(manager, websocket, path):
    authorized = await authorize(websocket)
    if not authorized:
        await websocket.close(code=websocket.CLOSE_STATUS_POLICY_VIOLATION, reason="Unauthorized")
        return

    ident = authorized['ident']