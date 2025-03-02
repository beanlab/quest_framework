async def _authorize(self, ws):
    try:
        token = ws.request_headers['Authorization']
        if token.startswith("Bearer "):
            token = token.split(" ")[1]
        else:
            return False

        jwt.decode(token, SECRET_KEY, algorithms="HS256")
        return True
    except (KeyError, InvalidTokenError):
        return False