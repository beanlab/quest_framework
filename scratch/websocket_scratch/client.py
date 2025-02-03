import json
import websockets


# Thoughts:
# Maybe we ought to create two different classes that can compartmentalize the different functionality of the
# stream vs call route on the server...
class Client:
    def __init__(self, uri):
        self.uri = uri
        self.ws = None

    @classmethod
    async def call(cls, uri: str):
        instance = cls(uri)
        await instance.__aenter__()
        return instance

    @classmethod
    async def stream(cls, uri: str):
        instance = cls(uri)
        await instance.__aenter__()
        return instance

    async def __aenter__(self):
        self.ws = await websockets.connect(self.uri)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.ws:
            await self.ws.close()
