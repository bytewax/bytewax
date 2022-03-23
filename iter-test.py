import asyncio
import abc

async def output(it):
    print("before")
    async for i in it:
        print("for i:", i)
    print("after")


class CaptureAiter(abc.Generator):
    def __init__(self):
        self.state = None
        
    def __aiter__(self):
        return self

    def __anext__(self):
        return self

    def __await__(self):
        return self

    def __next__(self):
        print("next:", self.state)
        return None

    def send(self, value):
        print("sent:", value)
        self.state = value
        return self

class C2(abc.AsyncGenerator):
    def asend(self, value):
        pass

    def athrow(self, 
    
it = CaptureAiter()

coro = output(it)
coro.send(None)
coro.send(1)
coro.send(2)
coro.send(3)
try:
    coro.throw(StopAsyncIteration())
except StopIteration:
    pass
