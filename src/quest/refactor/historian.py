import asyncio
from asyncio import Task
from typing import Callable

from quest import step


class Historian:
    async def invoke_action(self, identity, target: str, *args, **kwargs) -> Task:
        """Returns the task from invoking target in a new task"""
        task_id = await self.start_action(target, *args, **kwargs)
        return await self.get_task(task_id)
    
    def start_action(self, identity, target: str, *args, **kwargs) -> str:
        """Returns the task ID from invoking target in a new task"""
        # calls _start_task and returns the resulting task ID
    
    def get_task(self, identity, task_id) -> Task:
        """Returns the Task associated with target"""
        
    def get_values(self, identity: str | None):
        ...
    
    async def _start_task(self, target: Callable, *args, **kwargs) -> Task:
        """
        
        :param target: 
        :param args: 
        :param kwargs: 
        :return: 
        """
    
    async def _run_step(self, target: Callable, *args, **kwargs):
        """
        Checks to see if `target` has already run.
        If it has, it returns the result.
        If it hasn't, it runs the `target`.
        :param target: 
        :param args: 
        :param kwargs: 
        :return: The result of calling `target`
        """
        
    
async def main(a, b=7):
    @step
    async def get_random() -> int:
        return random.random()
    
    c = a + await get_random()
    d = b * await get_random()
    
    return c + d


async def elsewhere():
    historian = Historian(actions={'main': main})
    result = await historian.invoke_action('main', 7, b=5)


async def main(b=7):
    ident = '123'

    @step
    async def get_random() -> int:
        return random.random()

    class Request:
        def __init__(self, message):
            self._message = message
            self._queue = asyncio.Queue()
            
        @value(ident) 
        def message(self):
            return _message
        
        @action(ident)
        async def supply_value(self, value):
            await self._queue.put(value)
            
        async def get_value(self):
            return await self._queue.get()
            
    
    with namespace('inputs', ident, Request('Supply a value for A')) as request:
        a = await request.get_value()
        
    c = a + await get_random()
    d = b * await get_random()

    return c + d


async def elsewhere():
    historian = Historian(actions={'main': main})
    task_id = await historian.start_action('main', b=5)
    ...
    historian.get_values()  # sees the message and the `supply_value` action
    ...
    await historian.invoke_action('inputs.supply_value', 7)
    ...
    result = await historian.get_task(task_id)
    
    
    
async def elsewhere():
    historian = Historian(actions={'main': main})
    task_id = await historian.start_action('main', b=5)
    task_id = await historian.start_action('main', b=23)  # Why would I ever call `main` twice?
    # Calling this main twice will result in an error because the namespace/resource collision
    # You could write a main that used different identities; this use-case makes sense
    # You could write a main that used an input to modify the namespace/resource names
    # Namespaces and resources are the API, so this would be akin to dynamically building the API at runtime
    # I guess you could do this. Quest should be simple and unopinionated. 


async def main(b=7):
    ident1 = '123'
    ident2 = 'abc'

    @step
    async def get_random() -> int:
        return random.random()

    @task
    async def get_user_input(ident):
        
        class Request:
            def __init__(self, message):
                self._message = message
                self._queue = asyncio.Queue()
    
            @value(ident)
            def message(self):
                return self._message
    
            @action(ident)
            async def supply_value(self, value):
                await self._queue.put(value)
    
            async def get_value(self):
                return await self._queue.get()

        with namespace('inputs', ident, Request('Supply a value for A')) as request:
            return await request.get_value()

    a1_task = get_user_input(ident1)
    a2_task = get_user_input(ident2)
    a1, a2 = await asyncio.gather(a1_task, a2_task)
    
    c = a1 * a2
    d = b * await get_random()

    return c + d

    
async def elsewhere():
    historian = Historian(actions={'main': main})
    task_id = await historian.start_action('main', b=5)
    ...
    historian.get_values('123')  # sees the message and the `supply_value` action
    historian.get_values('abc')  # sees the message and the `supply_value` action
    ...
    await historian.invoke_action('123', 'inputs.supply_value', 7)
    await historian.invoke_action('abc', 'inputs.supply_value', 12)
    ...
    result = await historian.get_task(task_id)
    
