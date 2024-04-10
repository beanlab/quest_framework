import asyncio
import signal
from typing import Protocol, Callable
from contextvars import ContextVar

from .historian import Historian
from .history import History
from .persistence import BlobStorage

manager_context = ContextVar('manager'); manager_context.set(None)
guarded_signals: list[signal.signal] = [signal.SIGINT, signal.SIGABRT, signal.SIGTERM]
signal_defaults = {}

def custom_signal_handler(signal_num, frame):
# def custom_signal_handler():
        # print("\nhandler entered")
        # old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, guarded_signals)
        # print("manager exiting...")

        # def how_to_exit():
        #     print("...done")
        #     signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
        #     restore_default_signal_handlers()
        #     signal.raise_signal(signal_num)

        # manager: WorkflowManager = manager_context.get('manager')
        # if manager is not None: manager._exit_gracefully(how_to_exit)
        exit(1) 

def loop_signal_handler(*args):
    # asyncio.get_running_loop().call_soon() # USEFUL!
    # manager: WorkflowManager = manager_context.get('manager')
    # if manager is not None:
        # def how_to_exit():
        #     exit(77)

        # TODO: lookup and use the asyncio debug stuff so you can see everything that is inside
        
        # asyncio.get_running_loop().call_soon(manager._exit_gracefully, manager, how_to_exit)

    # raise KeyboardInterrupt
    exit(1)

def restore_default_signal_handlers():
    for sig in guarded_signals:
        signal.signal(sig, signal_defaults[sig])

class HistoryFactory(Protocol):
    def __call__(self, workflow_id: str) -> History: ...


class WorkflowFactory(Protocol):
    def __call__(self, workflow_type: str) -> Callable: ...


class WorkflowManager:
    """
    Runs workflow tasks
    It remembers which tasks are still active and resumes them on replay
    """

    def __init__(self, namespace: str, storage: BlobStorage, create_history: HistoryFactory,
                 create_workflow: WorkflowFactory):
        self._namespace = namespace
        self._storage = storage
        self._create_history = create_history
        self._create_workflow = create_workflow
        self._workflow_data = {}
        self._workflows: dict[str, Historian] = {}
        self._workflow_tasks: dict[str, asyncio.Task] = {}

    def _set_up_signal_handlers(self):
        for sig in guarded_signals:
            signal_defaults[sig] = signal.getsignal(sig)
            # signal.signal(sig, custom_signal_handler)
            asyncio.get_running_loop().add_signal_handler(sig, loop_signal_handler)

    async def _exit_gracefully(self, how_to_exit: Callable):
        self._storage.write_blob(self._namespace, self._workflow_data)
        for wid, historian in self._workflows.items():
            await historian.suspend()

        if how_to_exit is not None and callable(how_to_exit):
            ret = how_to_exit()
            if hasattr(ret, '__await__'):
                await ret
                
    async def __aenter__(self):
        """Load the workflows and get them running again"""
        # print("manager __aenter__ begun")
        manager_context.set(self)
        self._set_up_signal_handlers()
        # we don't need to worry about blocking/handling signals before this point because nothing is running yet
        if self._storage.has_blob(self._namespace):
            self._workflow_data = self._storage.read_blob(self._namespace)

        for wtype, wid, args, kwargs, background in self._workflow_data.values():
            self._start_workflow(wtype, wid, args, kwargs, delete_on_finish=background)

        # print("manager __aenter__ exiting")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Save whatever state is necessary before exiting"""
        def how_to_exit():
            restore_default_signal_handlers()
            manager_context.set(None)

        await self._exit_gracefully(how_to_exit)

    def _get_workflow(self, workflow_id: str):
        return self._workflows[workflow_id]  # TODO check for key, throw error

    def _remove_workflow(self, workflow_id: str, context):
        self._workflows.pop(workflow_id)
        self._workflow_tasks.pop(workflow_id)
        del self._workflow_data[workflow_id]

    def _start_workflow(self,
                        workflow_type: str, workflow_id: str, workflow_args, workflow_kwargs,
                        delete_on_finish=False):
        workflow_function = self._create_workflow(workflow_type)

        history = self._create_history(workflow_id)
        historian: Historian = Historian(workflow_id, workflow_function, history)
        self._workflows[workflow_id] = historian

        self._workflow_tasks[workflow_id] = (task := historian.run(*workflow_args, **workflow_kwargs))

        if delete_on_finish:
            task.add_done_callback(lambda t: self._remove_workflow(workflow_id, t))

    def start_workflow(self, workflow_type: str, workflow_id: str, 
                       delete_on_finish: bool,*workflow_args, **workflow_kwargs) -> asyncio.Task:
        
        """Start the workflow.
            Returns the asyncio.Task associated with the workflow.
            This allows the workflow to be awaited and the result retrieved."""
        if workflow_id not in self._workflow_data.keys():
            self._workflow_data[workflow_id] = (workflow_type, workflow_id, workflow_args, workflow_kwargs, delete_on_finish)
            self._start_workflow(workflow_type, workflow_id, workflow_args, workflow_kwargs, delete_on_finish)
        
        return self._workflow_tasks[workflow_id]

    def has_workflow(self, workflow_id: str) -> bool:
        return workflow_id in self._workflows

    def get_workflow(self, workflow_id: str) -> asyncio.Task:
        return self._workflow_tasks[workflow_id]

    async def suspend_workflow(self, workflow_id: str):
        await self._get_workflow(workflow_id).suspend()

    async def get_resources(self, workflow_id: str, identity):
        return await self._get_workflow(workflow_id).get_resources(identity)

    async def send_event(self, workflow_id: str, name: str, identity, action, *args, **kwargs):
        workflow_task = self._workflow_tasks[workflow_id]

        # TODO: asking to send an event to a finished task should throw an error
        result = False
        if not workflow_task.done():
            result = await self._get_workflow(workflow_id).record_external_event(name, identity, action, *args, **kwargs)
            
        return result
