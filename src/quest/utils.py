import asyncio
import logging
import sys

async def ainput(*args):
    return await asyncio.to_thread(input, *args)

class QuestLogger(logging.Logger):
    def makeRecord(self, *args, **kwargs):
        rv = super(QuestLogger, self).makeRecord(*args, **kwargs)
        rv.__dict__["Task"] = rv.__dict__.get("Task", "-")
        return rv


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s : %(levelname)s : %(message)s',
    handlers=[logging.StreamHandler()],
)

quest_logger = QuestLogger('quest')
quest_logger.propagate = False
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
formatter = logging.Formatter('%(asctime)s | %(Task)s | %(levelname)s | %(message)s')
handler.setFormatter(formatter)
quest_logger.addHandler(handler)