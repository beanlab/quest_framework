import inspect
from functools import wraps

from .historian import GLOBAL_VERSION, QUEST_VERSIONS, find_historian

DEFAULT_VERSION = ''


def version(global_version: str = None, **versions: str):
    # IMPORTANT: named after the value of QUEST_VERSIONS: _quest_versions
    _quest_versions = dict(versions)
    if global_version is not None:
        _quest_versions[GLOBAL_VERSION] = global_version

    def decorator(func):
        versions_discovered = False

        # noinspection PyProtectedMember
        @wraps(func)
        async def _quest_versioned_function(*args, **kwargs):
            nonlocal versions_discovered
            nonlocal _quest_versions  # save quest_versions in the local vars to lookup later
            if not versions_discovered:
                find_historian()._discover_versions(func, _quest_versions)
                versions_discovered = True
            return await func(*args, **kwargs)

        setattr(_quest_versioned_function, QUEST_VERSIONS, _quest_versions)
        return _quest_versioned_function

    return decorator


def get_version(version_name=GLOBAL_VERSION):
    outer_frame = inspect.currentframe()
    while outer_frame is not None:
        if outer_frame.f_code.co_name == '_quest_versioned_function' and \
                version_name in outer_frame.f_locals.get(QUEST_VERSIONS):
            module_name = outer_frame.f_locals.get('func').__module__
            func_name = outer_frame.f_locals.get('func').__qualname__
            return find_historian().get_version(module_name, func_name, version_name) or DEFAULT_VERSION

        # This function didn't have it, so move up the stack
        outer_frame = outer_frame.f_back

    # If I haven't found it by now, then that version is not defined
    # The default version is "", which sorts BEFORE all other strings
    # i.e. version "" is older than any other version
    return DEFAULT_VERSION
