from dataclasses import dataclass
from typing import Any, Callable

from .historian_helpers import get_function_name, _get_current_timestamp, _get_id, _get_qualified_version
from .quest_types import ConfigurationRecord, VersionRecord
from .utils import quest_logger

GLOBAL_VERSION = "_global_version"


@dataclass(slots=True)
class EvolutionRuntimeContext:
    history: list
    get_external_task_name: Callable[[], str]
    get_task_name: Callable[[], str]
    get_next_record: Callable[[], Any]
    replay_has_completed: Callable[[], bool]
    existing_history: Callable[[], list]
    record_gates: Callable[[], dict]


class EvolutionRuntime:
    def __init__(self, context: EvolutionRuntimeContext):
        self._context = context
        self._configurations: list[tuple[Callable, list, dict]] = []
        self._configuration_pos = 0
        self._versions = {}
        self._discovered_versions = {}

    def reset(self):
        self._configuration_pos = 0
        self._versions = {}

    def configure(self, config_function, *args, **kwargs):
        if not callable(config_function):
            raise Exception(f'First argument to configure must be a callable. Received {config_function}.')

        self._configurations.append((config_function, list(args), kwargs))

    def add_new_configurations(self):
        config_records = [
            record
            for record in self._context.history
            if record['type'] == 'configuration'
        ]

        assert len(config_records) <= len(self._configurations)

        for record, (config_function, args, kwargs) in zip(config_records, self._configurations):
            assert record['function_name'] == get_function_name(config_function)
            assert record['args'] == args
            assert record['kwargs'] == kwargs

        for config_function, args, kwargs in self._configurations[len(config_records):]:
            quest_logger.debug(f'Adding new configuration: {get_function_name(config_function)}(*{args}, **{kwargs}')

            self._context.history.append(ConfigurationRecord(
                type='configuration',
                timestamp=_get_current_timestamp(),
                step_id='configuration',
                task_id=self._context.get_external_task_name(),
                function_name=get_function_name(config_function),
                args=args,
                kwargs=kwargs
            ))

    async def run_configuration(self, config_record: ConfigurationRecord):
        config_function, args, kwargs = self._configurations[self._configuration_pos]
        quest_logger.debug(f'Running configuration: {get_function_name(config_function)}(*{args}, **{kwargs})')

        assert config_record['function_name'] == get_function_name(config_function), str(config_record)
        assert config_record['args'] == args, str(config_record)
        assert config_record['kwargs'] == kwargs, str(config_record)

        await config_function(*args, **kwargs)
        self._configuration_pos += 1

    def get_version(self, module_name, function_name, version_name=GLOBAL_VERSION):
        version = self._versions.get(_get_qualified_version(module_name, function_name, version_name), None)
        quest_logger.debug(
            f'{self._context.get_task_name()} get_version({module_name}, {function_name}, {version_name} returned "{version}"')
        return version

    def discover_versions(self, function, versions: dict[str, str]):
        self._discovered_versions.update({
            _get_qualified_version(function.__module__, function.__qualname__, version_name): version
            for version_name, version in versions.items()
        })

        if self._context.replay_has_completed():
            self.process_discovered_versions()

    def process_discovered_versions(self):
        for version_name, version in self._discovered_versions.items():
            self.record_version_event(version_name, version)
        self._discovered_versions = {}

    def record_version_event(self, version_name, version):
        if self._versions.get(version_name, None) == version:
            return

        quest_logger.debug(f'Version record: {version_name} = {version}')
        self._versions[version_name] = version

        self._context.history.append(VersionRecord(
            type='set_version',
            timestamp=_get_current_timestamp(),
            step_id=version_name,
            task_id=self._context.get_external_task_name(),
            version=version
        ))

    def replay_version(self, record: VersionRecord):
        quest_logger.debug(f'{self._context.get_task_name()} setting version {record["step_id"]} = "{record["version"]}"')
        self._versions[record['step_id']] = record['version']

    async def after_version(self, module_name, func_name, version_name, version):
        version_name = _get_qualified_version(module_name, func_name, version_name)
        quest_logger.debug(f'{self._context.get_task_name()} is waiting for version {version_name}=={version}')

        found = False
        for record in self._context.existing_history():
            if record['type'] == 'version' \
                    and record['version_name'] == version_name \
                    and record['version'] == version:
                found = True
                await self._context.record_gates()[_get_id(record)]

        if not found:
            quest_logger.error(f'{self._context.get_task_name()} did not find version {version_name}=={version}')
            raise Exception(f'{self._context.get_task_name()} did not find version {version_name}=={version}')

        if (next_record := await self._context.get_next_record()) is not None:
            with next_record as record:
                assert record['type'] == 'after_version', str(record)
                assert record['version_name'] == version_name, str(record)
                assert record['version'] == version, str(record)
        else:
            self._context.history.append(VersionRecord(
                type='after_version',
                timestamp=_get_current_timestamp(),
                step_id='version',
                task_id=self._context.get_task_name(),
                version=version
            ))
