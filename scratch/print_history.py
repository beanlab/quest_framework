from pathlib import Path

from history import LocalFileSystemBlobStorage, PersistentList


def print_record(record):
    print(record)
    # print(record['timestamp'], record['type'], record['task_id'], record['step_id'])


def main(wid: str, namespace_folder: Path):
    history = PersistentList(wid, LocalFileSystemBlobStorage(namespace_folder / wid))
    for record in history:
        print_record(record)


if __name__ == '__main__':
    main('sleep_workflow', Path('ainput_state/sleep'))
