import sys
from pathlib import Path
from src.quest import LocalFileSystemBlobStorage, History, PersistentHistory

def main():
    args = sys.argv
    if len(args) != 4:
        print("usage: <saved state folder name> <namespace> <workflow id>")

    path = Path(f"{args[1]}/{args[2]}/{args[3]}")
    print(f"Path: {path}")
    storage = LocalFileSystemBlobStorage(path)
    history: History = PersistentHistory(args[3], storage)
    print(f"PersistentHistory:\n")

    for item in history._items:
        print(f'{item}\n')

if __name__ == '__main__':
    main()