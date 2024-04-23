import sys
from pathlib import Path
from src.quest import LocalFileSystemBlobStorage, History, PersistentHistory

def main():
    if '-vb' in sys.argv:
        saved_state = Path('saved-state-main2b.py')
    else:
        saved_state = Path('saved-state-main2.py')

    storage = LocalFileSystemBlobStorage(saved_state / "multi-guess-game" / "multi-guess-game-1")
    history: History = PersistentHistory("multi-guess-game-1", storage)
    print(f"PersistentHistory:\n")

    for item in history._items:
        print(f'{item}\n')

if __name__ == '__main__':
    main()