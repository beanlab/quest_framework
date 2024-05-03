import sys
from main import run_main
from play_multi_guess import run_play_multi_guess

if __name__ == '__main__':
    sys.stderr = open("stderr.txt", "w") # make sure you add this to your .gitignore file

    print("Welcome to the quest demo")
    res = input("Enter \"m\" to include main.py in the demo, and/or \"g\" to enter a session of flexible multi-guess.\nEnter your choice in any order: ")

    for ch in res:
        if ch == "m":
            print("\n-------------------------------------")
            print("Running main.py")
            print("-------------------------------------\n")
            run_main(["--restart"])    

        if ch == "g":
            print("\n-------------------------------------")
            print("Running flexible_multi_guess.py")
            print("-------------------------------------\n")
            run_play_multi_guess()
