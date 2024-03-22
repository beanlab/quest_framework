import sys
from main import run_main
from demos.flexible_multi_guess import run_flexible_multi_guess

if __name__ == '__main__':
    sys.stderr = open("err.txt", "w") # make sure you add this to your .gitignore file

    print("Welcome to the quest demo")
    res = input("Enter \"m\" to include main.py in the demo, and/or \"f\" to enter a session of flexible multi-guess.\nEnter your choice in any order: ")

    for ch in res:
        if ch == "m":
            print("\n-------------------------------------")
            print("Running main.py example script")
            print("-------------------------------------")
            run_main()    

        if ch == "f":
            print("\n-------------------------------------")
            print("Running flexible_multi_guess.py")
            flags = input("Enter -w for WorkflowManager or -h for historian.\nIncluding -r with 0 or 1 other flags will wipe the json files.\nFlags: ")
            print("-------------------------------------")
            run_flexible_multi_guess(flags.split(" "))
