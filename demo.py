import sys
from main import run_main
from demos.flexible_multi_guess import run_flexible_multi_guess

if __name__ == '__main__':
    print("Running main.py example script")
    print("-------------------------------------")
    run_main()    

    print("\n-------------------------------------")
    print("Running flexible_multi_guess.py")
    flags = input("Enter -w for WorkflowManager or -h for historian.\nIncluding -r with 0 or 1 other flags will wipe the json files.\nFlags: ")
    print("-------------------------------------")
    run_flexible_multi_guess(flags.split(" "))
    