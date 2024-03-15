def do_foo(): ...


def do_bar(): ...


def workflow():
    do_foo()
    do_bar()


def do_more_foo(): ...


@version('2')
def workflow_v2():
    do_foo()
    if get_version() == '2':  # Just a state lookup, not a step, not in the history?
        do_more_foo()
    do_bar()
