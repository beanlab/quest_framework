from quest.context import these


class Context:
    def __init__(self, name, enter, exit):
        self.name = name
        self.enter = enter
        self.exit = exit

    def __enter__(self):
        print(self.name, 'entered')
        self.enter(self.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(self.name, 'exited')
        self.exit(self.name)


def test_context_dict():
    entered = []
    exited = []
    values = []
    with these({k: Context(k, entered.append, exited.append) for k in 'abcd'}) as data:
        for k, v in data.items():
            values.append(v.name)

    expected = list('abcd')
    assert entered == expected
    assert exited == expected
    assert values == expected


def test_context_list():
    entered = []
    exited = []
    with these(Context(k, entered.append, exited.append) for k in 'abcd') as data:
        values = [v.name for v in data]

    expected = list('abcd')
    assert entered == expected
    assert exited == expected
    assert values == expected


def test_generator():
    entered = []
    exited = []
    with contexts('abcd', entered.append, exited.append) as data:
        values = [v.name for v in data]

    expected = list('abcd')
    assert entered == expected
    assert exited == expected
    assert values == expected
