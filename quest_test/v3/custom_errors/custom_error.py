class MyError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class WeirdCustomException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code

    def __str__(self):
        return f"{self.message} (Error Code: {self.code})"

    def get_code(self):
        return self.code


class CustomTestException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code

    def __str__(self):
        return f"{self.message} (Error Code: {self.code})"

    def get_code(self):
        return self.code