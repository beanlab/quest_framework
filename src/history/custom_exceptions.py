class CustomSigintException(BaseException):
    """Custom exception to simulate a SIGINT (KeyboardInterrupt)"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise KeyboardInterrupt