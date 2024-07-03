"""Module contains custom exceptions."""


class CMAPIBasicError(Exception):
    """Basic exception raised for CMAPI related processes.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)
    def __str__(self) -> str:
        return self.message


class CEJError(CMAPIBasicError):
    """Exception raised for CEJ related processes.

    Attributes:
        message -- explanation of the error
    """
