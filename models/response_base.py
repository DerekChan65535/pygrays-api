class ResponseBase:
    errors : list[str] = []
    data : any = None
    is_success : bool | None

    def __init__(self, is_success : bool | None = None, data : any = None, errors=None):
        if errors is None:
            errors = []
        self.is_success = is_success
        self.data = data
        self.errors = errors


    def to_json(self):
        return {
            "is_success": self.is_success,
            "data": self.data,
            "errors": self.errors
        }