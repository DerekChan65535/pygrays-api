class ResponseBase:
    # Move class-level attributes to instance initialization to avoid shared mutable state
    def __init__(self, is_success : bool | None = None, data : any = None, errors=None):
        self.errors = [] if errors is None else errors
        self.data = data
        self.is_success = is_success if is_success is not None else True  # Default to True

    def to_dict(self):
        return {
            "is_success": self.is_success,
            "data": self.data,
            "errors": self.errors
        }