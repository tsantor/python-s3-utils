class S3KeyNotFoundError(Exception):
    def __init__(self, message="Key not found."):
        self.message = message
        super().__init__(self.message)
