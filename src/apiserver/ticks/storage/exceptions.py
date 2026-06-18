class KeyNotFound(Exception):
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(key)
