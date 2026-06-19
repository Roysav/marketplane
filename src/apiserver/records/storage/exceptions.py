from ...types import Subject


class KeyNotFound(Exception):
    def __init__(self, subject: Subject) -> None:
        self.subject = subject
        super().__init__({"subject": subject.key()})


class KeyAlreadyExists(Exception):
    def __init__(self, subject: Subject) -> None:
        self.subject = subject
        super().__init__({"subject": subject.key()})


class RevisionMismatch(Exception):
    def __init__(self, subject: Subject) -> None:
        self.subject = subject
        super().__init__({"subject": subject.key()})
