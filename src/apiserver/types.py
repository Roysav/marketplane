import pydantic


class Subject(pydantic.BaseModel):
    type: str
    tradespace: str
    name: str

    def key(self) -> str:
        return f"{self.type}/{self.tradespace}/{self.name}"

    @classmethod
    def from_key(cls, key: str) -> "Subject":
        type_, tradespace, name = key.split("/", 2)
        return cls(type=type_, tradespace=tradespace, name=name)
