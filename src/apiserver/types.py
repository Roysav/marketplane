import pydantic


class Subject(pydantic.BaseModel):
    type: str = pydantic.Field(min_length=1)
    tradespace: str = pydantic.Field(min_length=1)
    name: str = pydantic.Field(min_length=1)

    def key(self) -> str:
        return f"{self.type}/{self.tradespace}/{self.name}"

    @classmethod
    def from_key(cls, key: str) -> "Subject":
        type_, tradespace, name = key.split("/", 2)
        return cls(type=type_, tradespace=tradespace, name=name)
