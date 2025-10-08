from typing import Any, Dict

class BaseModel:
    model_config: Dict[str, Any]

    def __init__(self, **data: Any) -> None: ...

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]: ...


def Field(default: Any = ..., **kwargs: Any) -> Any: ...

ConfigDict = Dict[str, Any]
