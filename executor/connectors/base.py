from typing import Any, Dict


class Connector:
    def extract(self, config: Dict[str, Any]) -> Any:
        raise NotImplementedError

    def load(self, payload: Any, config: Dict[str, Any]) -> Any:
        raise NotImplementedError
