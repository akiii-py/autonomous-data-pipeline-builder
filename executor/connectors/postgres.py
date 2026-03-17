from typing import Any, Dict


def extract(config: Dict[str, Any]) -> Any:
    raise NotImplementedError(
        "postgres connector needs a DB driver package. In Phase 4 MVP use file/http connectors; "
        "Phase 4.1 can add psycopg-based postgres support."
    )


def load(payload: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    raise NotImplementedError(
        "postgres connector needs a DB driver package. In Phase 4 MVP use file/http connectors; "
        "Phase 4.1 can add psycopg-based postgres support."
    )
