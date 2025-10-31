from importlib.metadata import entry_points

from gauss.core.ports.kv_storage import BaseKVStorage


def load_storage(name: str, **kwargs) -> BaseKVStorage:
    storages = entry_points(group="gauss.storages")

    entry = next((e for e in storages if e.name == name), None)

    if not entry:
        raise ValueError(f"Storage plugin '{name}' not found")

    cls = entry.load()

    return cls(**kwargs)
