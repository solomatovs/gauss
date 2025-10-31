from typing import Dict, Any, get_origin, get_args, Union, Optional
from enum import Enum

from pydantic import BaseModel, PrivateAttr, ConfigDict

from gauss.core.ports.kv_storage import BaseKVStorage
from gauss.core.domain.kv_storage import MemoryKVStorage
from gauss.core.helper.asyncio import AsyncioHelper


class ValueType(Enum):
    SCALAR = "scalar"
    PYDANTIC_MODEL = "model"
    LIST = "list"
    DICT = "dict"


class KVModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)

    _storage: BaseKVStorage = PrivateAttr(default_factory=MemoryKVStorage)
    _key_prefix: str = PrivateAttr()
    _separator: str = PrivateAttr(default=".")

    def __init__(self, **data):
        super().__init__(**data)

        if "_key_prefix" not in self.__dict__ or self._key_prefix is None:
            object.__setattr__(self, "_key_prefix", f"{self.__class__.__name__}")

        if "_storage" not in self.__dict__:
            object.__setattr__(self, "_storage", MemoryKVStorage())

        for field_name in self.__class__.model_fields:
            if field_name in self.__dict__:
                value = self.__dict__[field_name]
                AsyncioHelper.run_sync(self._save_field_to_storage(field_name, value))

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Гарантируем наличие критичных настроек
        if not hasattr(cls, "model_config"):
            cls.model_config = ConfigDict()

        # Принудительно устанавливаем обязательные параметры
        cls.model_config["arbitrary_types_allowed"] = True
        cls.model_config["validate_assignment"] = True

    @classmethod
    async def from_storage(
        cls,
        storage: BaseKVStorage,
        key_prefix: str,
        if_not_exists: Optional["KVModel"] = None,
        separator: str = ".",
    ):
        existing_data = await storage.get_all(key_prefix)

        if not existing_data and if_not_exists is not None:
            instance = if_not_exists
            instance._storage = storage
            instance._key_prefix = key_prefix
            instance._separator = separator

            for field_name in instance.__class__.model_fields:
                if field_name in instance.__dict__:
                    value = instance.__dict__[field_name]
                    await instance._save_field_to_storage(field_name, value)
                    del instance.__dict__[field_name]

            return instance

        instance = cls.model_construct()
        object.__setattr__(instance, "_storage", storage)
        object.__setattr__(instance, "_key_prefix", key_prefix)
        object.__setattr__(instance, "_separator", separator)

        for field_name in cls.model_fields:
            if field_name in instance.__dict__:
                del instance.__dict__[field_name]

        return instance

    def _is_storage_bound(self) -> bool:
        return self._storage is not None and self._key_prefix is not None

    def _build_full_key(self, path: str) -> str:
        if not path:
            return self._key_prefix
        return f"{self._key_prefix}{self._separator}{path}"

    def _get_field_type(self, field_name: str) -> type | None:
        return self.__class__.model_fields[field_name].annotation

    def _get_value_type(self, value: Any) -> ValueType:
        if isinstance(value, BaseModel):
            return ValueType.PYDANTIC_MODEL
        elif isinstance(value, dict):
            return ValueType.DICT
        elif isinstance(value, (list, tuple)):
            return ValueType.LIST
        else:
            return ValueType.SCALAR

    def _extract_real_type(self, field_type: type) -> tuple[Any | None, Any | type]:
        origin = get_origin(field_type)  # type | None
        args = get_args(field_type)

        if origin is Union:
            non_none_types = [arg for arg in args if arg is not type(None)]
            if non_none_types:
                field_type = non_none_types[0]
                origin = get_origin(field_type)
                args = get_args(field_type)

        real_type = args[0] if args else field_type

        return origin, real_type  # (type | None, type)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return

        if name not in self.__class__.model_fields:
            super().__setattr__(name, value)
            return

        if self._is_storage_bound():
            AsyncioHelper.run_sync(self._delete_field_from_storage(field_name=name))
            AsyncioHelper.run_sync(self._save_field_to_storage(name, value))

            if name in self.__dict__:
                del self.__dict__[name]
        else:
            super().__setattr__(name, value)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_") or name in (
            "model_fields",
            "model_config",
            "__dict__",
            "__class__",
        ):
            return object.__getattribute__(self, name)

        try:
            if name not in object.__getattribute__(self, "__class__").model_fields:
                return object.__getattribute__(self, name)
        except AttributeError:
            return object.__getattribute__(self, name)

        try:
            dict_obj = object.__getattribute__(self, "__dict__")
            if name in dict_obj:
                return dict_obj[name]

            storage = object.__getattribute__(self, "_storage")
            key_prefix = object.__getattribute__(self, "_key_prefix")

            if storage is None or key_prefix is None:
                return object.__getattribute__(self, name)

            field_type = (
                object.__getattribute__(self, "__class__").model_fields[name].annotation
            )
            value = AsyncioHelper.run_sync(
                self._load_field_from_storage(name, field_type)
            )

            return value
        except AttributeError:
            return object.__getattribute__(self, name)

    async def _save_field_to_storage(
        self, field_name: str, value: Any, path: str = ""
    ) -> None:
        if not self._is_storage_bound():
            return

        current_path = f"{path}{self._separator}{field_name}" if path else field_name
        value_type = self._get_value_type(value)

        if value_type == ValueType.SCALAR:
            await self._save_scalar_to_storage(current_path, value)
            return

        if value_type == ValueType.PYDANTIC_MODEL:
            await self._save_pydantic_model_to_storage(current_path, value)
            return

        if value_type == ValueType.LIST:
            await self._save_list_to_storage(current_path, value)
            return

        if value_type == ValueType.DICT:
            await self._save_dict_to_storage(current_path, value)
            return

    async def _save_scalar_to_storage(self, path: str, value: Any) -> None:
        if value is None:
            return

        full_key = self._build_full_key(path)
        serialized = self._serialize_scalar(value)
        await self._storage.set(full_key, serialized)

    async def _save_pydantic_model_to_storage(
        self, path: str, value: BaseModel
    ) -> None:
        if isinstance(value, KVModel):
            await self._save_persistent_model(path, value)
            return

        # Обычная Pydantic модель
        data = value.model_dump()
        for field_name, field_value in data.items():
            await self._save_field_to_storage(field_name, field_value, path=path)

    async def _save_persistent_model(self, path: str, model: "KVModel") -> None:
        """Сохраняет PersistentModel, перемещая её в наш storage"""
        nested_path = self._build_full_key(path)

        # Собираем данные до переключения storage
        nested_data = {}
        for field_name in model.__class__.model_fields:
            nested_data[field_name] = getattr(model, field_name)

        # Переключаем модель на наш storage
        model._storage = self._storage
        model._key_prefix = nested_path
        model._separator = self._separator

        # Сохраняем все поля и чистим __dict__
        for field_name, field_value in nested_data.items():
            await model._save_field_to_storage(field_name, field_value)

            if field_name in model.__dict__:
                del model.__dict__[field_name]

    async def _save_list_to_storage(self, path: str, value: list) -> None:
        # Сохраняем длину списка
        length_key = self._build_full_key(f"{path}._length")
        await self._storage.set(length_key, str(len(value)))

        # Сохраняем каждый элемент
        for i, item in enumerate(value):
            item_path = f"{path}[{i}]"
            await self._save_list_item(item_path, item)

    async def _save_list_item(self, item_path: str, item: Any) -> None:
        """Сохраняет один элемент списка"""
        item_type = self._get_value_type(item)

        if item_type == ValueType.SCALAR:
            full_key = self._build_full_key(item_path)
            await self._storage.set(full_key, self._serialize_scalar(item))
            return

        if item_type == ValueType.PYDANTIC_MODEL:
            if isinstance(item, KVModel):
                await self._save_persistent_model(item_path, item)
            else:
                data = item.model_dump()
                for field_name, field_value in data.items():
                    await self._save_field_to_storage(
                        field_name, field_value, path=item_path
                    )
            return

        if item_type == ValueType.DICT:
            await self._save_dict_to_storage(item_path, item)
            return

    async def _save_dict_to_storage(self, path: str, value: dict) -> None:
        """Сохраняет словарь как плоские ключи"""
        for dict_key, dict_value in value.items():
            dict_path = f"{path}.{dict_key}"
            full_key = self._build_full_key(dict_path)
            await self._storage.set(full_key, self._serialize_scalar(dict_value))

    async def _delete_field_from_storage(self, field_name: str, path: str = "") -> None:
        if not self._is_storage_bound():
            return

        current_path = f"{path}{self._separator}{field_name}" if path else field_name
        prefix = self._build_full_key(current_path)
        await self._storage.delete_by_prefix(prefix)

    async def _load_field_from_storage(
        self, field_name: str, field_type: type, path: str = ""
    ) -> Any:
        if not self._is_storage_bound():
            if field_name in self.__dict__:
                return self.__dict__[field_name]
            return None

        current_path = f"{path}{self._separator}{field_name}" if path else field_name
        origin, real_type = self._extract_real_type(field_type)

        if origin is list:
            return await self._load_list_from_storage(current_path, field_type)

        elif origin is dict:
            return await self._load_dict_from_storage(current_path)

        elif isinstance(real_type, type) and issubclass(real_type, BaseModel):
            return await self._load_model_from_storage(current_path, real_type)

        else:
            full_key = self._build_full_key(current_path)
            raw_value = await self._storage.get(full_key)

            if raw_value is None:
                return None

            deserialized = self._deserialize_scalar(raw_value)
            return self._cast_scalar_to_type(deserialized, field_type)

    async def _load_list_from_storage(self, path: str, field_type: type) -> list:
        length_key = self._build_full_key(f"{path}._length")
        length_raw = await self._storage.get(length_key)

        if length_raw is None:
            return []

        length = int(length_raw)
        args = get_args(field_type)
        item_type = args[0] if args else Any

        result = []
        for i in range(length):
            item_path = f"{path}[{i}]"
            item_value = await self._load_value_from_storage(item_path, item_type)
            result.append(item_value)

        return result

    async def _load_dict_from_storage(self, path: str) -> dict:
        prefix = self._build_full_key(path)
        all_keys = await self._storage.get_all(prefix)

        result = {}
        prefix_with_dot = f"{prefix}."

        for full_key, value in all_keys.items():
            if full_key.startswith(prefix_with_dot):
                dict_key = (
                    full_key[len(prefix_with_dot) :]
                    .split(self._separator)[0]
                    .split("[")[0]
                )

                if dict_key and dict_key not in result:
                    dict_key_path = f"{path}.{dict_key}"
                    dict_key_full = self._build_full_key(dict_key_path)
                    raw_value = await self._storage.get(dict_key_full)

                    if raw_value is not None:
                        result[dict_key] = self._deserialize_scalar(raw_value)

        return result

    async def _load_model_from_storage(self, path: str, model_class: type) -> Any:
        prefix = self._build_full_key(path)
        all_keys = await self._storage.get_all(prefix)

        if not all_keys:
            return None

        # Проверяем, есть ли только один ключ равный префиксу с пустым значением (None)
        if len(all_keys) == 1 and prefix in all_keys:
            value = all_keys[prefix]
            if value == "" or value is None:
                return None

        if issubclass(model_class, KVModel):
            instance = model_class.model_construct()
            object.__setattr__(instance, "_storage", self._storage)
            object.__setattr__(instance, "_key_prefix", prefix)
            object.__setattr__(instance, "_separator", self._separator)

            for field_name in model_class.model_fields:
                if field_name in instance.__dict__:
                    del instance.__dict__[field_name]

            return instance
        else:
            data = {}
            prefix_with_dot = f"{prefix}."

            for full_key in all_keys.keys():
                if full_key.startswith(prefix_with_dot):
                    relative_key = full_key[len(prefix_with_dot) :]
                    field_name = relative_key.split(self._separator)[0].split("[")[0]

                    if (
                        field_name
                        and field_name not in data
                        and field_name in model_class.model_fields
                    ):
                        field_type = model_class.model_fields[field_name].annotation
                        field_path = f"{path}{self._separator}{field_name}"
                        field_value = await self._load_value_from_storage(
                            field_path, field_type
                        )
                        data[field_name] = field_value

            if data:
                return model_class(**data)
            return None

    async def _load_value_from_storage(self, path: str, value_type: type | Any) -> Any:
        origin, real_type = self._extract_real_type(value_type)

        if origin is list:
            return await self._load_list_from_storage(path, value_type)

        elif origin is dict:
            return await self._load_dict_from_storage(path)

        elif isinstance(real_type, type) and issubclass(real_type, BaseModel):
            return await self._load_model_from_storage(path, real_type)

        else:
            full_key = self._build_full_key(path)
            raw_value = await self._storage.get(full_key)

            if raw_value is None:
                return None

            deserialized = self._deserialize_scalar(raw_value)
            return self._cast_scalar_to_type(deserialized, value_type)

    def _cast_scalar_to_type(self, value: Any, target_type: type) -> Any:
        if value is None:
            return None

        origin = get_origin(target_type)
        if origin is Union:
            args = get_args(target_type)
            non_none_types = [arg for arg in args if arg is not type(None)]
            if non_none_types:
                target_type = non_none_types[0]

        if target_type is str:
            return str(value)
        elif target_type is int:
            return int(value)
        elif target_type is float:
            return float(value)
        elif target_type is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)

        return value

    def _reconstruct_value(self, value: Any, field_type: type, path: str) -> Any:
        origin, real_type = self._extract_real_type(field_type)

        if value is None:
            return None

        if origin is list:
            if isinstance(value, list):
                args = get_args(field_type)
                if args:
                    item_type = args[0]
                    result = []
                    for i, item in enumerate(value):
                        item_path = f"{path}[{i}]"
                        result.append(
                            self._reconstruct_value(item, item_type, item_path)
                        )
                    return result
            return value

        if origin is dict:
            return value

        if isinstance(real_type, type) and issubclass(real_type, BaseModel):
            if isinstance(value, dict):
                if issubclass(real_type, KVModel) and self._is_storage_bound() and path:
                    instance = real_type.model_construct()
                    object.__setattr__(instance, "_storage", self._storage)
                    object.__setattr__(
                        instance, "_key_prefix", self._build_full_key(path)
                    )
                    object.__setattr__(instance, "_separator", self._separator)
                    return instance
                else:
                    typed_dict = self._convert_dict_to_typed(value, real_type)
                    return real_type(**typed_dict)
            return value

        return self._cast_scalar_to_type(value, field_type)

    def _convert_dict_to_typed(self, data: dict, model_class: type) -> dict:
        result = {}

        for field_name, value in data.items():
            if field_name in model_class.model_fields:
                field_type = model_class.model_fields[field_name].annotation
                result[field_name] = self._reconstruct_value(value, field_type, "")
            else:
                result[field_name] = value

        return result

    def _serialize_scalar(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _deserialize_scalar(self, value: str) -> Any:
        if value == "":
            return None
        if value == "true":
            return True
        if value == "false":
            return False
        return value

    async def get_flat_representation(self) -> Dict[str, Any]:
        if not self._is_storage_bound():
            return {}
        return await self._storage.get_all(self._key_prefix)
