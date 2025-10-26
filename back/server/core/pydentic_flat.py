from typing import Dict, Any, get_origin, get_args, Union, Optional
from pydantic import BaseModel, PrivateAttr, ConfigDict
from enum import Enum
import uuid


class ValueType(Enum):
    SCALAR = "scalar"
    PYDANTIC_MODEL = "model"
    LIST = "list"
    DICT = "dict"


class BaseStorage:
    def get(self, key: str) -> Any:
        raise NotImplementedError

    def set(self, key: str, value: Any) -> None:
        raise NotImplementedError
    
    def delete(self, key: str) -> None:
        raise NotImplementedError
    
    def get_all(self, prefix: str) -> Dict[str, Any]:
        raise NotImplementedError
    
    def delete_by_prefix(self, prefix: str) -> None:
        raise NotImplementedError


class MemoryStorage(BaseStorage):
    def __init__(self):
        self._data: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        return self._data.get(key)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value
    
    def delete(self, key: str) -> None:
        if key in self._data:
            del self._data[key]
    
    def get_all(self, prefix: str) -> Dict[str, Any]:
        return {k: v for k, v in self._data.items() if k.startswith(prefix)}
    
    def delete_by_prefix(self, prefix: str) -> None:
        keys_to_delete = [k for k in self._data.keys() if k.startswith(prefix)]
        for key in keys_to_delete:
            del self._data[key]
    
    def __repr__(self):
        return f"MemoryStorage({self._data})"


class PersistentModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
        
    _storage: BaseStorage = PrivateAttr(default_factory=MemoryStorage)
    _key_prefix: str = PrivateAttr()
    _separator: str = PrivateAttr(default=".")
    
    def __init__(self, **data):
        super().__init__(**data)
        
        if '_key_prefix' not in self.__dict__ or self._key_prefix is None:
            object.__setattr__(self, '_key_prefix', f"{self.__class__.__name__}:{uuid.uuid4()}")
        
        if '_storage' not in self.__dict__:
            object.__setattr__(self, '_storage', MemoryStorage())
        
        for field_name in self.__class__.model_fields:
            if field_name in self.__dict__:
                value = self.__dict__[field_name]
                self._save_field_to_storage(field_name, value)
    
    @classmethod
    def from_storage(
        cls, 
        storage: BaseStorage, 
        key_prefix: str, 
        if_not_exists: 'PersistentModel' = None,
        separator: str = "."
    ):
        existing_data = storage.get_all(key_prefix)
        
        if not existing_data and if_not_exists is not None:
            instance = if_not_exists
            instance._storage = storage
            instance._key_prefix = key_prefix
            instance._separator = separator
            
            for field_name in instance.__class__.model_fields:
                if field_name in instance.__dict__:
                    value = instance.__dict__[field_name]
                    instance._save_field_to_storage(field_name, value)
                    del instance.__dict__[field_name]
            
            return instance
        
        instance = cls.model_construct()
        object.__setattr__(instance, '_storage', storage)
        object.__setattr__(instance, '_key_prefix', key_prefix)
        object.__setattr__(instance, '_separator', separator)
        
        return instance
    
    def _is_storage_bound(self) -> bool:
        return self._storage is not None and self._key_prefix is not None
    
    def _build_full_key(self, path: str) -> str:
        if not path:
            return self._key_prefix
        return f"{self._key_prefix}{self._separator}{path}"
    
    def _get_field_type(self, field_name: str) -> type:
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
    
    def _extract_real_type(self, field_type: type) -> tuple[type, type]:
        origin = get_origin(field_type)
        args = get_args(field_type)
        
        if origin is Union:
            non_none_types = [arg for arg in args if arg is not type(None)]
            if non_none_types:
                field_type = non_none_types[0]
                origin = get_origin(field_type)
                args = get_args(field_type)
        
        real_type = args[0] if args else field_type
        
        return origin, real_type
    
    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith('_'):
            object.__setattr__(self, name, value)
            return
        
        if name not in self.__class__.model_fields:
            super().__setattr__(name, value)
            return
        
        if self._is_storage_bound():
            self._delete_field_from_storage(field_name=name)
            self._save_field_to_storage(name, value)
            
            if name in self.__dict__:
                del self.__dict__[name]
        else:
            super().__setattr__(name, value)
    
    def __getattribute__(self, name: str) -> Any:
        if name.startswith('_') or name in ('model_fields', 'model_config', '__dict__', '__class__'):
            return object.__getattribute__(self, name)
        
        try:
            if name not in object.__getattribute__(self, '__class__').model_fields:
                return object.__getattribute__(self, name)
        except AttributeError:
            return object.__getattribute__(self, name)
        
        try:
            dict_obj = object.__getattribute__(self, '__dict__')
            if name in dict_obj:
                return dict_obj[name]
            
            storage = object.__getattribute__(self, '_storage')
            key_prefix = object.__getattribute__(self, '_key_prefix')
            
            if storage is None or key_prefix is None:
                return object.__getattribute__(self, name)
            
            field_type = object.__getattribute__(self, '__class__').model_fields[name].annotation
            value = self._load_field_from_storage(name, field_type)
            
            return value
        except AttributeError:
            return object.__getattribute__(self, name)
    
    def _save_field_to_storage(self, field_name: str, value: Any, path: str = "") -> None:
        if not self._is_storage_bound():
            return
        
        current_path = f"{path}{self._separator}{field_name}" if path else field_name
        value_type = self._get_value_type(value)
        
        if value_type == ValueType.SCALAR:
            full_key = self._build_full_key(current_path)
            serialized = self._serialize_scalar(value)
            self._storage.set(full_key, serialized)
        
        elif value_type == ValueType.PYDANTIC_MODEL:
            if isinstance(value, PersistentModel):
                nested_path = self._build_full_key(current_path)
                
                nested_data = {}
                for nested_field_name in value.__class__.model_fields:
                    nested_data[nested_field_name] = getattr(value, nested_field_name)
                
                value._storage = self._storage
                value._key_prefix = nested_path
                value._separator = self._separator
                
                for nested_field_name, nested_value in nested_data.items():
                    value._save_field_to_storage(nested_field_name, nested_value)
                    
                    if nested_field_name in value.__dict__:
                        del value.__dict__[nested_field_name]
            else:
                data = value.model_dump()
                for nested_field_name, nested_value in data.items():
                    nested_path = f"{current_path}{self._separator}{nested_field_name}"
                    self._save_field_to_storage(nested_field_name, nested_value, path=current_path)
        
        elif value_type == ValueType.LIST:
            full_key = self._build_full_key(f"{current_path}._length")
            self._storage.set(full_key, str(len(value)))
            
            for i, item in enumerate(value):
                item_path = f"{current_path}[{i}]"
                item_type = self._get_value_type(item)
                
                if item_type == ValueType.SCALAR:
                    full_key = self._build_full_key(item_path)
                    self._storage.set(full_key, self._serialize_scalar(item))
                
                elif item_type == ValueType.PYDANTIC_MODEL:
                    if isinstance(item, PersistentModel):
                        nested_path = self._build_full_key(item_path)
                        
                        nested_data = {}
                        for nested_field_name in item.__class__.model_fields:
                            nested_data[nested_field_name] = getattr(item, nested_field_name)
                        
                        item._storage = self._storage
                        item._key_prefix = nested_path
                        item._separator = self._separator
                        
                        for nested_field_name, nested_value in nested_data.items():
                            item._save_field_to_storage(nested_field_name, nested_value)
                            
                            if nested_field_name in item.__dict__:
                                del item.__dict__[nested_field_name]
                    else:
                        data = item.model_dump()
                        for nested_field_name, nested_value in data.items():
                            self._save_field_to_storage(nested_field_name, nested_value, path=item_path)
                
                elif item_type == ValueType.DICT:
                    for dict_key, dict_value in item.items():
                        dict_path = f"{item_path}.{dict_key}"
                        full_key = self._build_full_key(dict_path)
                        self._storage.set(full_key, self._serialize_scalar(dict_value))
        
        elif value_type == ValueType.DICT:
            for dict_key, dict_value in value.items():
                dict_path = f"{current_path}.{dict_key}"
                full_key = self._build_full_key(dict_path)
                self._storage.set(full_key, self._serialize_scalar(dict_value))
    
    def _delete_field_from_storage(self, field_name: str, path: str = "") -> None:
        if not self._is_storage_bound():
            return
        
        current_path = f"{path}{self._separator}{field_name}" if path else field_name
        prefix = self._build_full_key(current_path)
        self._storage.delete_by_prefix(prefix)
    
    def _load_field_from_storage(self, field_name: str, field_type: type, path: str = "") -> Any:
        if not self._is_storage_bound():
            if field_name in self.__dict__:
                return self.__dict__[field_name]
            return None
        
        current_path = f"{path}{self._separator}{field_name}" if path else field_name
        origin, real_type = self._extract_real_type(field_type)
        
        if origin is list:
            return self._load_list_from_storage(current_path, field_type)
        
        elif origin is dict:
            return self._load_dict_from_storage(current_path)
        
        elif isinstance(real_type, type) and issubclass(real_type, BaseModel):
            return self._load_model_from_storage(current_path, real_type)
        
        else:
            full_key = self._build_full_key(current_path)
            raw_value = self._storage.get(full_key)
            
            if raw_value is None:
                return None
            
            deserialized = self._deserialize_scalar(raw_value)
            return self._cast_scalar_to_type(deserialized, field_type)
    
    def _load_list_from_storage(self, path: str, field_type: type) -> list:
        length_key = self._build_full_key(f"{path}._length")
        length_raw = self._storage.get(length_key)
        
        if length_raw is None:
            return []
        
        length = int(length_raw)
        args = get_args(field_type)
        item_type = args[0] if args else Any
        
        result = []
        for i in range(length):
            item_path = f"{path}[{i}]"
            item_value = self._load_value_from_storage(item_path, item_type)
            result.append(item_value)
        
        return result
    
    def _load_dict_from_storage(self, path: str) -> dict:
        prefix = self._build_full_key(path)
        all_keys = self._storage.get_all(prefix)
        
        result = {}
        prefix_with_dot = f"{prefix}."
        
        for full_key, value in all_keys.items():
            if full_key.startswith(prefix_with_dot):
                dict_key = full_key[len(prefix_with_dot):].split(self._separator)[0].split('[')[0]
                
                if dict_key and dict_key not in result:
                    dict_key_path = f"{path}.{dict_key}"
                    dict_key_full = self._build_full_key(dict_key_path)
                    raw_value = self._storage.get(dict_key_full)
                    
                    if raw_value is not None:
                        result[dict_key] = self._deserialize_scalar(raw_value)
        
        return result
    
    def _load_model_from_storage(self, path: str, model_class: type) -> Any:
        prefix = self._build_full_key(path)
        all_keys = self._storage.get_all(prefix)
        
        if not all_keys:
            return None
        
        if issubclass(model_class, PersistentModel):
            instance = model_class.model_construct()
            object.__setattr__(instance, '_storage', self._storage)
            object.__setattr__(instance, '_key_prefix', prefix)
            object.__setattr__(instance, '_separator', self._separator)
            return instance
        else:
            data = {}
            prefix_with_dot = f"{prefix}."
            
            for full_key in all_keys.keys():
                if full_key.startswith(prefix_with_dot):
                    relative_key = full_key[len(prefix_with_dot):]
                    field_name = relative_key.split(self._separator)[0].split('[')[0]
                    
                    if field_name and field_name not in data and field_name in model_class.model_fields:
                        field_type = model_class.model_fields[field_name].annotation
                        field_path = f"{path}{self._separator}{field_name}"
                        field_value = self._load_value_from_storage(field_path, field_type)
                        data[field_name] = field_value
            
            if data:
                return model_class(**data)
            return None
    
    def _load_value_from_storage(self, path: str, value_type: type) -> Any:
        origin, real_type = self._extract_real_type(value_type)
        
        if origin is list:
            return self._load_list_from_storage(path, value_type)
        
        elif origin is dict:
            return self._load_dict_from_storage(path)
        
        elif isinstance(real_type, type) and issubclass(real_type, BaseModel):
            return self._load_model_from_storage(path, real_type)
        
        else:
            full_key = self._build_full_key(path)
            raw_value = self._storage.get(full_key)
            
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
                return value.lower() in ('true', '1', 'yes')
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
                        result.append(self._reconstruct_value(item, item_type, item_path))
                    return result
            return value
        
        if origin is dict:
            return value
        
        if isinstance(real_type, type) and issubclass(real_type, BaseModel):
            if isinstance(value, dict):
                if issubclass(real_type, PersistentModel) and self._is_storage_bound() and path:
                    instance = real_type.model_construct()
                    object.__setattr__(instance, '_storage', self._storage)
                    object.__setattr__(instance, '_key_prefix', self._build_full_key(path))
                    object.__setattr__(instance, '_separator', self._separator)
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
    
    def get_flat_representation(self) -> Dict[str, Any]:
        if not self._is_storage_bound():
            return {}
        return self._storage.get_all(self._key_prefix)


if __name__ == "__main__":
    class Address(PersistentModel):
        street: str
        city: str
        zip_code: Optional[str] = None

    class Contact(PersistentModel):
        email: str
        phone: Optional[str] = None

    class User(PersistentModel):
        name: str
        age: int
        is_active: bool = True
        address: Address
        contacts: list[Contact] = []
        tags: list[str] = []
        metadata: dict[str, Any] = {}

    print("=== Тест 1: Создание с дефолтным MemoryStorage ===")
    user1 = User(
        name="John Doe",
        age=30,
        address=Address(street="Main St", city="NYC"),
        tags=["developer", "python"]
    )
    print(f"Key prefix: {user1._key_prefix}")
    print(f"Name: {user1.name}")
    print(f"City: {user1.address.city}")
    print(f"Storage: {user1._storage}")

    print("\n=== Тест 2: Создание с кастомным storage через from_storage ===")
    storage = MemoryStorage()
    
    default_user = User(
        name="Default User",
        age=25,
        address=Address(street="Default St", city="Default City"),
        tags=["default"]
    )
    
    user2 = User.from_storage(
        storage=storage,
        key_prefix="user:123",
        if_not_exists=default_user
    )
    
    print(f"Name from storage: {user2.name}")
    print(f"Age from storage: {user2.age}")
    print(f"City from storage: {user2.address.city}")
    print(f"Tags from storage: {user2.tags}")
    
    print("\n=== Содержимое storage ===")
    for key, value in sorted(storage._data.items()):
        print(f"{key} = {value}")
    
    print("\n=== Тест 3: Загрузка существующих данных ===")
    user3 = User.from_storage(storage=storage, key_prefix="user:123")
    print(f"Name: {user3.name}")
    print(f"Age: {user3.age}")
    
    print("\n=== Тест 4: Изменение данных ===")
    user3.age = 26
    user3.address.city = "New City"
    
    print(f"Updated age: {user3.age}")
    print(f"Updated city: {user3.address.city}")
    
    print("\n=== Проверка изменений в storage ===")
    user4 = User.from_storage(storage=storage, key_prefix="user:123")
    print(f"Age from new instance: {user4.age}")
    print(f"City from new instance: {user4.address.city}")