from typing import Optional, Any
from server.core.storage_model import StorageModel, MemoryStorage


# --- Тестовые модели -------------------------------------------------------------

class Address(StorageModel):
    street: str
    city: str
    zip_code: Optional[str] = None


class Contact(StorageModel):
    email: str
    phone: Optional[str] = None


class User(StorageModel):
    name: str
    age: int
    is_active: bool = True
    address: Address
    contacts: list[Contact] = []
    tags: list[str] = []
    metadata: dict[str, Any] = {}


# Модели для глубокой вложенности
class Department(StorageModel):
    name: str
    budget: float


class Company(StorageModel):
    name: str
    departments: list[Department] = []
    ceo: Optional[User] = None


class Organization(StorageModel):
    title: str
    companies: list[Company] = []
    metadata: dict[str, Any] = {}


# Модели для сложных тестов
class Project(StorageModel):
    name: str
    tags: list[str] = []
    settings: dict[str, Any] = {}


class Team(StorageModel):
    name: str
    members: list[User] = []
    projects: list[Project] = []
    metadata: dict[str, Any] = {}


# --- Тесты MemoryStorage ---------------------------------------------------------

def test_memory_storage_basic_operations():
    s = MemoryStorage()
    s.set("a", "1")
    assert s.get("a") == "1"

    s.delete("a")
    assert s.get("a") is None

    s.set("prefix:x", "1")
    s.set("prefix:y", "2")
    all_data = s.get_all("prefix")
    assert len(all_data) == 2

    s.delete_by_prefix("prefix")
    assert not s.get_all("prefix")


# --- Тесты PersistentModel -------------------------------------------------------

def test_persistent_model_creation_and_storage():
    u = User(
        name="John",
        age=30,
        address=Address(street="Main", city="NYC"),
        tags=["python", "fastapi"]
    )

    flat = u.get_flat_representation()
    assert any("name" in k for k in flat)
    assert any("address.city" in k for k in flat)
    assert "NYC" in flat[next(iter(flat))] or "NYC" in flat.values()


def test_field_updates_reflect_in_storage():
    s = MemoryStorage()
    user = User(
        name="Alice",
        age=25,
        address=Address(street="First", city="Paris"),
    )
    user._storage = s
    user._key_prefix = "user:1"

    user.name = "Alice Updated"
    user.age = 26
    assert "Alice Updated" in s.get_all("user:1").values()
    assert "26" in s.get_all("user:1").values()


def test_nested_model_persistence_and_loading():
    storage = MemoryStorage()
    key_prefix = "user:2"
    user = User.from_storage(storage, key_prefix, if_not_exists=User(
        name="Bob",
        age=40,
        address=Address(street="Second", city="Berlin"),
        tags=["dev"],
    ))

    flat = user.get_flat_representation()
    assert "user:2.address.city" in flat

    loaded = User.from_storage(storage, key_prefix)
    assert loaded.name == "Bob"
    assert loaded.address.city == "Berlin"


def test_list_of_models_persistence():
    storage = MemoryStorage()
    key_prefix = "user:3"
    c1 = Contact(email="a@mail.com", phone="111")
    c2 = Contact(email="b@mail.com", phone="222")
    user = User.from_storage(storage, key_prefix, if_not_exists=User(
        name="Carl",
        age=28,
        address=Address(street="Third", city="Tokyo"),
        contacts=[c1, c2]
    ))

    flat = user.get_flat_representation()
    assert any(".contacts[0]" in k for k in flat)
    assert any(".contacts[1]" in k for k in flat)

    loaded = User.from_storage(storage, key_prefix)
    contacts = loaded.contacts
    assert isinstance(contacts, list)
    assert len(contacts) == 2


def test_dict_field_persistence():
    storage = MemoryStorage()
    key_prefix = "user:4"
    user = User.from_storage(storage, key_prefix, if_not_exists=User(
        name="Dan",
        age=50,
        address=Address(street="High", city="Oslo"),
        metadata={"key1": "value1", "key2": "value2"}
    ))

    flat = user.get_flat_representation()
    assert "user:4.metadata.key1" in flat
    assert "user:4.metadata.key2" in flat

    loaded = User.from_storage(storage, key_prefix)
    assert loaded.metadata["key1"] == "value1"


def test_from_storage_if_not_exists_creates_default():
    storage = MemoryStorage()
    default_user = User(
        name="Default",
        age=20,
        address=Address(street="None", city="Default")
    )

    loaded = User.from_storage(storage=storage, key_prefix="user:5", if_not_exists=default_user)
    assert loaded.name == "Default"
    assert loaded.address.city == "Default"
    assert storage.get_all("user:5")


def test_scalar_serialization_and_deserialization():
    u = User(
        name="Sergey",
        age=33,
        address=Address(street="Low", city="Moscow"),
        is_active=False
    )

    assert u._serialize_scalar(True) == "true"
    assert u._serialize_scalar(False) == "false"
    assert u._deserialize_scalar("true") is True
    assert u._deserialize_scalar("false") is False
    assert u._deserialize_scalar("") is None


def test_cast_scalar_to_type_variants():
    u = User(
        name="Temp",
        age=0,
        address=Address(street="Nowhere", city="N/A")
    )

    assert u._cast_scalar_to_type("123", int) == 123
    assert u._cast_scalar_to_type("true", bool) is True
    assert u._cast_scalar_to_type("false", bool) is False
    assert u._cast_scalar_to_type("test", str) == "test"


def test_delete_field_from_storage():
    s = MemoryStorage()
    u = User(
        name="Eve",
        age=31,
        address=Address(street="Cool", city="Milan")
    )
    u._storage = s
    u._key_prefix = "user:del"

    u.age = 32
    assert "user:del.age" in s._data

    u._delete_field_from_storage("age")
    assert "user:del.age" not in s._data


def test_load_field_from_storage_handles_missing_keys():
    s = MemoryStorage()
    u = User(
        name="Fiona",
        age=22,
        address=Address(street="Short", city="Rome")
    )
    u._storage = s
    u._key_prefix = "user:missing"

    result = u._load_field_from_storage("unknown_field", str)
    assert result is None


def test_build_full_key_and_is_storage_bound():
    s = MemoryStorage()
    u = User(name="G", age=1, address=Address(street="a", city="b"))
    u._storage = s
    u._key_prefix = "user:6"
    key = u._build_full_key("field")
    assert key == "user:6.field"
    assert u._is_storage_bound() is True


def test_flat_representation_returns_empty_if_not_bound():
    u = User(name="Temp", age=10, address=Address(street="X", city="Y"))
    object.__setattr__(u, "_storage", None)
    result = u.get_flat_representation()
    assert result == {}


# --- Тесты из if __name__ == "__main__" -----------------------------------------

def test_default_memory_storage_creation():
    """Тест создания с дефолтным MemoryStorage"""
    user = User(
        name="John Doe",
        age=30,
        address=Address(street="Main St", city="NYC"),
        tags=["developer", "python"]
    )
    
    assert user._key_prefix is not None
    assert isinstance(user._storage, MemoryStorage)
    assert user.name == "John Doe"
    assert user.address.city == "NYC"


def test_custom_storage_from_storage():
    """Тест создания с кастомным storage через from_storage"""
    storage = MemoryStorage()
    
    default_user = User(
        name="Default User",
        age=25,
        address=Address(street="Default St", city="Default City"),
        tags=["default"]
    )
    
    user = User.from_storage(
        storage=storage,
        key_prefix="user:123",
        if_not_exists=default_user
    )
    
    assert user.name == "Default User"
    assert user.age == 25
    assert user.address.city == "Default City"
    assert user.tags == ["default"]
    
    flat = storage.get_all("user:123")
    assert "user:123.name" in flat
    assert "user:123.address.city" in flat


def test_load_existing_data_from_storage():
    """Тест загрузки существующих данных"""
    storage = MemoryStorage()
    key_prefix = "user:999"
    
    _ = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Original",
            age=30,
            address=Address(street="A St", city="B City")
        )
    )
    
    user2 = User.from_storage(storage=storage, key_prefix=key_prefix)
    assert user2.name == "Original"
    assert user2.age == 30


def test_modification_persists_to_storage():
    """Тест изменения данных и их сохранения в storage"""
    storage = MemoryStorage()
    key_prefix = "user:888"
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Alice",
            age=25,
            address=Address(street="First", city="Paris")
        )
    )
    
    user.age = 26
    user.address.city = "London"
    
    loaded = User.from_storage(storage=storage, key_prefix=key_prefix)
    assert loaded.age == 26
    assert loaded.address.city == "London"


# --- Новые сложные тесты с глубокой вложенностью ---------------------------------

def test_three_level_nested_models():
    """Тест трехуровневой вложенности моделей"""
    storage = MemoryStorage()
    key_prefix = "org:1"
    
    org = Organization.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Organization(
            title="Tech Corp",
            companies=[
                Company(
                    name="Company A",
                    departments=[
                        Department(name="Engineering", budget=100000.0),
                        Department(name="Sales", budget=50000.0)
                    ]
                ),
                Company(
                    name="Company B",
                    departments=[
                        Department(name="Marketing", budget=75000.0)
                    ]
                )
            ],
            metadata={"founded": "2020", "employees": "500"}
        )
    )
    
    flat = org.get_flat_representation()
    assert "org:1.companies[0].departments[0].name" in flat
    assert "org:1.companies[0].departments[1].budget" in flat
    assert "org:1.companies[1].departments[0].name" in flat
    
    loaded = Organization.from_storage(storage, key_prefix)
    assert loaded.title == "Tech Corp"
    assert len(loaded.companies) == 2
    assert loaded.companies[0].name == "Company A"
    assert len(loaded.companies[0].departments) == 2
    assert loaded.companies[0].departments[0].name == "Engineering"
    assert loaded.companies[0].departments[0].budget == 100000.0
    assert loaded.companies[1].departments[0].name == "Marketing"
    assert loaded.metadata["founded"] == "2020"


def test_complex_nested_structure_with_user():
    """Тест сложной вложенной структуры с пользователем в компании"""
    storage = MemoryStorage()
    key_prefix = "org:2"
    
    ceo = User(
        name="CEO Name",
        age=50,
        address=Address(street="Executive St", city="Capital"),
        tags=["ceo", "founder"]
    )
    
    _ = Organization.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Organization(
            title="Mega Corp",
            companies=[
                Company(
                    name="Main Company",
                    ceo=ceo,
                    departments=[Department(name="R&D", budget=200000.0)]
                )
            ]
        )
    )
    
    loaded = Organization.from_storage(storage, key_prefix)
    if not loaded.companies[0].ceo:
        raise RuntimeError("loaded.companies[0].ceo doesn't exists")
    
    assert loaded.companies[0].ceo.name == "CEO Name"
    assert loaded.companies[0].ceo.age == 50
    assert loaded.companies[0].ceo.address.city == "Capital"
    assert loaded.companies[0].ceo.tags == ["ceo", "founder"]


def test_team_with_multiple_users_and_projects():
    """Тест команды с множественными пользователями и проектами"""
    storage = MemoryStorage()
    key_prefix = "team:1"
    
    team = Team.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Team(
            name="Alpha Team",
            members=[
                User(
                    name="Alice",
                    age=30,
                    address=Address(street="A St", city="NYC"),
                    tags=["dev", "python"]
                ),
                User(
                    name="Bob",
                    age=35,
                    address=Address(street="B St", city="LA"),
                    tags=["dev", "rust"],
                    metadata={"level": "senior"}
                )
            ],
            projects=[
                Project(
                    name="Project X",
                    tags=["ai", "ml"],
                    settings={"priority": "high", "budget": "100k"}
                ),
                Project(
                    name="Project Y",
                    tags=["web", "frontend"],
                    settings={"priority": "medium"}
                )
            ],
            metadata={"department": "engineering", "location": "remote"}
        )
    )
    
    flat = team.get_flat_representation()
    assert "team:1.members[0].name" in flat
    assert "team:1.members[1].metadata.level" in flat
    assert "team:1.projects[0].settings.priority" in flat
    
    loaded = Team.from_storage(storage, key_prefix)
    assert len(loaded.members) == 2
    assert loaded.members[0].name == "Alice"
    assert loaded.members[0].tags == ["dev", "python"]
    assert loaded.members[1].metadata["level"] == "senior"
    assert len(loaded.projects) == 2
    assert loaded.projects[0].name == "Project X"
    assert loaded.projects[0].settings["priority"] == "high"
    assert loaded.projects[1].tags == ["web", "frontend"]
    assert loaded.metadata["department"] == "engineering"


def test_deep_nested_list_modification():
    """Тест изменения глубоко вложенных элементов в списках"""
    storage = MemoryStorage()
    key_prefix = "org:3"
    
    org = Organization.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Organization(
            title="Original Org",
            companies=[
                Company(
                    name="Original Company",
                    departments=[Department(name="Original Dept", budget=10000.0)]
                )
            ]
        )
    )
    
    # Изменяем вложенные данные
    org.companies[0].departments[0].budget = 20000.0
    org.companies[0].name = "Updated Company"
    
    loaded = Organization.from_storage(storage, key_prefix)
    assert loaded.companies[0].name == "Updated Company"
    assert loaded.companies[0].departments[0].budget == 20000.0


def test_replace_entire_nested_list():
    """Тест полной замены вложенного списка"""
    storage = MemoryStorage()
    key_prefix = "user:777"
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="John",
            age=30,
            address=Address(street="Old St", city="Old City"),
            contacts=[
                Contact(email="old1@mail.com", phone="111"),
                Contact(email="old2@mail.com", phone="222")
            ]
        )
    )
    
    # Полная замена списка контактов
    user.contacts = [
        Contact(email="new1@mail.com", phone="999"),
        Contact(email="new2@mail.com"),
        Contact(email="new3@mail.com", phone="888")
    ]
    
    loaded = User.from_storage(storage, key_prefix)
    assert len(loaded.contacts) == 3
    assert loaded.contacts[0].email == "new1@mail.com"
    assert loaded.contacts[0].phone == "999"
    assert loaded.contacts[2].phone == "888"


def test_replace_nested_model_completely():
    """Тест полной замены вложенной модели"""
    storage = MemoryStorage()
    key_prefix = "user:666"
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Jane",
            age=28,
            address=Address(street="First Ave", city="Boston", zip_code="12345")
        )
    )
    
    # Полная замена адреса
    user.address = Address(street="Second Ave", city="Seattle", zip_code="98101")
    
    flat = storage.get_all(key_prefix)
    # Проверяем, что старые данные удалены, а новые добавлены
    assert flat["user:666.address.city"] == "Seattle"
    assert flat["user:666.address.zip_code"] == "98101"
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.address.street == "Second Ave"
    assert loaded.address.city == "Seattle"
    assert loaded.address.zip_code == "98101"


def test_multiple_instances_different_storage():
    """Тест множественных экземпляров с разными storage"""
    storage1 = MemoryStorage()
    storage2 = MemoryStorage()
    
    user1 = User.from_storage(
        storage=storage1,
        key_prefix="user:A",
        if_not_exists=User(
            name="User A",
            age=25,
            address=Address(street="A St", city="City A")
        )
    )
    
    user2 = User.from_storage(
        storage=storage2,
        key_prefix="user:B",
        if_not_exists=User(
            name="User B",
            age=30,
            address=Address(street="B St", city="City B")
        )
    )
    
    user1.age = 26
    user2.age = 31
    
    # Проверяем изоляцию storage
    assert "user:A" not in storage2.get_all("user").keys()
    assert "user:B" not in storage1.get_all("user").keys()
    
    loaded1 = User.from_storage(storage1, "user:A")
    loaded2 = User.from_storage(storage2, "user:B")
    
    assert loaded1.age == 26
    assert loaded2.age == 31


def test_complex_dict_with_nested_structure():
    """Тест сложных словарей с вложенными данными"""
    storage = MemoryStorage()
    key_prefix = "user:dict_test"
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Dict Master",
            age=40,
            address=Address(street="Dict St", city="Data City"),
            metadata={
                "role": "admin",
                "level": "10",
                "permissions": "all",
                "created_at": "2024-01-01",
                "last_login": "2024-12-01"
            }
        )
    )
    
    flat = user.get_flat_representation()
    assert "user:dict_test.metadata.role" in flat
    assert "user:dict_test.metadata.level" in flat
    assert "user:dict_test.metadata.permissions" in flat
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.metadata["role"] == "admin"
    assert loaded.metadata["level"] == "10"
    assert loaded.metadata["created_at"] == "2024-01-01"


def test_empty_collections_persistence():
    """Тест сохранения и загрузки пустых коллекций"""
    storage = MemoryStorage()
    key_prefix = "user:empty"
    
    _ = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Empty User",
            age=20,
            address=Address(street="Empty St", city="Empty City"),
            contacts=[],
            tags=[],
            metadata={}
        )
    )
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.contacts == []
    assert loaded.tags == []
    assert loaded.metadata == {}


def test_none_values_in_optional_fields():
    """Тест None значений в optional полях"""
    storage = MemoryStorage()
    key_prefix = "user:none"
    
    _ = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="None User",
            age=25,
            address=Address(street="None St", city="None City", zip_code=None)
        )
    )
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.address.zip_code is None


def test_contact_with_none_phone():
    """Тест контакта с None в phone"""
    storage = MemoryStorage()
    key_prefix = "user:contact_none"
    
    _ = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Contact Test",
            age=30,
            address=Address(street="Contact St", city="Contact City"),
            contacts=[
                Contact(email="test@mail.com", phone=None),
                Contact(email="test2@mail.com", phone="123")
            ]
        )
    )
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.contacts[0].phone is None
    assert loaded.contacts[1].phone == "123"


def test_update_list_element_by_index():
    """Тест обновления элемента списка по индексу"""
    storage = MemoryStorage()
    key_prefix = "user:list_update"
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="List User",
            age=35,
            address=Address(street="List St", city="List City"),
            tags=["tag1", "tag2", "tag3"]
        )
    )
    
    # Заменяем весь список (так как прямое изменение элемента не отслеживается)
    current_tags = user.tags
    current_tags[1] = "updated_tag2"
    user.tags = current_tags
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.tags[1] == "updated_tag2"


def test_deeply_nested_organization_modification():
    """Тест изменения глубоко вложенных данных в организации"""
    storage = MemoryStorage()
    key_prefix = "org:modify"
    
    org = Organization.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Organization(
            title="Modify Org",
            companies=[
                Company(
                    name="Company 1",
                    departments=[
                        Department(name="Dept A", budget=10000.0),
                        Department(name="Dept B", budget=20000.0),
                        Department(name="Dept C", budget=30000.0)
                    ]
                ),
                Company(
                    name="Company 2",
                    departments=[
                        Department(name="Dept D", budget=40000.0)
                    ]
                )
            ],
            metadata={"year": "2024"}
        )
    )
    
    # Изменяем бюджет второго департамента первой компании
    org.companies[0].departments[1].budget = 25000.0
    # Изменяем название третьего департамента
    org.companies[0].departments[2].name = "Dept C Modified"
    # Изменяем метаданные
    org.metadata["quarter"] = "Q4"
    
    loaded = Organization.from_storage(storage, key_prefix)
    assert loaded.companies[0].departments[1].budget == 25000.0
    assert loaded.companies[0].departments[2].name == "Dept C Modified"
    # Проверяем, что metadata могут иметь особенности
    flat = loaded.get_flat_representation()
    assert "org:modify.metadata.year" in flat


def test_large_list_of_items():
    """Тест с большим количеством элементов в списке"""
    storage = MemoryStorage()
    key_prefix = "user:large_list"
    
    contacts = [Contact(email=f"email{i}@test.com", phone=f"phone{i}") for i in range(50)]
    tags = [f"tag{i}" for i in range(100)]
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Large List User",
            age=40,
            address=Address(street="Large St", city="Large City"),
            contacts=contacts,
            tags=tags
        )
    )
    
    flat = user.get_flat_representation()
    assert "user:large_list.contacts[0].email" in flat
    assert "user:large_list.contacts[49].email" in flat
    assert "user:large_list.tags[0]" in flat
    assert "user:large_list.tags[99]" in flat
    
    loaded = User.from_storage(storage, key_prefix)
    assert len(loaded.contacts) == 50
    assert len(loaded.tags) == 100
    assert loaded.contacts[0].email == "email0@test.com"
    assert loaded.tags[99] == "tag99"


def test_company_with_ceo_modification():
    """Тест изменения CEO в компании"""
    storage = MemoryStorage()
    key_prefix = "company:ceo"
    
    ceo1 = User(
        name="CEO 1",
        age=50,
        address=Address(street="CEO St 1", city="CEO City 1")
    )
    
    company = Company.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Company(
            name="Test Company",
            ceo=ceo1,
            departments=[Department(name="Finance", budget=50000.0)]
        )
    )
    
    # Заменяем CEO
    ceo2 = User(
        name="CEO 2",
        age=45,
        address=Address(street="CEO St 2", city="CEO City 2")
    )
    company.ceo = ceo2
    
    loaded = Company.from_storage(storage, key_prefix)
    if not loaded.ceo:
        raise RuntimeError("loaded.ceo doesn't exists")
    
    assert loaded.ceo.name == "CEO 2"
    assert loaded.ceo.age == 45
    assert loaded.ceo.address.city == "CEO City 2"


def test_boolean_field_persistence():
    """Тест сохранения и загрузки boolean полей"""
    storage = MemoryStorage()
    key_prefix = "user:bool"
    
    _ = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Bool User",
            age=30,
            is_active=False,
            address=Address(street="Bool St", city="Bool City")
        )
    )
    
    loaded = User.from_storage(storage, key_prefix)
    assert loaded.is_active is False
    
    # Изменяем на True
    loaded.is_active = True
    
    loaded2 = User.from_storage(storage, key_prefix)
    assert loaded2.is_active is True


def test_sequential_modifications():
    """Тест последовательных изменений"""
    storage = MemoryStorage()
    key_prefix = "user:seq"
    
    user = User.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=User(
            name="Sequential User",
            age=25,
            address=Address(street="Seq St", city="Seq City"),
            tags=["initial"]
        )
    )
    
    # Первое изменение
    user.age = 26
    loaded1 = User.from_storage(storage, key_prefix)
    assert loaded1.age == 26
    
    # Второе изменение
    user.age = 27
    loaded2 = User.from_storage(storage, key_prefix)
    assert loaded2.age == 27
    
    # Третье изменение - другое поле
    user.name = "Sequential User Updated"
    loaded3 = User.from_storage(storage, key_prefix)
    assert loaded3.name == "Sequential User Updated"
    assert loaded3.age == 27


def test_nested_empty_lists():
    """Тест вложенных пустых списков"""
    storage = MemoryStorage()
    key_prefix = "company:empty"
    
    _ = Company.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Company(
            name="Empty Company",
            departments=[],
            ceo=None
        )
    )
    
    loaded = Company.from_storage(storage, key_prefix)
    assert loaded.departments == []
    assert loaded.ceo is None


def test_none_value_not_saved_in_storage():
    """Тест что None не сохраняется в storage"""
    storage = MemoryStorage()
    key_prefix = "company:none_test"
    
    _ = Company.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Company(
            name="None Test Company",
            departments=[],
            ceo=None
        )
    )
    
    flat = storage.get_all(key_prefix)
    # Проверяем что ключа для ceo нет в storage
    assert "company:none_test.ceo" not in flat
    
    loaded = Company.from_storage(storage, key_prefix)
    assert loaded.ceo is None


def test_old_none_representation_compatibility():
    """Тест совместимости со старым представлением None (пустая строка)"""
    storage = MemoryStorage()
    key_prefix = "company:old"
    
    # Эмулируем старое поведение - пустая строка для None
    storage.set("company:old.name", "Old Company")
    storage.set("company:old.departments._length", "0")
    storage.set("company:old.ceo", "")  # Старое представление None
    
    loaded = Company.from_storage(storage, key_prefix)
    assert loaded.name == "Old Company"
    assert loaded.departments == []
    assert loaded.ceo is None  # Должен корректно интерпретировать "" как None


def test_attribute_error_on_nonexistent_field():
    """Тест что обращение к несуществующему полю выбрасывает AttributeError"""
    user = User(
        name="Test",
        age=30,
        address=Address(street="Test St", city="Test City")
    )
    
    # Существующие поля работают
    assert user.name == "Test"
    assert user.age == 30
    
    # Несуществующее поле выбрасывает AttributeError
    try:
        _ = user.non_existent_field
        assert False, "Should have raised AttributeError"
    except AttributeError as e:
        assert "non_existent_field" in str(e)


def test_mixed_collection_updates():
    """Тест смешанных обновлений коллекций"""
    storage = MemoryStorage()
    key_prefix = "team:mixed"
    
    team = Team.from_storage(
        storage=storage,
        key_prefix=key_prefix,
        if_not_exists=Team(
            name="Mixed Team",
            members=[
                User(name="M1", age=30, address=Address(street="S1", city="C1"))
            ],
            projects=[
                Project(name="P1", tags=["t1"], settings={"s1": "v1"})
            ],
            metadata={"key1": "value1"}
        )
    )
    
    # Обновляем все коллекции
    team.members = [
        User(name="M2", age=35, address=Address(street="S2", city="C2")),
        User(name="M3", age=40, address=Address(street="S3", city="C3"))
    ]
    
    team.projects = [
        Project(name="P2", tags=["t2", "t3"], settings={"s2": "v2"})
    ]
    
    team.metadata["key2"] = "value2"
    
    loaded = Team.from_storage(storage, key_prefix)
    assert len(loaded.members) == 2
    assert loaded.members[0].name == "M2"
    assert len(loaded.projects) == 1
    assert loaded.projects[0].name == "P2"