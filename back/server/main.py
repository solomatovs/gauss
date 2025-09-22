from fastapi import FastAPI

# создаём приложение
app = FastAPI(title="My FastAPI App")

# базовый эндпоинт
@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

# пример эндпоинта с параметром
@app.get("/hello/{name}")
def read_item(name: str):
    return {"message": f"Hello, {name}!"}
