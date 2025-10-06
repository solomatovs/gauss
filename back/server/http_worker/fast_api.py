import time
from datetime import datetime
from typing import Optional, List, Any

from pydantic import BaseModel, Field, field_validator
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from server.helper.os import OsHelper
from server.helper.logging import LoggingHelper
from server.http_worker.middleware.detailed_logging import DetailedLoggingMiddleware



class ConnectionRequest(BaseModel):
    """Модель запроса на создание подключения"""
    client_info: str = Field(default="web_client", max_length=100)
    
    class Config:
        json_schema_extra = {
            "example": {
                "client_info": "React WebSocket Client v1.0"
            }
        }

class ConnectionResponse(BaseModel):
    """Модель ответа с данными для подключения"""
    worker_id: str = Field(..., description="Unique worker identifier")
    host: str = Field(..., description="Server host")
    port: int = Field(..., description="Server port", ge=1, le=65535)
    websocket_url: str = Field(..., description="WebSocket connection URL")
    created_at: str = Field(..., description="Creation timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "worker_id": "12345678-1234-1234-1234-123456789012",
                "host": "localhost",
                "port": 8001,
                "websocket_url": "ws://localhost:8001",
                "created_at": "2024-01-01T12:00:00Z"
            }
        }

class WorkerStats(BaseModel):
    """Статистика воркера"""
    worker_id: str = Field(..., description="Worker identifier")
    pid: int = Field(..., description="Process ID")
    port: int = Field(..., description="Server port")
    uptime_seconds: float = Field(..., description="Uptime in seconds")
    start_time: str = Field(..., description="Start time ISO format")
    requests_count: int = Field(..., description="Total requests processed")
    memory_usage_mb: Optional[float] = Field(None, description="Memory usage in MB")

class HealthResponse(BaseModel):
    """Ответ проверки здоровья"""
    status: str = Field(..., description="Health status")
    timestamp: float = Field(..., description="Current timestamp")
    worker_id: str = Field(..., description="Worker ID")
    uptime_seconds: float = Field(..., description="Uptime in seconds")

class WorkerRootResponse(BaseModel):
    """Ответ корневого эндпоинта воркера"""
    message: str = Field(..., description="Welcome message")
    status: str = Field(..., description="Worker status")
    worker_id: str = Field(..., description="Worker identifier")
    pid: int = Field(..., description="Process ID")
    port: int = Field(..., description="Server port")
    uptime_seconds: float = Field(..., description="Uptime in seconds")
    requests_count: int = Field(..., description="Total requests")
    start_time: str = Field(..., description="Start time ISO format")

class ValidationErrorResponse(BaseModel):
    """Ответ на ошибку валидации"""
    detail: str = Field(..., description="Error description")
    errors: List[Any] = Field(..., description="Validation errors")
    worker_id: str = Field(..., description="Worker identifier")

class HttpErrorResponse(BaseModel):
    """Ответ на HTTP ошибку"""
    detail: str = Field(..., description="Error description")
    worker_id: str = Field(..., description="Worker identifier")
    timestamp: str = Field(..., description="Error timestamp")

class ShutdownResponse(BaseModel):
    """Ответ при завершении работы сервера"""
    detail: str = Field(default="Server is shutting down")


class HttpContext(BaseModel):
    """
    Контекст HTTP-воркера.

    Хранит основную информацию о состоянии воркера:
    - уникальный идентификатор
    - порт приложения
    - время старта
    - количество обработанных запросов
    - статус завершения работы
    """

    worker_id: str = Field(
        default_factory=lambda: OsHelper.generate_identifier(),
        description="Уникальный идентификатор воркера"
    )

    start_time: datetime = Field(
        default_factory=datetime.now,
        description="Время запуска воркера."
    )

    requests_count: int = Field(
        default=0,
        ge=0,
        description="Количество успешно обработанных запросов (не может быть отрицательным)."
    )

    # Валидация: worker_id должен быть корректным
    @field_validator("worker_id")
    @classmethod
    def validate_worker_id(cls, v: str) -> str:
        try:
            # проверка что это валидный identifier
            OsHelper.validate_identifier(v)
        except ValueError:
            raise ValueError("worker_id must be a valid identifier string")
        return v

    def uptime_seconds(self) -> int:
        """Возвращает аптайм воркера в секундах."""
        return int((datetime.now() - self.start_time).total_seconds())

    def increment_requests(self, count: int = 1) -> None:
        """Увеличить счетчик обработанных запросов."""
        if count < 0:
            raise ValueError("increment count must be >= 0")
        self.requests_count += count
 
class TrustedHostConfig(BaseModel):
    allowed_hosts: List[Any] = Field(default=[
        "localhost",
        "127.0.0.1",
        "*.localhost",
    ], description="allowed_hosts")
    www_redirect: bool = Field(default=True, description="www redirect")

class CORSConfig(BaseModel):
    allowed_origins: List[Any] = Field(default=[
        "http://localhost:8000",
        "http://localhost:9000",
    ], description="allowed_origins")
    allow_credentials: bool = Field(default=True, description="allow_credentials")
    allow_methods: List[Any] = Field(default=["GET", "POST", "OPTIONS"], description="allow_methods")
    allow_headers: List[Any] = Field(default=["*"], description="allow_headers")
    max_age: int = Field(default=3600, description="max_age")

class CompressionConfig(BaseModel):
    minimum_size: int = Field(default=1000, description="minimum_size")
    compresslevel: int = Field(default=9, description="compresslevel")

class WebsocketConfig(BaseModel):
    host: str = Field(default="localhost", description="websocket host")
    port: int = Field(default=9000, description="websocket port")

class FastApiConfig(BaseModel):
    host: str = Field(default="localhost", description="api host")
    port: int = Field(default=8000, description="api port")
    trusted_host: TrustedHostConfig = Field(default_factory=TrustedHostConfig, description="trusted host")
    cors: CORSConfig = Field(default_factory=CORSConfig, description="cors")
    compression: CompressionConfig = Field(default_factory=CompressionConfig, description="compression")
    websocket: WebsocketConfig = Field(default_factory=WebsocketConfig, description="websocket config")
    request_log: bool = Field(default=False, description="request logging")
    request_body_log: bool = Field(default=False, description="request body logging")

class FastApiContext(BaseModel):
    """Конфиг fast api приложения"""
    http_cx: HttpContext = Field(..., description="Http Context")


def get_fast_api(config: FastApiConfig, cx: HttpContext) -> FastAPI:
    """Создать FastAPI приложение"""
    logger = LoggingHelper.getLogger(f"fastapi {cx.worker_id}")

    app = FastAPI(
        title=f"HTTP Worker {cx.worker_id}",
        description="Scalable HTTP Worker for WebSocket connections",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    # Добавляем middleware
    if config.request_log:
        app.add_middleware(
            DetailedLoggingMiddleware,
            request_body_log=config.request_body_log,
        )
    
    # Security Middleware
    app.add_middleware(
        TrustedHostMiddleware, 
        allowed_hosts=config.trusted_host.allowed_hosts,
        www_redirect=config.trusted_host.www_redirect,
    )
    
    # CORS Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.cors.allowed_origins,
        allow_credentials=config.cors.allow_credentials,
        allow_methods=config.cors.allow_methods,
        allow_headers=config.cors.allow_headers,
        max_age=config.cors.max_age,
    )
    
    # Compression Middleware
    app.add_middleware(
        GZipMiddleware,
        minimum_size=config.compression.minimum_size,
        compresslevel=config.compression.compresslevel,
    )
    
    # Request tracking middleware
    @app.middleware("http")
    async def track_requests(request: Request, call_next):
        cx.requests_count += 1
        start_time = time.time()
        
        response = await call_next(request)
        
        process_time = time.time() - start_time
        logger.debug("request %s %s took %:.2fs", request.method, request.url, process_time)
        response.headers["X-Process-Time"] = str(process_time)
        response.headers["X-Worker-ID"] = cx.worker_id
        
        return response
    
    # Exception handlers
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        # Исправляем ошибку типизации: преобразуем Sequence в List
        error_response = ValidationErrorResponse(
            detail="Validation error",
            errors=list(exc.errors()),  # Преобразуем Sequence в List
            worker_id=cx.worker_id
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=error_response.model_dump()
        )
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        error_response = HttpErrorResponse(
            detail=exc.detail,
            worker_id=cx.worker_id,
            timestamp=datetime.now().isoformat()
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response.model_dump()
        )
    
    # Routes
    @app.get("/", response_model=WorkerRootResponse)
    async def worker_root():
        """Корневой эндпоинт воркера"""
        uptime_seconds = (datetime.now() - cx.start_time).total_seconds()
        return WorkerRootResponse(
            message=f"HTTP Worker {cx.worker_id}",
            status="running",
            worker_id=cx.worker_id,
            pid=OsHelper.getpid(),
            port=config.port,
            uptime_seconds=uptime_seconds,
            requests_count=cx.requests_count,
            start_time=cx.start_time.isoformat()
        )

    @app.post("/create-connection", response_model=ConnectionResponse)
    async def create_connection(request: ConnectionRequest):
        """Создать новое WebSocket подключение"""
        try:
            websocket_worker_id = OsHelper.generate_identifier()
            host = config.websocket.host
            port = config.websocket.port
            
            connection_info = ConnectionResponse(
                worker_id=websocket_worker_id,
                host=host,
                port=port,
                websocket_url=f"ws://{host}:{port}",
                created_at=datetime.now().isoformat()
            )
            
            return connection_info
            
        except Exception as e:
            logger.exception("failed to create web socket")
            raise HTTPException(
                status_code=500,
                detail=f"failed to create web socket: {str(e)}"
            )
    
    @app.get("/stats", response_model=WorkerStats)
    async def get_stats():
        """Получить статистику воркера"""
        uptime = (datetime.now() - cx.start_time).total_seconds()
        memory_usage = OsHelper.get_memory_usage_mb()
        
        return WorkerStats(
            worker_id=cx.worker_id,
            pid=OsHelper.getpid(),
            port=config.port,
            uptime_seconds=uptime,
            start_time=cx.start_time.isoformat(),
            requests_count=cx.requests_count,
            memory_usage_mb=memory_usage
        )
    
    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Проверка здоровья сервера"""
        uptime = (datetime.now() - cx.start_time).total_seconds()

        return HealthResponse(
            status="healthy",
            timestamp=time.time(),
            worker_id=cx.worker_id,
            uptime_seconds=uptime
        )
    
    @app.get("/metrics")
    async def get_metrics():
        """Метрики для мониторинга (Prometheus format)"""
        uptime = (datetime.now() - cx.start_time).total_seconds()
        memory_usage = OsHelper.get_memory_usage_mb() or 0
        
        metrics = f"""# HELP http_worker_uptime_seconds Worker uptime in seconds
# TYPE http_worker_uptime_seconds counter
http_worker_uptime_seconds{{worker_id="{cx.worker_id}"}} {uptime}

# HELP http_worker_requests_total Total number of requests
# TYPE http_worker_requests_total counter
http_worker_requests_total{{worker_id="{cx.worker_id}"}} {cx.requests_count}

# HELP http_worker_memory_usage_bytes Memory usage in bytes
# TYPE http_worker_memory_usage_bytes gauge
http_worker_memory_usage_bytes{{worker_id="{cx.worker_id}"}} {memory_usage * 1024 * 1024}
"""
        return JSONResponse(content=metrics, media_type="text/plain")
    
    return app
