from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from server.helper.logging import LoggingHelper


class DetailedLoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, request_body_log: bool) -> None:
        super().__init__(app)
        self._body_log = request_body_log
        self._logger = LoggingHelper.getLogger("request logger")
    
    async def dispatch(self, request: Request, call_next):
        # Сырой URL
        raw_url = str(request.url)
        if request.client:
            self._logger.info("=== Incoming Request (%s:%s) ===", request.client.host, request.client.port)
        else:
            self._logger.info("=== Incoming Request ===")
        self._logger.info("method: %s", request.method)
        self._logger.info("url: %s", raw_url)
        self._logger.info("path: %s", request.url.path)
        self._logger.info("query string: %s", request.url.query)
        
        # Все заголовки
        self._logger.info("headers:")
        for name, value in request.headers.items():
            self._logger.info("  %s: %s", name, value)
        
        # Cookies
        if request.cookies:
            self._logger.info("cookies: %s", request.cookies)
        
        # Тело запроса
        if self._body_log:
            body = await request.body()
            if body:
                self._logger.info("body (raw) first 1000 symbols")
                self._logger.info(body.decode('utf-8', errors='ignore')[:1000])
                
                # Восстанавливаем body для следующих обработчиков
                async def receive():
                    return {
                        "type": "http.request",
                        "body": body,
                    }
                request._receive = receive
            
        # Обрабатываем запрос
        response = await call_next(request)
        
        self._logger.info("response status: %s", response.status_code)
        self._logger.info("=== End Request ===")
        
        return response
