import { useState, useEffect, useRef } from "react";
import reactLogo from "./assets/react.svg";
import viteLogo from "/vite.svg";
import "./App.css";

function App() {
  const [count, setCount] = useState(0);
  const [ws, setWs] = useState(null);
  const [isConnected, setIsConnected] = useState(false);
  const [reconnectAttempts, setReconnectAttempts] = useState(0);
  const [connectionInfo, setConnectionInfo] = useState(null);
  const [isCreatingConnection, setIsCreatingConnection] = useState(false);
  const [connectionError, setConnectionError] = useState(null);

  // используем useRef чтобы избежать замыкания в setTimeout
  const reconnectTimeoutRef = useRef(null);
  const wsRef = useRef(null);
  const maxReconnectAttempts = 5;
  const reconnectInterval = 3000; // 3 секунды

  // Создание нового подключения через REST API
  const createConnection = async () => {
    setIsCreatingConnection(true);
    setConnectionError(null);

    try {
      const response = await fetch("http://localhost:8000/create-connection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          client_info: "React WebSocket Client",
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const connectionData = await response.json();
      console.log("Connection created:", connectionData);

      setConnectionInfo(connectionData);

      // Подключаемся к выделенному воркеру
      connectWebSocket(connectionData.websocket_url);
    } catch (error) {
      console.error("Failed to create connection:", error);
      setConnectionError(error.message);
    } finally {
      setIsCreatingConnection(false);
    }
  };

  const connectWebSocket = (websocketUrl = null) => {
    // Если URL не передан, пытаемся использовать сохраненный
    const url =
      websocketUrl || (connectionInfo && connectionInfo.websocket_url);

    if (!url) {
      console.error("No WebSocket URL available");
      setConnectionError(
        "No WebSocket URL available. Please create a new connection."
      );
      return;
    }

    try {
      const socket = new WebSocket(url);
      wsRef.current = socket;

      socket.onopen = () => {
        console.log("WebSocket connected to:", url);
        setIsConnected(true);
        setWs(socket);
        setReconnectAttempts(0); // сбрасываем счетчик попыток при успешном подключении
        setConnectionError(null);
      };

      socket.onclose = (event) => {
        console.log("WebSocket disconnected", event.code, event.reason);
        setIsConnected(false);
        setWs(null);
        wsRef.current = null;

        // автоматическое переподключение только если это не намеренное закрытие
        if (event.code !== 1000 && reconnectAttempts < maxReconnectAttempts) {
          const timeout = reconnectInterval * Math.pow(1.5, reconnectAttempts); // экспоненциальная задержка
          console.log(
            `Attempting to reconnect in ${timeout}ms... (attempt ${reconnectAttempts + 1}/${maxReconnectAttempts})`
          );

          reconnectTimeoutRef.current = setTimeout(() => {
            setReconnectAttempts((prev) => prev + 1);
            connectWebSocket();
          }, timeout);
        } else if (reconnectAttempts >= maxReconnectAttempts) {
          setConnectionError(
            "Max reconnection attempts reached. Please create a new connection."
          );
        }
      };

      socket.onerror = (error) => {
        console.error("WebSocket error:", error);
        setConnectionError("WebSocket connection error");
      };

      socket.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          if (data.type === "state_update") {
            setCount(data.count);
          } else if (data.count !== undefined) {
            // Поддержка старого формата сообщений
            setCount(data.count);
          } else if (data.type === "error") {
            setConnectionError(data.message);
          }
        } catch (error) {
          console.error("Failed to parse message:", error);
        }
      };
    } catch (error) {
      console.error("Failed to create WebSocket:", error);

      // повторная попытка подключения при ошибке создания сокета
      if (reconnectAttempts < maxReconnectAttempts) {
        const timeout = reconnectInterval * Math.pow(1.5, reconnectAttempts);
        reconnectTimeoutRef.current = setTimeout(() => {
          setReconnectAttempts((prev) => prev + 1);
          connectWebSocket();
        }, timeout);
      } else {
        setConnectionError("Failed to create WebSocket connection");
      }
    }
  };

  useEffect(() => {
    // Автоматически создаем подключение при загрузке
    createConnection();

    // очистка при размонтировании компонента
    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close(1000, "Component unmounting"); // код 1000 = нормальное закрытие
      }
    };
  }, []);

  // функция для ручного переподключения
  const manualReconnect = () => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }
    setReconnectAttempts(0);
    connectWebSocket();
  };

  // создание нового подключения (новый воркер)
  const createNewConnection = () => {
    // Закрываем старое подключение
    if (wsRef.current) {
      wsRef.current.close(1000, "Creating new connection");
    }

    setConnectionInfo(null);
    setCount(0);
    setReconnectAttempts(0);
    setConnectionError(null);

    createConnection();
  };

  const increment = () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ action: "increment" }));
    }
  };

  return (
    <>
      <div>
        <a href="https://vite.dev" target="_blank">
          <img src={viteLogo} className="logo" alt="Vite logo" />
        </a>
        <a href="https://react.dev" target="_blank">
          <img src={reactLogo} className="logo react" alt="React logo" />
        </a>
      </div>
      <h1>Vite + React</h1>
      <div className="card">
        <div style={{ marginBottom: "10px", fontSize: "14px" }}>
          Статус:{" "}
          {isConnected ? (
            <span style={{ color: "green" }}>Подключен</span>
          ) : (
            <span style={{ color: "red" }}>
              Отключен{" "}
              {reconnectAttempts > 0 &&
                `(попытка ${reconnectAttempts}/${maxReconnectAttempts})`}
            </span>
          )}
          {/* Показываем информацию о подключении */}
          {connectionInfo && (
            <div style={{ fontSize: "12px", marginTop: "5px", color: "#666" }}>
              Worker: {connectionInfo.worker_id.substring(0, 8)}... | Port:{" "}
              {connectionInfo.port}
            </div>
          )}
          {/* Показываем ошибки */}
          {connectionError && (
            <div style={{ fontSize: "12px", marginTop: "5px", color: "red" }}>
              Ошибка: {connectionError}
            </div>
          )}
          {/* Кнопки управления */}
          <div style={{ marginTop: "10px" }}>
            {!isConnected && !isCreatingConnection && (
              <>
                {connectionInfo ? (
                  <button
                    onClick={manualReconnect}
                    style={{ marginRight: "10px", fontSize: "12px" }}
                  >
                    Переподключить
                  </button>
                ) : null}

                <button
                  onClick={createNewConnection}
                  style={{ fontSize: "12px" }}
                >
                  {connectionInfo ? "Новое подключение" : "Создать подключение"}
                </button>
              </>
            )}

            {isCreatingConnection && (
              <span style={{ fontSize: "12px", color: "#666" }}>
                Создание подключения...
              </span>
            )}
          </div>
        </div>

        <button onClick={increment} disabled={!isConnected}>
          count is {count}
        </button>

        <p>
          Edit <code>src/App.jsx</code> and save to test HMR
        </p>
      </div>
      <p className="read-the-docs">
        Click on the Vite and React logos to learn more
      </p>
    </>
  );
}

export default App;
