const config = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL || "http://localhost:8000",
  apiPrefix: import.meta.env.VITE_API_PREFIX || "",
  wsPath: import.meta.env.VITE_WS_PATH || "/ws",
  reconnect: {
    interval: 3000,
    maxAttempts: 5,
  },
};

export default config;
