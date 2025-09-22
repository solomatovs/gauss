import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig((configEnv) => {
  const env = loadEnv(configEnv.mode, process.cwd(), "");

  const BASE_URL = env.BASE_URL ?? "";
  const BASE = BASE_URL === "" ? "/" : BASE_URL;
  
  console.log(process.cwd());
  console.log(configEnv);
  console.log("BASE_URL: ", BASE_URL);
  console.log("BASE: ", BASE);
  
  return {
    plugins: [react()],
    base: BASE,
    server: {
      open: false,
    },
  };
});
