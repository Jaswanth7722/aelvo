import {
  createContext,
  useContext,
  useState,
  useCallback,
  type ReactNode,
} from "react";
import type { WSConfig } from "../types";

const DEFAULT_CONFIG: WSConfig = {
  url: import.meta.env.VITE_WS_URL || "ws://127.0.0.1:8765",
  reconnectDelay: 3000,
  maxEvents: 500,
};

function loadConfig(): WSConfig {
  try {
    const stored = localStorage.getItem("aelvo_ws_config");
    if (stored) {
      const parsed = JSON.parse(stored);
      return {
        url: parsed.url || DEFAULT_CONFIG.url,
        reconnectDelay: parsed.reconnectDelay ?? DEFAULT_CONFIG.reconnectDelay,
        maxEvents: parsed.maxEvents ?? DEFAULT_CONFIG.maxEvents,
      };
    }
  } catch {
    // Corrupted storage, fall through to defaults
  }
  return { ...DEFAULT_CONFIG };
}

interface SettingsContextValue {
  config: WSConfig;
  updateConfig: (patch: Partial<WSConfig>) => void;
  resetConfig: () => void;
}

const SettingsContext = createContext<SettingsContextValue | null>(null);

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<WSConfig>(loadConfig);

  const updateConfig = useCallback((patch: Partial<WSConfig>) => {
    setConfig((prev) => {
      const next = { ...prev, ...patch };
      try {
        localStorage.setItem("aelvo_ws_config", JSON.stringify(next));
      } catch {
        // Storage full or unavailable — ignore
      }
      return next;
    });
  }, []);

  const resetConfig = useCallback(() => {
    setConfig({ ...DEFAULT_CONFIG });
    try {
      localStorage.removeItem("aelvo_ws_config");
    } catch {
      // Ignore
    }
  }, []);

  return (
    <SettingsContext.Provider value={{ config, updateConfig, resetConfig }}>
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings(): SettingsContextValue {
  const ctx = useContext(SettingsContext);
  if (!ctx) {
    throw new Error("useSettings must be used within a SettingsProvider");
  }
  return ctx;
}
