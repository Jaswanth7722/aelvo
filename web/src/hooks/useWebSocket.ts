import { useEffect, useRef, useState, useCallback } from "react";
import type { UIEvent, ConnectionStatus } from "../types";
import { useSettings } from "../context/SettingsContext";

interface UseWebSocketReturn {
  status: ConnectionStatus;
  events: UIEvent[];
  lastEvent: UIEvent | null;
  clearEvents: () => void;
  reconnect: () => void;
}

export function useWebSocket(): UseWebSocketReturn {
  const { config } = useSettings();
  const [status, setStatus] = useState<ConnectionStatus>("disconnected");
  const [events, setEvents] = useState<UIEvent[]>([]);
  const [lastEvent, setLastEvent] = useState<UIEvent | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  const scheduleReconnect = useCallback(() => {
    if (reconnectTimerRef.current || !mountedRef.current) return;
    reconnectTimerRef.current = setTimeout(() => {
      reconnectTimerRef.current = null;
      if (mountedRef.current) connect();
    }, config.reconnectDelay);
  }, [config.reconnectDelay]);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    if (!mountedRef.current) return;

    setStatus("connecting");

    try {
      const ws = new WebSocket(config.url);

      ws.onopen = () => {
        if (!mountedRef.current) {
          ws.close();
          return;
        }
        setStatus("connected");
        if (reconnectTimerRef.current) {
          clearTimeout(reconnectTimerRef.current);
          reconnectTimerRef.current = null;
        }
      };

      ws.onmessage = (event: MessageEvent) => {
        if (!mountedRef.current) return;
        try {
          const uiEvent: UIEvent = JSON.parse(event.data);
          setLastEvent(uiEvent);
          setEvents((prev) => {
            const next = [...prev, uiEvent];
            return next.length > config.maxEvents
              ? next.slice(next.length - config.maxEvents)
              : next;
          });
        } catch (error) {
          // Surface malformed messages so connection issues are visible
          console.warn("Ignoring malformed WebSocket message:", error);
        }
      };

      ws.onclose = () => {
        if (!mountedRef.current) return;
        setStatus("disconnected");
        wsRef.current = null;
        scheduleReconnect();
      };

      ws.onerror = () => {
        if (!mountedRef.current) return;
        setStatus("error");
        ws.close();
      };

      wsRef.current = ws;
    } catch {
      if (mountedRef.current) {
        setStatus("error");
        scheduleReconnect();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [config.url, config.reconnectDelay, config.maxEvents]);

  const clearEvents = useCallback(() => {
    setEvents([]);
    setLastEvent(null);
  }, []);

  const reconnect = useCallback(() => {
    // Close existing connection and reconnect immediately
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    connect();
  }, [connect]);

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect]);

  return { status, events, lastEvent, clearEvents, reconnect };
}
