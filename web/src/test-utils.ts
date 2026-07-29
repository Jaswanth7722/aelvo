import type { UIEvent } from "./types";

/** Build a minimal UIEvent for testing */
export function mockEvent(overrides: Partial<UIEvent> & { type: string }): UIEvent {
  const now = Date.now() / 1000;
  return {
    source: "test",
    specialist: "ARCHITECT",
    action: "test action",
    data: {},
    timestamp: now,
    icon: "●",
    color: "#3b82f6",
    ...overrides,
  };
}

/** Build a sequence of events at fixed offsets from "now" */
export function mockEventSequence(
  baseTime: number,
  specs: Array<{ type: string; specialist?: string; action?: string; data?: Record<string, unknown> }>
): UIEvent[] {
  return specs.map((s, i) =>
    mockEvent({
      type: s.type,
      specialist: s.specialist || "ARCHITECT",
      action: s.action || s.type,
      data: s.data || {},
      timestamp: baseTime - i * 10,
    })
  );
}
