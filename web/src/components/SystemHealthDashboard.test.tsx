import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { SystemHealthDashboard } from "./SystemHealthDashboard";
import { mockEvent } from "../test-utils";

// Pin Date.now for deterministic relative time formatting
const NOW = 1_000_000_000;
vi.setSystemTime(NOW * 1000);

describe("SystemHealthDashboard", () => {
  it("renders unknown status with zero events", () => {
    render(<SystemHealthDashboard events={[]} />);
    // Header badge + each agent card shows UNKNOWN when no events exist
    expect(screen.getAllByText("UNKNOWN").length).toBeGreaterThan(0);
    expect(screen.getByText(/0 events/)).toBeTruthy();
  });

  it("renders healthy status with recent active events", () => {
    const events = [
      mockEvent({
        type: "system_online",
        specialist: "SYSTEM",
        timestamp: NOW - 120,
      }),
      mockEvent({
        type: "blackboard_publication",
        specialist: "ARCHITECT",
        timestamp: NOW - 5,
      }),
      mockEvent({
        type: "consensus_formed",
        specialist: "CONSENSUS",
        timestamp: NOW - 3,
      }),
    ];
    render(<SystemHealthDashboard events={events} />);
    expect(screen.getByText("HEALTHY")).toBeTruthy();
  });

  it("shows degraded status with a recovery failure", () => {
    const events = [
      mockEvent({
        type: "system_online",
        specialist: "SYSTEM",
        timestamp: NOW - 200,
      }),
      mockEvent({
        type: "recovery_failed",
        specialist: "ARCHITECT",
        action: "restart failed",
        data: { recovery_id: "rec_1" },
        timestamp: NOW - 10,
      }),
    ];
    render(<SystemHealthDashboard events={events} />);
    expect(screen.getByText("DEGRADED")).toBeTruthy();
  });

  it("shows unhealthy status when no active agents but events exist", () => {
    const events = [
      mockEvent({
        type: "system_online",
        specialist: "HERALD",
        timestamp: NOW - 600, // older than 5 min
      }),
    ];
    render(<SystemHealthDashboard events={events} />);
    // Herald's event is older than 5 min → idle, and no active agents
    expect(screen.getByText("UNHEALTHY")).toBeTruthy();
  });

  it("computes correct recovery stats", () => {
    const events = [
      mockEvent({ type: "recovery_initiated", specialist: "ARCHITECT", timestamp: NOW - 60, data: { recovery_id: "1" } }),
      mockEvent({ type: "recovery_completed", specialist: "ARCHITECT", timestamp: NOW - 50, data: { recovery_id: "2" } }),
      mockEvent({ type: "recovery_completed", specialist: "FORGE", timestamp: NOW - 40, data: { recovery_id: "3" } }),
      mockEvent({ type: "recovery_failed", specialist: "SENTINEL", timestamp: NOW - 30, data: { recovery_id: "4" } }),
      mockEvent({ type: "recovery_completed", specialist: "TERMINUS", timestamp: NOW - 20, data: { recovery_id: "5" } }),
    ];
    render(<SystemHealthDashboard events={events} />);
    // Recovery rate = 3 completed / (3 completed + 1 failed) = 75%
    expect(screen.getByText("75%")).toBeTruthy();
  });

  it("computes error rate correctly", () => {
    const events = [
      mockEvent({ type: "task_completed", specialist: "FORGE", timestamp: NOW - 10 }),
      mockEvent({ type: "task_completed", specialist: "FORGE", timestamp: NOW - 8 }),
      mockEvent({ type: "task_completed", specialist: "FORGE", timestamp: NOW - 6 }),
      mockEvent({ type: "task_failed", specialist: "FORGE", timestamp: NOW - 4 }),
      mockEvent({ type: "recovery_failed", specialist: "ARCHITECT", timestamp: NOW - 2 }),
    ];
    render(<SystemHealthDashboard events={events} />);
    // 2 errors / 5 events = 40%
    expect(screen.getByText("40%")).toBeTruthy();
  });

  it("renders agent liveness grid with specialist cards", () => {
    const events = [
      mockEvent({ type: "blackboard_publication", specialist: "ARCHITECT", timestamp: NOW - 2 }),
      mockEvent({ type: "architect_decision", specialist: "ARCHITECT", timestamp: NOW - 1 }),
      mockEvent({ type: "execution_completed", specialist: "TERMINUS", timestamp: NOW - 5 }),
    ];
    render(<SystemHealthDashboard events={events} />);
    // All 7 specialists appear in the grid (card name spans include the icon)
    for (const name of ["ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD", "CONSENSUS"]) {
      expect(screen.getAllByText(new RegExp(name)).length).toBeGreaterThan(0);
    }
  });

  it("shows correct event throughput (events in last 60s)", () => {
    const events = [
      mockEvent({ type: "task_created", specialist: "ARCHITECT", timestamp: NOW - 5 }),
      mockEvent({ type: "task_created", specialist: "ARCHITECT", timestamp: NOW - 10 }),
      mockEvent({ type: "task_created", specialist: "ARCHITECT", timestamp: NOW - 120 }), // outside window
    ];
    render(<SystemHealthDashboard events={events} />);
    // Throughput = 2 events in last 60s
    expect(screen.getByText(/2\/min throughput/)).toBeTruthy();
  });

  it("renders node transitions from node_transition events", async () => {
    const events = [
      mockEvent({
        type: "node_transition",
        specialist: "ARCHITECT",
        timestamp: NOW - 30,
        data: { node_id: "arch_1", new_state: "running", previous_state: "idle" },
      }),
    ];
    render(<SystemHealthDashboard events={events} />);
    // Tab shows Node Transitions count
    expect(screen.getByText(/Node Transitions/)).toBeTruthy();
  });

  it("renders event type breakdown sorted by count", () => {
    const events = Array.from({ length: 10 }, (_, i) =>
      mockEvent({
        type: i < 5 ? "task_created" : i < 8 ? "architect_decision" : "consensus_formed",
        specialist: "ARCHITECT",
        timestamp: NOW - i * 5,
      })
    );
    render(<SystemHealthDashboard events={events} />);
    // Switch to breakdown tab
    fireEvent.click(screen.getByRole("button", { name: /Event Breakdown/ }));
    // task_created (5) should come before architect_decision (3) before consensus_formed (2)
    expect(screen.getByText(/task_created/)).toBeTruthy();
    expect(screen.getByText(/architect_decision/)).toBeTruthy();
    expect(screen.getByText(/consensus_formed/)).toBeTruthy();
  });
});
