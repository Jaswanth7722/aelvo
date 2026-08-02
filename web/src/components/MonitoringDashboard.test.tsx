import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MonitoringDashboard } from "./MonitoringDashboard";
import { mockEvent } from "../test-utils";

const NOW = 1_000_000_000;
vi.setSystemTime(NOW * 1000);

describe("MonitoringDashboard", () => {
  it("renders empty state with no events", () => {
    render(<MonitoringDashboard events={[]} />);
    expect(screen.getByText("Monitoring Dashboard")).toBeTruthy();
    expect(screen.getByText("0 total events")).toBeTruthy();
  });

  describe("event rate buckets", () => {
    it("creates 5 buckets with correct labels", () => {
      const events = [
        mockEvent({ type: "task_created", timestamp: NOW - 30 }),
      ];
      render(<MonitoringDashboard events={events} />);
      expect(screen.getByText("-1m")).toBeTruthy();
      expect(screen.getByText("-2m")).toBeTruthy();
      expect(screen.getByText("-3m")).toBeTruthy();
      expect(screen.getByText("-4m")).toBeTruthy();
      expect(screen.getByText("-5m")).toBeTruthy();
    });

    it("places events in correct time buckets", () => {
      const events = [
        mockEvent({ type: "task_created", timestamp: NOW - 10 }),   // -1m bucket
        mockEvent({ type: "task_created", timestamp: NOW - 80 }),   // -2m bucket
        mockEvent({ type: "task_created", timestamp: NOW - 150 }),  // -3m bucket
        mockEvent({ type: "task_created", timestamp: NOW - 200 }),  // -4m bucket
        mockEvent({ type: "task_created", timestamp: NOW - 280 }),  // -5m bucket
      ];
      render(<MonitoringDashboard events={events} />);
      // Event rate bucketed across 5 min → total 5 events
      const rateText = screen.getByText(/in last 5 min/);
      expect(rateText.textContent).toContain("5");
    });
  });

  describe("alert derivation", () => {
    it("creates error alert from recovery_failed", () => {
      const events = [
        mockEvent({
          type: "recovery_failed",
          specialist: "ARCHITECT",
          action: "consensus recovery failed",
          data: { recovery_id: "r1" },
          timestamp: NOW - 10,
        }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Alert Feed/ }));
      expect(screen.getByText(/Recovery operation failed/)).toBeTruthy();
      expect(screen.getAllByText(/ERROR/).length).toBeGreaterThan(0);
    });

    it("creates warning alert from verification_failed", () => {
      const events = [
        mockEvent({
          type: "verification_failed",
          specialist: "SENTINEL",
          data: { check_id: "v1" },
          timestamp: NOW - 10,
        }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Alert Feed/ }));
      expect(screen.getByText(/Verification check failed/)).toBeTruthy();
      expect(screen.getAllByText(/WARNING/).length).toBeGreaterThan(0);
    });

    it("creates error alert from task_failed", () => {
      const events = [
        mockEvent({
          type: "task_failed",
          specialist: "FORGE",
          data: { task_id: "t1" },
          timestamp: NOW - 10,
        }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Alert Feed/ }));
      expect(screen.getByText(/Task execution failed/)).toBeTruthy();
      expect(screen.getAllByText(/ERROR/).length).toBeGreaterThan(0);
    });

    it("creates warning alert from execution_completed with non-zero exit code", () => {
      const events = [
        mockEvent({
          type: "execution_completed",
          specialist: "TERMINUS",
          data: { task_id: "t1", exit_code: 1 },
          timestamp: NOW - 10,
        }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Alert Feed/ }));
      expect(screen.getByText(/Execution completed with errors/)).toBeTruthy();
      expect(screen.getAllByText(/WARNING/).length).toBeGreaterThan(0);
    });

    it("counts alert severities correctly", () => {
      const events = [
        mockEvent({ type: "recovery_failed", data: {}, timestamp: NOW - 30 }),
        mockEvent({ type: "verification_failed", data: {}, timestamp: NOW - 25 }),
        mockEvent({ type: "task_failed", data: {}, timestamp: NOW - 20 }),
        mockEvent({ type: "execution_completed", data: { exit_code: 1 }, timestamp: NOW - 15 }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Alert Feed/ }));
      // 2 errors (recovery_failed + task_failed) + 2 warnings (verification_failed + execution_completed w/ exit_code != 0)
      expect(screen.getByText(/ERROR 2/)).toBeTruthy();
      expect(screen.getByText(/WARNING 2/)).toBeTruthy();
    });
  });

  describe("metric series", () => {
    it("computes 8 metric series from events", () => {
      const events = [
        mockEvent({ type: "recovery_completed", timestamp: NOW - 10 }),
        mockEvent({ type: "verification_passed", timestamp: NOW - 8 }),
        mockEvent({ type: "architect_decision", timestamp: NOW - 6 }),
        mockEvent({ type: "consensus_formed", timestamp: NOW - 4 }),
        mockEvent({ type: "blackboard_publication", timestamp: NOW - 2 }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Metric Series/ }));
      expect(screen.getByText("recovery.attempt")).toBeTruthy();
      expect(screen.getByText("verification.total")).toBeTruthy();
      expect(screen.getByText("architect.decisions")).toBeTruthy();
      expect(screen.getByText("consensus.sessions")).toBeTruthy();
      expect(screen.getByText("blackboard.publications")).toBeTruthy();
      expect(screen.getByText("execution.completed")).toBeTruthy();
      expect(screen.getByText("challenges.raised")).toBeTruthy();
      expect(screen.getByText("reports.generated")).toBeTruthy();
    });

    it("shows 4 active metric series", () => {
      const events = [
        mockEvent({ type: "recovery_initiated", timestamp: NOW - 50 }),
        mockEvent({ type: "recovery_completed", timestamp: NOW - 40 }),
        mockEvent({ type: "verification_passed", timestamp: NOW - 30 }),
        mockEvent({ type: "architect_decision", timestamp: NOW - 20 }),
        mockEvent({ type: "consensus_formed", timestamp: NOW - 10 }),
      ];
      render(<MonitoringDashboard events={events} />);
      // Active series: recovery.attempt (2), verification.total (1),
      // architect.decisions (1), consensus.sessions (1) = 4 active
      expect(screen.getByText("4 active")).toBeTruthy();
    });
  });

  describe("subsystem health", () => {
    it("shows recovery subsystem healthy with recent recovery events", () => {
      const events = [
        mockEvent({ type: "recovery_completed", specialist: "ARCHITECT", timestamp: NOW - 10 }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Subsystem Health/ }));
      expect(screen.getByText("recovery")).toBeTruthy();
    });

    it("shows subsystem degraded when recent failures exist", () => {
      const events = [
        mockEvent({ type: "recovery_failed", specialist: "ARCHITECT", timestamp: NOW - 10 }),
      ];
      render(<MonitoringDashboard events={events} />);
      fireEvent.click(screen.getByRole("button", { name: /Subsystem Health/ }));
      expect(screen.getByText("DEGRADED")).toBeTruthy();
    });
  });

  describe("alert rules", () => {
    it("renders 5 default alert rules in collapsible section", () => {
      render(<MonitoringDashboard events={[]} />);
      fireEvent.click(screen.getByRole("button", { name: /Alert Feed/ }));
      // The rules section is a <details> — expand it so the rules render
      fireEvent.click(screen.getByText(/Alert Rules/));
      expect(screen.getByText(/High recovery failure rate/)).toBeTruthy();
      expect(screen.getByText(/Verification failures/)).toBeTruthy();
      expect(screen.getByText(/Task failure rate/)).toBeTruthy();
      expect(screen.getByText(/System health degradation/)).toBeTruthy();
      expect(screen.getByText(/Execution errors/)).toBeTruthy();
    });
  });
});
