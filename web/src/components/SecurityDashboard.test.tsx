import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { SecurityDashboard } from "./SecurityDashboard";
import { mockEvent } from "../test-utils";

const NOW = 1_000_000_000;
vi.setSystemTime(NOW * 1000);

describe("SecurityDashboard", () => {
  it("renders unknown posture when no events", () => {
    render(<SecurityDashboard events={[]} />);
    expect(screen.getByText("Security Dashboard")).toBeTruthy();
    expect(screen.getByText(/UNKNOWN/)).toBeTruthy();
  });

  describe("finding derivation", () => {
    it("creates high-severity finding from verification_failed", () => {
      const events = [
        mockEvent({
          type: "verification_failed",
          specialist: "SENTINEL",
          data: { check_id: "v1", diagnostics: "Security scan found vulnerabilities" },
          timestamp: NOW - 10,
        }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText("HIGH")).toBeTruthy();
      expect(screen.getByText(/Verification check failed/)).toBeTruthy();
    });

    it("creates medium-severity finding from task_failed", () => {
      const events = [
        mockEvent({
          type: "task_failed",
          specialist: "FORGE",
          data: { task_id: "t1" },
          timestamp: NOW - 10,
        }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText("MEDIUM")).toBeTruthy();
      expect(screen.getByText(/Task failed unexpectedly/)).toBeTruthy();
    });

    it("creates low-severity finding from challenge_raised", () => {
      const events = [
        mockEvent({
          type: "challenge_raised",
          specialist: "SENTINEL",
          data: { challenge_id: "c1" },
          timestamp: NOW - 10,
        }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText("LOW")).toBeTruthy();
      expect(screen.getByText(/Challenge raised on blackboard entry/)).toBeTruthy();
    });

    it("classifies verification_failed as policy_violation when diagnostics mention security", () => {
      const events = [
        mockEvent({
          type: "verification_failed",
          data: { diagnostics: "Security policy violation detected" },
          timestamp: NOW - 10,
        }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText(/Policy Violation/)).toBeTruthy();
    });

    it("classifies verification_failed as sandbox_tamper when diagnostics mention sandbox", () => {
      const events = [
        mockEvent({
          type: "verification_failed",
          data: { diagnostics: "Sandbox integrity violation" },
          timestamp: NOW - 10,
        }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText(/Sandbox Tamper/)).toBeTruthy();
    });
  });

  describe("posture calculation", () => {
    it("shows critical posture with integrity failures", () => {
      const events = Array.from({ length: 10 }, (_, i) =>
        mockEvent({ type: "verification_failed", timestamp: NOW - i * 5 })
      );
      render(<SecurityDashboard events={events} />);
      // 10 verification_failed → binary_integrity fails, fs_isolation fails → critical
      expect(screen.getByText(/CRITICAL/)).toBeTruthy();
    });

    it("shows attention_needed posture with high findings but no critical", () => {
      const events = [
        mockEvent({ type: "verification_failed", data: {}, timestamp: NOW - 10 }),
      ];
      render(<SecurityDashboard events={events} />);
      // 1 verification_failed = high severity → attention_needed
      expect(screen.getByText(/ATTENTION NEEDED/)).toBeTruthy();
    });

    it("shows healthy posture when events exist with no failures", () => {
      const events = [
        mockEvent({ type: "task_completed", timestamp: NOW - 10 }),
        mockEvent({ type: "blackboard_publication", timestamp: NOW - 5 }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText(/HEALTHY/)).toBeTruthy();
    });
  });

  describe("scan history", () => {
    it("creates 5 scan windows with correct time labels", () => {
      render(<SecurityDashboard events={[]} />);
      const scansBtn = screen.getByText(/Scan History/);
      scansBtn.click();
      expect(screen.getByText(/scan_1m/)).toBeTruthy();
      expect(screen.getByText(/scan_5m/)).toBeTruthy();
      expect(screen.getByText(/scan_15m/)).toBeTruthy();
      expect(screen.getByText(/scan_1h/)).toBeTruthy();
      expect(screen.getByText(/scan_24h/)).toBeTruthy();
    });

    it("marks scan as passed when no failure events in window", () => {
      const events = [
        mockEvent({ type: "task_completed", timestamp: NOW - 10 }),
      ];
      render(<SecurityDashboard events={events} />);
      const scansBtn = screen.getByText(/Scan History/);
      scansBtn.click();
      const passedLabels = screen.getAllByText("PASSED");
      expect(passedLabels.length).toBeGreaterThan(0);
    });

    it("marks scan as failed when failure events exist", () => {
      const events = [
        mockEvent({ type: "verification_failed", timestamp: NOW - 10 }),
      ];
      render(<SecurityDashboard events={events} />);
      const scansBtn = screen.getByText(/Scan History/);
      scansBtn.click();
      expect(screen.getByText("FAILED")).toBeTruthy();
    });
  });

  describe("integrity checks", () => {
    it("renders all 4 integrity check types", () => {
      render(<SecurityDashboard events={[]} />);
      const integrityBtn = screen.getByText(/Integrity Checks/);
      integrityBtn.click();
      expect(screen.getByText("Binary Integrity")).toBeTruthy();
      expect(screen.getByText("Audit Log Integrity")).toBeTruthy();
      expect(screen.getByText("Process Health")).toBeTruthy();
      expect(screen.getByText("Filesystem Isolation")).toBeTruthy();
    });
  });

  describe("severity breakdown", () => {
    it("renders severity bar when findings exist", () => {
      const events = [
        mockEvent({ type: "verification_failed", data: {}, timestamp: NOW - 10 }),
        mockEvent({ type: "task_failed", data: {}, timestamp: NOW - 8 }),
        mockEvent({ type: "challenge_raised", data: {}, timestamp: NOW - 6 }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText(/Finding Severity Breakdown/)).toBeTruthy();
    });

    it("shows recommendations when findings exist", () => {
      const events = [
        mockEvent({ type: "verification_failed", data: {}, timestamp: NOW - 10 }),
        mockEvent({ type: "verification_failed", data: {}, timestamp: NOW - 8 }),
      ];
      render(<SecurityDashboard events={events} />);
      expect(screen.getByText(/Recommendations/)).toBeTruthy();
    });
  });
});
