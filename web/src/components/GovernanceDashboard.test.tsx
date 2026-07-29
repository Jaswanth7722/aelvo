import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { GovernanceDashboard } from "./GovernanceDashboard";
import { mockEvent } from "../test-utils";

const NOW = 1_000_000_000;
vi.setSystemTime(NOW * 1000);

describe("GovernanceDashboard", () => {
  it("renders empty state with no events", () => {
    render(<GovernanceDashboard events={[]} />);
    expect(screen.getByText("Governance Dashboard")).toBeTruthy();
    expect(screen.getByText(/0 total events/)).toBeTruthy();
  });

  describe("evaluation outcome classification", () => {
    it("classifies 'approve' outcomes correctly", () => {
      const events = [
        mockEvent({
          type: "architect_decision",
          data: { outcome: "approved", target_type: "task", target_id: "t1", reason: "Looks good", conditions: ["test > 80%"] },
          timestamp: NOW - 30,
        }),
      ];
      render(<GovernanceDashboard events={events} />);
      expect(screen.getByText(/APPROVE/)).toBeTruthy();
    });

    it("classifies 'reject' outcomes correctly", () => {
      const events = [
        mockEvent({
          type: "architect_decision",
          data: { outcome: "rejected", target_type: "task", reason: "Insufficient evidence" },
          timestamp: NOW - 30,
        }),
      ];
      render(<GovernanceDashboard events={events} />);
      expect(screen.getByText(/REJECT/)).toBeTruthy();
    });

    it("classifies 'escalate' outcomes correctly", () => {
      const events = [
        mockEvent({
          type: "architect_decision",
          data: { outcome: "escalated", target_type: "consensus", assigned_to: "ORACLE" },
          timestamp: NOW - 30,
        }),
      ];
      render(<GovernanceDashboard events={events} />);
      expect(screen.getByText(/ESCALATE/)).toBeTruthy();
    });

    it("computes summary stats correctly with mixed outcomes", () => {
      const events = [
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 40 }),
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 35 }),
        mockEvent({ type: "architect_decision", data: { outcome: "rejected" }, timestamp: NOW - 30 }),
        mockEvent({ type: "architect_decision", data: { outcome: "escalated" }, timestamp: NOW - 25 }),
        mockEvent({ type: "architect_decision", data: { outcome: "overridden" }, timestamp: NOW - 20 }),
      ];
      render(<GovernanceDashboard events={events} />);
      // 5 evaluations, 2 approved, 1 rejected, 1 escalated, 1 overridden
      expect(screen.getByText("5")).toBeTruthy(); // Evaluations
      expect(screen.getByText("2")).toBeTruthy(); // Approved
      expect(screen.getByText("1")).toBeTruthy(); // Rejected
    });

    it("classifies fallback outcome for unknown action strings", () => {
      const events = [
        mockEvent({
          type: "architect_decision",
          action: "something_unknown",
          data: {},
          timestamp: NOW - 10,
        }),
      ];
      render(<GovernanceDashboard events={events} />);
      // Falls back to "replan"
      expect(screen.getByText(/REPLAN/)).toBeTruthy();
    });
  });

  describe("security audits", () => {
    it("derives passed status from verification_passed events", () => {
      const events = [
        mockEvent({ type: "verification_passed", action: "Security scan OK", timestamp: NOW - 10 }),
      ];
      render(<GovernanceDashboard events={events} />);
      expect(screen.getByText("PASSED")).toBeTruthy();
    });

    it("derives failed status from verification_failed events", () => {
      const events = [
        mockEvent({ type: "verification_failed", action: "Vulnerability detected", timestamp: NOW - 10 }),
      ];
      render(<GovernanceDashboard events={events} />);
      expect(screen.getByText("FAILED")).toBeTruthy();
    });
  });

  describe("sandbox integrity", () => {
    it("shows pending bridge when no verification data", () => {
      render(<GovernanceDashboard events={[]} />);
      // Switch to sandbox tab
      const sandboxBtn = screen.getByText(/Sandbox Integrity/);
      sandboxBtn.click();
      expect(screen.getByText(/pending bridge/i)).toBeTruthy();
    });

    it("derives binary integrity from verification pass/fail ratio", () => {
      const events = [
        mockEvent({ type: "verification_passed", timestamp: NOW - 10 }),
        mockEvent({ type: "verification_passed", timestamp: NOW - 8 }),
        mockEvent({ type: "verification_failed", timestamp: NOW - 6 }),
      ];
      render(<GovernanceDashboard events={events} />);
      const sandboxBtn = screen.getByText(/Sandbox Integrity/);
      sandboxBtn.click();
      // 2 passes > 1 fail → verified
      expect(screen.getByText("VERIFIED")).toBeTruthy();
    });

    it("derives process health from execution exit codes", () => {
      const events = [
        mockEvent({ type: "execution_completed", data: { exit_code: 0 }, timestamp: NOW - 10 }),
        mockEvent({ type: "execution_completed", data: { exit_code: 0 }, timestamp: NOW - 8 }),
      ];
      render(<GovernanceDashboard events={events} />);
      const sandboxBtn = screen.getByText(/Sandbox Integrity/);
      sandboxBtn.click();
      expect(screen.getByText("HEALTHY")).toBeTruthy();
    });

    it("marks process health degraded when executions fail", () => {
      const events = [
        mockEvent({ type: "execution_completed", data: { exit_code: 1 }, timestamp: NOW - 10 }),
      ];
      render(<GovernanceDashboard events={events} />);
      const sandboxBtn = screen.getByText(/Sandbox Integrity/);
      sandboxBtn.click();
      expect(screen.getByText("DEGRADED")).toBeTruthy();
    });
  });

  describe("audit trail", () => {
    it("populates audit trail from governance-significant events", () => {
      const events = [
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 20 }),
        mockEvent({ type: "challenge_raised", data: { challenge_id: "c1" }, timestamp: NOW - 15 }),
        mockEvent({ type: "consensus_formed", data: { consensus_id: "cs1" }, timestamp: NOW - 10 }),
      ];
      render(<GovernanceDashboard events={events} />);
      const auditBtn = screen.getByText(/Audit Trail/);
      auditBtn.click();
      expect(screen.getByText("3")).toBeTruthy(); // auditRecords count in tab
    });
  });

  describe("active policies", () => {
    it("renders all 6 default policies", () => {
      render(<GovernanceDashboard events={[]} />);
      const policiesBtn = screen.getByText(/Active Policies/);
      policiesBtn.click();
      expect(screen.getByText(/Deny destructive consensus actions/)).toBeTruthy();
      expect(screen.getByText(/Log specialist failover events/)).toBeTruthy();
      expect(screen.getByText(/Deny silent task aborts/)).toBeTruthy();
      expect(screen.getByText(/Log consensus escalations/)).toBeTruthy();
      expect(screen.getByText(/Log task replan events/)).toBeTruthy();
      expect(screen.getByText(/Deny SENTINEL escalation/)).toBeTruthy();
    });
  });

  describe("decision outcome bar", () => {
    it("renders stacked bar and legend", () => {
      const events = Array.from({ length: 10 }, (_, i) =>
        mockEvent({
          type: "architect_decision",
          data: {
            outcome: i < 4 ? "approved" : i < 7 ? "rejected" : "escalated",
          },
          timestamp: NOW - i * 5,
        })
      );
      render(<GovernanceDashboard events={events} />);
      expect(screen.getByText(/Decision Outcomes/)).toBeTruthy();
      expect(screen.getByText(/Approved \(4\)/)).toBeTruthy();
      expect(screen.getByText(/Rejected \(3\)/)).toBeTruthy();
      expect(screen.getByText(/Escalated \(3\)/)).toBeTruthy();
    });
  });
});
