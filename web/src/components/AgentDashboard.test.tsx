import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { AgentDashboard } from "./AgentDashboard";
import { mockEvent } from "../test-utils";

const NOW = 1_000_000_000;
vi.setSystemTime(NOW * 1000);

describe("AgentDashboard", () => {
  it("renders all 7 agents", () => {
    render(<AgentDashboard events={[]} />);
    expect(screen.getByText("Architect")).toBeTruthy();
    expect(screen.getByText("Oracle")).toBeTruthy();
    expect(screen.getByText("Forge")).toBeTruthy();
    expect(screen.getByText("Sentinel")).toBeTruthy();
    expect(screen.getByText("Terminus")).toBeTruthy();
    expect(screen.getByText("Herald")).toBeTruthy();
    expect(screen.getByText("Consensus")).toBeTruthy();
  });

  describe("agent status derivation", () => {
    it("shows idle status when agent has no events", () => {
      render(<AgentDashboard events={[]} />);
      const idleLabels = screen.getAllByText("Idle");
      expect(idleLabels.length).toBe(7);
    });

    it("shows active status for agents with recent events", () => {
      const events = [
        mockEvent({
          type: "blackboard_publication",
          specialist: "ARCHITECT",
          timestamp: NOW - 5,
        }),
        mockEvent({
          type: "execution_completed",
          specialist: "TERMINUS",
          data: { exit_code: 0 },
          timestamp: NOW - 10,
        }),
      ];
      render(<AgentDashboard events={events} />);
      const activeLabels = screen.getAllByText("Active");
      expect(activeLabels.length).toBe(2);
    });
  });

  describe("Terminus success rate", () => {
    it("computes Terminus success rate from exit codes", () => {
      const events = [
        mockEvent({ type: "execution_completed", specialist: "TERMINUS", data: { exit_code: 0 }, timestamp: NOW - 10 }),
        mockEvent({ type: "execution_completed", specialist: "TERMINUS", data: { exit_code: 0 }, timestamp: NOW - 8 }),
        mockEvent({ type: "execution_completed", specialist: "TERMINUS", data: { exit_code: 1 }, timestamp: NOW - 6 }),
      ];
      render(<AgentDashboard events={events} />);
      // 2 successes / 3 total ≈ 67%
      // The success rate is displayed per agent card. Terminus should show ~67%
      const successElements = screen.getAllByText(/67%/);
      expect(successElements.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe("Sentinel success rate", () => {
    it("computes Sentinel success rate from verification events", () => {
      const events = [
        mockEvent({ type: "verification_passed", specialist: "SENTINEL", timestamp: NOW - 10 }),
        mockEvent({ type: "verification_passed", specialist: "SENTINEL", timestamp: NOW - 8 }),
        mockEvent({ type: "verification_failed", specialist: "SENTINEL", timestamp: NOW - 6 }),
      ];
      render(<AgentDashboard events={events} />);
      // 2 passes / 3 total ≈ 67%
      const successElements = screen.getAllByText(/67%/);
      expect(successElements.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe("Forge success rate", () => {
    it("computes Forge success rate from verified publications", () => {
      const events = [
        mockEvent({ type: "blackboard_publication", specialist: "FORGE", data: { verification_status: "verified" }, timestamp: NOW - 10 }),
        mockEvent({ type: "blackboard_publication", specialist: "FORGE", data: { verification_status: "failed" }, timestamp: NOW - 8 }),
      ];
      render(<AgentDashboard events={events} />);
      // 1 success / 2 total = 50%
      expect(screen.getByText("50%")).toBeTruthy();
    });
  });

  describe("Architect success rate", () => {
    it("computes Architect success rate from non-rejected decisions", () => {
      const events = [
        mockEvent({ type: "architect_decision", specialist: "ARCHITECT", action: "approved plan", timestamp: NOW - 10 }),
        mockEvent({ type: "architect_decision", specialist: "ARCHITECT", action: "approved task", timestamp: NOW - 8 }),
        mockEvent({ type: "architect_decision", specialist: "ARCHITECT", action: "rejected proposal", timestamp: NOW - 6 }),
      ];
      render(<AgentDashboard events={events} />);
      // 2 approvals / 3 total ≈ 67%
      const successElements = screen.getAllByText(/67%/);
      expect(successElements.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe("CONSENSUS success rate", () => {
    it("computes Consensus success rate from high-confidence outcomes", () => {
      const events = [
        mockEvent({ type: "consensus_formed", specialist: "CONSENSUS", data: { confidence: 0.9 }, timestamp: NOW - 10 }),
        mockEvent({ type: "consensus_formed", specialist: "CONSENSUS", data: { confidence: 0.5 }, timestamp: NOW - 8 }),
      ];
      render(<AgentDashboard events={events} />);
      // 1 high-confidence / 2 total = 50%
      expect(screen.getByText("50%")).toBeTruthy();
    });
  });

  describe("contribution score", () => {
    it("computes contribution score from weighted events", () => {
      const events = [
        mockEvent({ type: "blackboard_publication", specialist: "ARCHITECT", timestamp: NOW - 10 }),   // pubs * 3 = 3
        mockEvent({ type: "architect_decision", specialist: "ARCHITECT", timestamp: NOW - 8 }),         // decisions * 4 = 4
        mockEvent({ type: "architect_decision", specialist: "ARCHITECT", timestamp: NOW - 6 }),         // decisions * 4 = 4
      ];
      render(<AgentDashboard events={events} />);
      // Total: 3 + 4 + 4 = 11
      // Architect should show contribution of 11
      expect(screen.getByText("11")).toBeTruthy();
    });
  });

  describe("confidence calculation", () => {
    it("computes average confidence from blackboard publications", () => {
      const events = [
        mockEvent({ type: "blackboard_publication", specialist: "ORACLE", data: { confidence: 0.8 }, timestamp: NOW - 10 }),
        mockEvent({ type: "blackboard_publication", specialist: "ORACLE", data: { confidence: 0.9 }, timestamp: NOW - 8 }),
      ];
      render(<AgentDashboard events={events} />);
      // Average = 0.85 → rounded to 85%
      expect(screen.getByText("85%")).toBeTruthy();
    });
  });

  describe("current task", () => {
    it("shows latest action as current task", () => {
      const events = [
        mockEvent({ type: "execution_completed", specialist: "TERMINUS", action: "deploy release v2.0", data: { exit_code: 0 }, timestamp: NOW - 5 }),
        mockEvent({ type: "execution_started", specialist: "TERMINUS", action: "start deploy", timestamp: NOW - 10 }),
      ];
      render(<AgentDashboard events={events} />);
      expect(screen.getByText("deploy release v2.0")).toBeTruthy();
    });
  });

  describe("metrics breakdown", () => {
    it("tracks per-agent event counts", () => {
      const events = [
        mockEvent({ type: "blackboard_publication", specialist: "FORGE", timestamp: NOW - 10 }),
        mockEvent({ type: "finding_consumed", specialist: "FORGE", timestamp: NOW - 8 }),
        mockEvent({ type: "verification_passed", specialist: "FORGE", timestamp: NOW - 6 }),
      ];
      render(<AgentDashboard events={events} />);
      // Metrics displayed in the agent card as "Events" count
      // Total events for FORGE = 3
      const eventCounts = screen.getAllByText("3");
      expect(eventCounts.length).toBeGreaterThanOrEqual(1);
    });
  });
});
