import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { ConsensusDashboard } from "./ConsensusDashboard";
import { mockEvent } from "../test-utils";

const NOW = 1_000_000_000;
vi.setSystemTime(NOW * 1000);

describe("ConsensusDashboard", () => {
  it("renders empty state with no events", () => {
    render(<ConsensusDashboard events={[]} />);
    expect(screen.getByText("Consensus Dashboard")).toBeTruthy();
    expect(screen.getByText("0 total events")).toBeTruthy();
  });

  describe("architect decisions", () => {
    it("renders approved decisions with green badge", () => {
      const events = [
        mockEvent({
          type: "architect_decision",
          data: {
            decision_id: "d1",
            outcome: "approved",
            reason: "Plan meets all requirements",
          },
          timestamp: NOW - 30,
        }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/APPROVED/)).toBeTruthy();
      expect(screen.getByText("Plan meets all requirements")).toBeTruthy();
    });

    it("derives decision summary stats from mixed outcomes", () => {
      const events = [
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 50 }),
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 45 }),
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 40 }),
        mockEvent({ type: "architect_decision", data: { outcome: "rejected" }, timestamp: NOW - 35 }),
        mockEvent({ type: "architect_decision", data: { outcome: "escalated" }, timestamp: NOW - 30 }),
        mockEvent({ type: "architect_decision", data: { outcome: "revision" }, timestamp: NOW - 25 }),
        mockEvent({ type: "architect_decision", data: { outcome: "replanned" }, timestamp: NOW - 20 }),
      ];
      render(<ConsensusDashboard events={events} />);
      // Total: 7 decisions. 3 approved, 1 rejected, 1 escalated, 1 revision, 1 replanned
      expect(screen.getByText("7")).toBeTruthy(); // Decisions
      // Approval rate: 3/7 ≈ 43%
      expect(screen.getByText("43%")).toBeTruthy();
    });

    it("shows overridden recommendation when present", () => {
      const events = [
        mockEvent({
          type: "architect_decision",
          data: {
            outcome: "overridden",
            overridden_recommendation: "Use FORGE instead",
            assigned_to: "TERMINUS",
          },
          timestamp: NOW - 20,
        }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/Use FORGE instead/)).toBeTruthy();
    });
  });

  describe("consensus sessions", () => {
    it("renders consensus sessions with confidence badges", () => {
      const events = [
        mockEvent({
          type: "consensus_formed",
          data: {
            consensus_id: "cs1",
            confidence: 0.85,
            method: "majority",
            recommendation: "Proceed with plan A",
            positions: {
              ARCHITECT: "for",
              SENTINEL: "for",
              ORACLE: "against",
            },
          },
          timestamp: NOW - 30,
        }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/Proceed with plan A/)).toBeTruthy();
      expect(screen.getByText(/majority/)).toBeTruthy();
      expect(screen.getByText(/85%/)).toBeTruthy(); // 0.85 → 85%
    });

    it("counts for/against positions correctly", () => {
      const events = [
        mockEvent({
          type: "consensus_formed",
          data: {
            consensus_id: "cs1",
            confidence: 0.7,
            positions: {
              ARCHITECT: "for",
              SENTINEL: "for",
              ORACLE: "against",
              FORGE: "for",
              TERMINUS: "abstain",
            },
          },
          timestamp: NOW - 30,
        }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/3 for/)).toBeTruthy();
      expect(screen.getByText(/1 against/)).toBeTruthy();
      expect(screen.getByText(/1 neutral/)).toBeTruthy();
    });
  });

  describe("challenges", () => {
    it("renders challenge records", () => {
      const events = [
        mockEvent({
          type: "challenge_raised",
          specialist: "SENTINEL",
          data: {
            challenge_id: "ch1",
            entry_id: "entry_abc123",
            challenger: "SENTINEL",
            challenged_claim: "Claimed 95% accuracy without sufficient evidence",
          },
          timestamp: NOW - 30,
        }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/CHALLENGE/)).toBeTruthy();
      expect(screen.getByText(/by SENTINEL/)).toBeTruthy();
      expect(screen.getByText(/Claimed 95% accuracy without sufficient evidence/)).toBeTruthy();
    });

    it("counts total challenges in summary card", () => {
      const events = [
        mockEvent({ type: "challenge_raised", data: { challenge_id: "ch1" }, timestamp: NOW - 30 }),
        mockEvent({ type: "challenge_raised", data: { challenge_id: "ch2" }, timestamp: NOW - 20 }),
        mockEvent({ type: "challenge_raised", data: { challenge_id: "ch3" }, timestamp: NOW - 10 }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText("3")).toBeTruthy(); // Challenges count
    });
  });

  describe("decision outcome bar", () => {
    it("renders stacked outcome bar when decisions exist", () => {
      const events = [
        mockEvent({ type: "architect_decision", data: { outcome: "approved" }, timestamp: NOW - 50 }),
        mockEvent({ type: "architect_decision", data: { outcome: "rejected" }, timestamp: NOW - 40 }),
        mockEvent({ type: "architect_decision", data: { outcome: "escalated" }, timestamp: NOW - 30 }),
        mockEvent({ type: "architect_decision", data: { outcome: "revision" }, timestamp: NOW - 20 }),
        mockEvent({ type: "architect_decision", data: { outcome: "replanned" }, timestamp: NOW - 10 }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/Decision Outcomes/)).toBeTruthy();
      expect(screen.getByText(/Approved \(1\)/)).toBeTruthy();
      expect(screen.getByText(/Rejected \(1\)/)).toBeTruthy();
      expect(screen.getByText(/Escalated \(1\)/)).toBeTruthy();
      expect(screen.getByText(/Revision \(1\)/)).toBeTruthy();
      expect(screen.getByText(/Replanned \(1\)/)).toBeTruthy();
    });
  });

  describe("high confidence count", () => {
    it("counts consensus sessions with confidence >= 0.7 as high confidence", () => {
      const events = [
        mockEvent({ type: "consensus_formed", data: { confidence: 0.9 }, timestamp: NOW - 30 }),
        mockEvent({ type: "consensus_formed", data: { confidence: 0.5 }, timestamp: NOW - 20 }),
        mockEvent({ type: "consensus_formed", data: { confidence: 0.8 }, timestamp: NOW - 10 }),
      ];
      render(<ConsensusDashboard events={events} />);
      expect(screen.getByText(/2 high confidence/)).toBeTruthy();
    });
  });
});
