"""
Unit and Integration Tests for HERMES and HERALD Specialists
Run with: pytest tests/test_specialists.py -v
"""
import pytest
from specialists.hermes import HermesSpecialist
from specialists.herald import HeraldSpecialist

def test_hermes_activation_scores():
    hermes = HermesSpecialist()
    # High score for preference/style/calibration tasks
    score1 = hermes.compute_activation_score("calibrate the response style for a new user", {})
    assert score1 >= 0.5
    
    # Low score for unrelated task
    score2 = hermes.compute_activation_score("run a git commit command on staging branch", {})
    assert score2 < 0.5

def test_hermes_user_model_extraction():
    hermes = HermesSpecialist()
    history = [
        {"role": "user", "content": "I need help with python database migrations immediately! Eslint failed and it is really frustrating!"},
        {"role": "user", "content": "No, that's not what I meant. Please just show me the code directly."}
    ]
    model = hermes.build_user_model(history)
    assert model["communication_style"] == "brief_direct"
    assert model["workflow_mode"] == "debugging"
    assert model["code_preference"] == "code_first"
    assert model["frustration"] is True

def test_hermes_calibration_formatting():
    hermes = HermesSpecialist()
    user_model = {
        "expertise": "high",
        "workflow_mode": "build",
        "communication_style": "brief_direct",
        "frustration": True,
        "code_preference": "code_first"
    }
    
    raw_response = (
        "Basically, here is the solution to your issue. We will implement it now.\n\n"
        "```python\n"
        "def solve():\n"
        "    return True\n"
        "```\n\n"
        "Let me explain what this code does in detail."
    )
    
    calibrated = hermes.calibrate_response(raw_response, user_model)
    # Checks:
    # 1. 'Basically,' should be stripped because expertise is high
    assert "Basically" not in calibrated
    # 2. Code should be hoisted to the top because workflow is build/code_first
    assert calibrated.startswith("```python")
    # 3. Text should be trimmed to a shorter budget because frustration is true
    assert len(calibrated.split()) < 150

def test_herald_activation_scores():
    herald = HeraldSpecialist()
    # High score for writing drafts, emails, slack messages
    score1 = herald.compute_activation_score("draft a formal email to our manager explaining the regression blocker", {})
    assert score1 >= 0.6
    
    # Low score for pure technical queries
    score2 = herald.compute_activation_score("run tests on file module.py", {})
    assert score2 < 0.6

def test_herald_classification():
    herald = HeraldSpecialist()
    task = "draft a message to my manager explaining why the deployment is late and the client is upset"
    classification = herald.classify_communication(task)
    assert classification["stakes"] == "high"
    assert classification["relationship"] == "manager"
    assert classification["emotional_tone"] == "difficult"

def test_herald_variants():
    herald = HeraldSpecialist()
    task = "draft an email to the team regarding missed deadlines"
    variants = herald.draft_variants(task, {})
    assert len(variants) == 3
    for v in variants:
        assert "strategy_name" in v
        assert "what_it_prioritizes" in v
        assert "what_it_trades_off" in v
        assert "draft" in v
        assert "when_to_use" in v

def test_herald_verify_output():
    herald = HeraldSpecialist()
    context = {"task": "write an email to client about late delivery"}
    
    # Missing variants / tradeoffs in high-stakes
    bad_output = "Variant A: We are sorry it is late. Here is the draft."
    ok, err = herald.verify_output(bad_output, context)
    assert ok is False
    
    # Correct structure
    good_output = (
        "Here are the options:\n\n"
        "Variant A: Hold Firm\n"
        "Prioritizes: scope control.\n"
        "Trades Off: relationship.\n"
        "Draft: Sorry, we need time.\n\n"
        "Variant B: Seek Alignment\n"
        "Prioritizes: relationship.\n"
        "Trades Off: scope.\n"
        "Draft: Let's find a compromise."
    )
    ok2, err2 = herald.verify_output(good_output, context)
    assert ok2 is True
