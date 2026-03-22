# VAIB SYSTEM RULES v20.0

================================================================================
1. SYSTEM MODE
================================================================================
CURRENT_MODE: PROTOTYPE

Options:
  - PROTOTYPE: Fast iterations, relaxed checks, NO Skeptic
  - PRODUCTION: Strict limits, mandatory logs, Skeptic Active

================================================================================
2. ABSOLUTE PROHIBITIONS
================================================================================
Unless explicitly requested in development_plan.md OR by User:

| Action | Why Forbidden | Exception Protocol |
|--------|---------------|--------------------|
| Dependency Upgrades | Breaking changes risk | Separate task with version check |
| DB Schema Changes | Data loss / Migration | Explicit migration plan with rollback |
| Infra/CI-CD Changes | Environment breakage | ADR + staged rollout |
| Breaking API Changes | Client compatibility | Versioned API (v2) or migration guide |
| Deletion of Logic | Business value loss | Mark as @deprecated instead |
| Plan rewrite without backup | Traceability loss | Mandatory backup first |

================================================================================
3. LANGUAGE POLICY
================================================================================
- Chat/Reasoning: Russian
- Code (variables, files): English
- NEW Code Comments: Russian (Intent/Why/Contract)
- LEGACY Comments: NEVER translate, keep original
- Public Docs: English

================================================================================
4. STOP DISCIPLINE
================================================================================
- Requirements unclear -> STOP -> Ask User
- No assumptions -> Wait for explicit "GO" / "Approved"
- Phase unclear -> STOP -> Ask Architect/User
- Code contradicts plan -> STOP -> Request reconciliation

================================================================================
5. CODING STANDARDS
================================================================================
- Markup: Follow vaib/markup_standard.md for GRACE anchors
- Typing: Strict (no any in TS, type hints in Python)
- Errors: No swallowing, log with trace

================================================================================
6. SECURITY GATES
================================================================================
- Secrets: ENV variables only, no hardcoded keys
- SQL: Parameterized queries only
- Input: Explicit validation on public methods

================================================================================
7. LOOP & ROUTING
================================================================================

7.1 Loop Limits
---------------
- Coder<->Tester: 3 iterations -> Expert
- Editor<->Tester: 3 iterations -> Expert
- Skeptic per-module: 2 rejections -> Expert

7.2 Routing Criteria
--------------------
- Logic Fail (contract violated, wrong output) -> Coder
- Minor/Syntax (typo, import, style) -> Editor
- Phase Gate: Next phase cannot start until current verified

7.3 Self-Correction
-------------------
- Coder and Editor MUST run syntax checks before handoff

================================================================================
8. COMPLEXITY LIMITS
================================================================================
                        PROTOTYPE    PRODUCTION
Function Length         60 lines     20 lines
Cyclomatic Complexity   12 (Rank B)  7 (Rank A)
Class Size              400 lines    200 lines

Tools: radon, flake8, wemake-python-styleguide (or JS equivalents)

================================================================================
9. TEST OWNERSHIP
================================================================================
- Coder: NEVER writes tests - all testing is Tester's responsibility
- Tester: OWNS ALL testing - smoke, coverage, comprehensive, adversarial tests
- Tester: Writes tests BEFORE verifying implementation (IF NO TESTS: WRITE THEM)
- Tester: Emits PHASE_RESULT: PASS | FAIL via attempt_completion
- Orchestrator: Updates Phase Execution Status (NOT Tester)
- Orchestrator: NEVER delegates test writing to Coder

================================================================================
10. SKEPTIC AUDIT
================================================================================

10.1 Trigger
------------
Runs ONLY if CURRENT_MODE == PRODUCTION

10.2 Rejection Criteria
-----------------------
1. Duplication: Logic repeated > 2 times (DRY violation)
2. Over-Engineering: Heavy libs for trivial tasks
3. Fragility: Hardcoded values instead of config
4. Contract Rot: Code does not match Intent

10.3 Arbitration
----------------
If Skeptic rejects SAME module > 2 times:
-> ESCALATE to Expert [Arbitration Needed]
-> Expert decides: Force Approve OR Rewrite Architecture

================================================================================
11. DEVELOPMENT PLAN GOVERNANCE
================================================================================

11.1 Single Source of Truth
----------------------------
- development_plan.md is the ONLY active implementation source
- Backup files are read-only history
- No backup files for implementation unless explicit rollback request

11.2 Backup Policy
------------------
BEFORE modifying development_plan.md or technology.md:
- Location: vaib/02-architect/backups/ 
- Format: backup-YYYYMMDD-HHMMSS-{filename}.md

11.3 Phase Structure
--------------------
Each phase:
- Self-contained milestone
- 1 logical milestone, 1-2 coder sessions, 3-7 deliverables
- If too large: split into additional phases

11.4 Required Plan Sections
---------------------------
1. Project Overview
2. Architecture / Modules
3. Negative Constraints
4. Phases
5. Phase Execution Status
6. Open Questions (optional)
7. Archive (optional)

11.5 Phase Format
-----------------
## Phase N: [Name]
- Goal:
- Scope:
- Deliverables:
- Dependencies:
- Negative Constraints:
- Done Criteria:
- Notes for Tester:

11.6 Phase Statuses
-------------------
See section 12.4 Phase State Machine

11.7 Ownership
--------------
Architect owns:
  - architecture, module contracts, phase definitions
  - phase restructuring, status correction

Coder owns:
  - code implementation, personal TODO file
  - CANNOT: modify plan, phases, status, architecture

Tester owns:
  - verification result, done criteria validation
  - CANNOT: update execution status

Orchestrator owns:
  - Phase Execution Status updates
  - Agent delegation, loop counters, state machine

11.8 Phase Progression
----------------------
- Coder implements ONLY current approved phase
- Coder NEVER silently jumps phases
- Active phase resolution:
  1. First IN_PROGRESS
  2. Else first PLANNED
  3. BLOCKED never auto-selected

11.9 Plan Changes
-----------------
If User changes scope during implementation:
1. Create backup
2. Update affected phases
3. Preserve unfinished work
4. Mark obsolete work explicitly
5. Update statuses

11.10 Definition of Done
------------------------
Phase DONE requires:
- Implementation exists
- Tests exist and pass
- Tester confirms plan met
- No unresolved blockers

PROTOTYPE: Tester marks DONE after PASS
PRODUCTION: Tester waits for Skeptic approval

11.11 Anti-Drift
----------------
If code and plan diverge (Structural Drift):
- Tester/Skeptic MUST report
- Architect MUST reconcile before next phase

11.12 Completion
----------------
When all phases DONE or CANCELLED:
- Architect may produce new plan version or close cycle

================================================================================
12. ORCHESTRATOR GOVERNANCE
================================================================================

12.1 Role
---------
Central workflow coordinator:
- Agent delegation via new_task (ONLY vaibX-* agents)
- Phase progression tracking
- Phase Execution Status updates
- Loop escalation handling

12.2 Allowed Agents
-------------------
MAY delegate to:
  vaib1-analyst through vaib8-skeptic
  vaib99-archaeologist

MUST NOT delegate to:
  code, architect, debug, ask, any non-vaib modes
  


12.3 State File
---------------
Location: vaib/memory/orchestrator_state.md
Tracks: current_phase, phase_status, loop_counters, active/completed_tasks
Backups: vaib/memory/backups/

12.4 Phase State Machine
------------------------
```
PLANNED -> IN_PROGRESS -> TESTING -> (AUDITING) -> DONE
    ^          |            |            |
    +----------+------------+------------+
               v
           BLOCKED
```

Statuses:
- PLANNED: phase not started
- IN_PROGRESS: Coder working
- TESTING: Tester verifying
- AUDITING: Skeptic reviewing (PRODUCTION only)
- DONE: phase complete
- BLOCKED: escalation required
- CANCELLED: cancelled by user/architect

12.5 Loop Limits
----------------
See section 7.1 Loop Limits

12.6 Decision Matrix
--------------------
| Current     | Result              | Mode       | Next        | Update           |
|-------------|---------------------|------------|-------------|------------------|
| PLANNED     | -                   | any        | Spec        | -                |
| Spec        | SUCCESS             | any        | Coder       | -> IN_PROGRESS   |
| Spec        | FAIL/NO_DOCS        | any        | Spec        | retry            |
| IN_PROGRESS | Coder SUCCESS       | any        | Tester      | -> TESTING       |
| IN_PROGRESS | Coder FAIL(Arch)    | any        | Expert      | -> BLOCKED       |
| TESTING     | Tester PASS         | PROTOTYPE  | Coder(next) | current -> DONE  |
| TESTING     | Tester PASS         | PRODUCTION | Skeptic     | -> AUDITING      |
| TESTING     | Tester FAIL(Logic)  | any        | Coder       | coder_tester++   |
| TESTING     | Tester FAIL(Minor)  | any        | Editor      | editor_tester++  |
| TESTING     | Editor SUCCESS      | any        | Tester      | reset editor     |
| AUDITING    | Skeptic APPROVED    | PRODUCTION | Coder(next) | current -> DONE  |
| AUDITING    | Skeptic REJECT(CODE)| PRODUCTION | Coder       | skeptic++        |
| AUDITING    | Skeptic REJECT(ARCH)| PRODUCTION | Architect   | skeptic++        |
| any         | Loop limit          | any        | Expert      | -> BLOCKED       |
| BLOCKED     | Expert ESCALATION   | any        | TARGET      | per recovery     |
| all DONE    | -                   | any        | -           | COMPLETE         |

12.7 Agent Delegation Enhancement
---------------------------------
When delegating to agents via new_task:
- message: explain WHAT happened, pass critical data (errors, questions) AND ALWAYS INCLUDE THE FULL ORIGINAL USER REQUEST TEXT to ensure complete context is maintained
- This ensures agents have complete understanding of user requirements from the beginning of the project

12.8 Handling Blocked States
----------------------------
When an agent completes with STATUS: BLOCKED and pending questions for the user:
- The orchestrator MUST NOT attempt to answer these questions independently
- The orchestrator MUST follow the procedure in section 5: HANDLING BLOCKED WITH QUESTIONS
- The orchestrator MUST set phase_status: BLOCKED, present questions to user, and wait for actual user answers

12.9 Completion Signal
----------------------
All agents use attempt_completion:
```
STATUS: SUCCESS | FAILURE | BLOCKED | ESCALATION
TYPE: Logic | Minor | Syntax | Architecture | PlanDrift | Unknown
OUTPUT: [list of files]
SUMMARY: brief description