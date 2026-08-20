# Invariant Governance

This document describes how Invariant is governed, how decisions are made, and the roles and responsibilities of project participants.

## Project Philosophy

Invariant is an open-source project designed to:

1. **Serve the data engineering community** with a robust verification primitive
2. **Remain vendor-neutral** and not favoring any single platform or engine
3. **Move deliberately** on breaking changes to maintain ecosystem stability
4. **Empower contributors** to shape the project vision
5. **Maintain transparency** in all significant decisions

## Decision-Making Process

### Proposal Levels

**Level 1: Patch Fixes (Bug Fixes, Documentation)**

- **Process**: Direct PR, no issue required
- **Approval**: 1 maintainer review
- **Timeline**: Can merge immediately
- **Examples**: Fix typo, update docs, patch regression

**Level 2: Minor Features (New Transformation, Test Improvement)**

- **Process**: Discussion issue OR feature request issue
- **Approval**: 2 maintainer reviews, community feedback welcome
- **Timeline**: Discuss for 1 week, then can proceed
- **Examples**: Add new computed column, improve error messages

**Level 3: Major Features (New Engine Support, Contract Format Change)**

- **Process**: Extensive discussion, design proposal, RFC (Request for Comments)
- **Approval**: Maintainer consensus (no vetoes)
- **Timeline**: 2-4 weeks of community input
- **Examples**: dbt support (Phase 2), breaking API changes

**Level 4: Architectural Decisions (Major Roadmap Pivot, Licensing Change)**

- **Process**: Steering committee discussion, open RFC, maintainer vote
- **Approval**: Unanimous or super-majority (80%+)
- **Timeline**: 1-2 months, formal period
- **Examples**: Drop support for Spark 3.4, change to different contract standard

### Decision Criteria

Decisions are evaluated on:

1. **Alignment with Mission** — Does it serve Invariant's vision?
2. **Community Benefit** — Does it help the target audience?
3. **Maintainability** — Can the project maintain it long-term?
4. **Backward Compatibility** — Does it break existing usage?
5. **Precedent** — Is it consistent with past decisions?

## Roles and Responsibilities

### Contributor

**Who:** Anyone who submits code, documentation, or bug reports.

**Permissions:**
- Report issues
- Propose features via discussions
- Submit PRs (following CONTRIBUTING.md)
- Comment on issues and PRs

**Responsibilities:**
- Follow Code of Conduct
- Follow contribution guidelines
- Write tests for code changes
- Respond to review feedback

### Committer

**Who:** Active contributor with merge access (≥3 months, ≥5 merged PRs, community respect).

**Permissions:**
- All contributor permissions
- Merge PRs (following review requirements)
- Close issues with justification
- Commit directly (for urgent fixes)

**Responsibilities:**
- Maintain quality bar (tests, documentation)
- Mentor new contributors
- Review PRs within 5 business days
- Participate in major decisions
- Keep CHANGELOG updated

**Process to become committer:**
1. Be nominated by existing maintainer or committer
2. Demonstrate understanding of codebase and project goals
3. Show respect for community and contribution process
4. Maintainer consensus vote (no vetoes)
5. Formal announcement in CONTRIBUTORS.md

### Maintainer

**Who:** Long-term project leaders (≥6 months active, deep codebase knowledge, strategic vision).

**Permissions:**
- All committer permissions
- Final approval on major decisions
- Release authority
- Add/remove committers
- Represent project externally

**Responsibilities:**
- Drive roadmap and long-term strategy
- Mentor committers
- Final review on architectural decisions
- Ensure project health (tests, documentation, dependencies)
- Respond to critical bugs quickly
- Attend steering committee meetings (if formed)

**Maintainers (current Phase 0):**
- Project lead (mlltx organization)

**Process to become maintainer:**
1. Be nominated by existing maintainer
2. Demonstrate deep project knowledge and alignment with mission
3. Show leadership in mentoring or architecture
4. Unanimous vote among existing maintainers
5. Formal announcement and role transition

### Steering Committee (Future - Post Phase 1)

**When formed:** Once project reaches production use (Phase 1 completion).

**Members:** 3-5 maintainers representing:
- Project foundation
- Major use cases (e.g., data platform vendor)
- Academic/research community
- Enterprise users

**Responsibilities:**
- Approve major roadmap changes
- Settle disputes among maintainers
- Review financial/legal decisions
- Represent project interests
- Meet quarterly

## Consensus and Disagreement

### Reaching Consensus

1. **Proposal**: Idea presented in issue with clear rationale
2. **Discussion**: Open for 1-2 weeks (Level 2+), community feedback
3. **Refinement**: Proposal author addresses concerns
4. **Decision**: Maintainer(s) make final call with explanation

### When Maintainers Disagree

- **Minor decision**: Higher-level maintainer breaks tie
- **Major decision**: Escalate to steering committee (or form one)
- **Fundamental disagreement**: Document both viewpoints, decide based on mission alignment

### Appeal Process

If a decision is disagreed with:

1. Request reconsideration with new information/perspective
2. Ask second maintainer to review decision
3. Escalate to steering committee if available
4. As last resort: fork the project (community option, not forbidden)

## Conflict Resolution

### Code of Conduct Violations

See [CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md) for reporting and enforcement.

### Technical Disagreements

1. Document both perspectives in the issue
2. Discuss trade-offs openly (not personally)
3. Defer to domain expert if clear expert status
4. If no expert, defer to maintainer judgment
5. Document decision for future reference

### Governance Disputes

1. Attempt resolution with directly involved parties
2. Escalate to senior maintainer or steering committee
3. Document resolution for precedent
4. Update governance docs if pattern emerges

## Roadmap and Planning

### Release Planning

**Phases**: Major milestones defined in [ROADMAP.md](../ROADMAP.md)

- **Phase 0** (Current): Foundations (licensing, governance, docs)
- **Phase 1**: Core verification engine
- **Phase 2**: Multi-engine support
- **Phase 3**: Contract registry
- **Phase 4**: AI integration

**Version Planning**:
- **0.1.0**: Phase 0 complete (foundation launch)
- **0.2.0 - 0.x.x**: Phase 1 iterations
- **1.0.0**: Phase 1 complete, ready for production
- **2.0.0+**: Later phases

### Roadmap Updates

- Proposed via GitHub issue or discussion
- Public roadmap updated quarterly
- Community input solicited
- Maintainer approval required

### Long-Term Vision

Maintained in [MISSION.md](../MISSION.md). Reviewed annually.

Can be evolved through formal RFC process (Level 4 decision).

## Communication

### Public Channels

- **GitHub Issues**: Bug reports, feature requests
- **GitHub Discussions**: Questions, ideas, community discussion
- **Changelog**: Released changes documented
- **README**: Quick start and feature overview
- **Documentation**: Design and development guidance

### Private Communication

- **Security issues**: Report to maintainers privately (see SECURITY.md)
- **Personal matters**: Email project lead if needed
- **Sensitive discussion**: Maintainer email if public forum inappropriate

### Announcement Format

Major announcements (releases, policy changes):

1. Posted in GitHub Discussions
2. Described in CHANGELOG.md
3. Included in release notes (if version-related)
4. May be tweeted/shared if significant

## Conflict of Interest

### Policy

Maintainers with conflicts of interest in a decision (direct financial gain, employment stake, etc.) should:

1. **Disclose** the conflict
2. **Abstain** from final decision
3. **Participate** in discussion (to share perspective)
4. **Recuse** from vote

Example: Maintainer works for Company X which funds a feature request. They can discuss but should not vote.

## Transparency Reports

**Quarterly** (starting in Phase 1):

- Number of contributors
- PRs merged and average review time
- Issues closed and average resolution time
- Major decisions and rationale
- Security issues reported and fixes
- Community health metrics

Published in GitHub Discussions.

## Amendment Process

This governance document can be amended through:

1. **Typo fixes or clarifications**: 1 maintainer approval
2. **Policy changes or role definitions**: Level 4 decision (see above)
3. **Major restructuring**: Requires RFC and steering committee consensus

Amendments are documented with date and rationale.

## References and Further Reading

- [Contributor Covenant](https://www.contributor-covenant.org/) - Community standards
- [Semantic Versioning](https://semver.org/) - Version numbering
- [OpenGovernance](https://www.open-governance.org/) - Open source governance models
- [Open Source Maintainer Handbook](https://producingoss.com/) - Governance best practices

---

**Last Updated:** 2024-08-20
**Next Review:** Phase 1 launch
**Governance Version:** 1.0
