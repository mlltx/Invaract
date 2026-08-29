# Security Policy

## Reporting Security Vulnerabilities

**Do not open public GitHub issues for security vulnerabilities.**

If you discover a security vulnerability in Invaract, please report it responsibly to the project maintainers.

### How to Report

1. **Email**: Contact maintainers directly
   - Subject: `SECURITY: [Brief description]`
   - Include: Vulnerability details, affected versions, reproduction steps, proposed fix (if any)

2. **GitHub Security Advisory** (Preferred for GitHub-hosted projects):
   - Go to repository → Security tab → Advisories
   - Click "Report a vulnerability"
   - Fill in vulnerability details privately

### What Happens Next

- Maintainers acknowledge receipt within 48 hours
- Initial assessment within 5 business days
- Timeline for fix depends on severity
- Coordinated disclosure: fix released before public announcement
- Credit offered to reporter (unless confidentiality requested)

## Supported Versions

| Version | Status | Security Updates |
|---------|--------|------------------|
| 0.1.x   | Active | Yes (current) |
| < 0.1   | N/A    | N/A |

- **Active**: Receives security updates and bug fixes
- **LTS** (planned in future): Extended support for 2+ years
- **End-of-life**: No further updates

See [ROADMAP.md](ROADMAP.md) for release schedule.

## Security Best Practices

### For Developers

#### Code Review

- All changes reviewed before merge
- Security-sensitive areas (cryptography, auth, data handling) reviewed by multiple maintainers
- No single-person approval for security-related changes

#### Dependency Management

- Lock all dependencies to known versions (see `build.sbt`, `package.json`)
- Regular dependency audits via:
  - `sbt dependencyUpdates` (Scala)
  - `npm audit` (Node.js)
- Prefer security updates over feature delays
- Monitor for CVEs in dependencies

#### Code Quality

- No hardcoded secrets (passwords, API keys, tokens)
- No eval() or dynamic code execution
- Input validation at system boundaries (CLI args, file paths)
- Safe defaults (prefer fail-safe over fail-open)

#### Testing

- Unit tests for security-sensitive functions
- Integration tests exercise error paths
- Fuzz testing for parser inputs (future phase)

### For Users/Operators

#### Deployment Security

- Run plugin in trusted Spark cluster only
- Use latest Spark version (security patches)
- Keep JDK 21 updated for latest security patches
- Run with minimal required JVM permissions (if using Java security manager)

#### Data Handling

- Contracts may contain sensitive business logic or schema details
- Restrict contract repository access as needed
- Verified lineage reports may reveal data flows (handle with appropriate confidentiality)

#### Dependency Audit

Check for known vulnerabilities:

```bash
# For Scala dependencies
cd plugin && sbt dependencyUpdates && cd ..
cd runner && sbt dependencyUpdates && cd ..

# For Node.js dependencies
cd web && npm audit && npm audit fix && cd ..
```

## Vulnerability Disclosure Timeline

### Critical Severity (CVSS 9.0-10.0)

- Example: Remote code execution, complete data breach
- **Patch released:** Within 1 week
- **Public disclosure:** Immediately after patch release
- **Deprecation:** Affected version marked as security-critical

### High Severity (CVSS 7.0-8.9)

- Example: Unauthorized data access, denial of service
- **Patch released:** Within 2 weeks
- **Public disclosure:** With patch release
- **Deprecation:** Users encouraged to upgrade

### Medium Severity (CVSS 4.0-6.9)

- Example: Limited data exposure, privilege escalation
- **Patch released:** Within 4 weeks
- **Public disclosure:** With patch release
- **Deprecation:** Standard maintenance cycle

### Low Severity (CVSS 0.1-3.9)

- Example: Information disclosure, minor logic flaw
- **Patch released:** Next scheduled release
- **Public disclosure:** With release notes
- **Deprecation:** No special action required

## Secure Coding Guidelines

### Input Validation

```scala
// ✓ Good: Validate input before processing
def processInput(path: String): DataFrame = {
  require(path.nonEmpty, "Path cannot be empty")
  require(!path.contains(".."), "Path traversal not allowed")
  spark.read.csv(path)
}

// ✗ Bad: No validation
def processInput(path: String): DataFrame = {
  spark.read.csv(path)  // Could read any file
}
```

### Error Handling

```scala
// ✓ Good: Specific error messages (don't leak internals)
catch {
  case e: IllegalArgumentException =>
    logEvent(s"Validation failed: ${e.getMessage}")
    throw new RuntimeException("Input validation failed", e)
}

// ✗ Bad: Expose stack traces or debug info to users
catch {
  case e: Exception =>
    e.printStackTrace()  // Don't expose internals
    throw e
}
```

### Dependency Versions

```scala
// ✓ Good: Explicit versions, security patches included
"org.apache.spark" %% "spark-sql" % "3.5.1"  // Latest patch

// ✗ Bad: Open-ended versions
"org.apache.spark" %% "spark-sql" % "3.5+"  // Could pull insecure patch
```

## Known Limitations

### Phase 0 (Current)

- **No authentication/authorization** in web UI (assumes local/trusted network)
- **No encryption** for data at rest or in transit
- **No audit logging** of who accessed what
- **Local Spark only** (no remote cluster security)

### Phase 1+ (Future)

- Will add role-based access control (RBAC) for contract registry
- Will support TLS/HTTPS for data in transit
- Will implement audit logging for compliance
- Will support Kerberos/LDAP for cluster authentication

## Security Incident Response

If a security incident is discovered:

1. **Notification**: Maintainers notified immediately
2. **Assessment**: Determine scope and impact
3. **Triage**: Assign severity level
4. **Response**: Develop and test fix
5. **Release**: Push patch release with fix
6. **Communication**: Public disclosure (CVE if applicable)
7. **Post-mortem**: Review how incident happened, prevent recurrence

## Compliance and Standards

### Planned Compliance (Future Phases)

- **Open Source**: Apache 2.0 licensed, code publicly reviewed
- **Dependency Transparency**: Build manifest includes all dependencies
- **Vulnerability Reporting**: Public CVE tracking
- **Security Updates**: Timely patch releases for critical issues

### Related Standards

- [OWASP Top 10](https://owasp.org/www-project-top-ten/) – Web application security
- [CWE Top 25](https://cwe.mitre.org/top25/) – Most dangerous software weaknesses
- [CVSS v3.1](https://www.first.org/cvss/v3.1/specification-document) – Vulnerability scoring

## Security Audit

Invaract has not undergone formal third-party security audit. Security audit is planned for later phases (Phase 1 or 2) as the codebase matures.

To request security review:
- Open issue on GitHub: "Security Review Request"
- Provide context (integration points, threat model, compliance needs)

## Further Reading

- [OWASP Secure Coding Practices](https://owasp.org/www-community/attacks/)
- [Scala Security Guidelines](https://docs.scala-lang.org/)
- [Spark Security Documentation](https://spark.apache.org/docs/latest/security.html)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)

## Questions?

For security questions (non-disclosure), open a GitHub Discussion with tag "security".

---

**Last Updated:** 2024-08-20
**Policy Version:** 1.0
