'use client'

import { useEffect, useState } from 'react'
import styles from './page.module.css'

interface Report {
  status: string
  timestamp: string
  pluginVersion: string
  sparkVersion: string
  scalaVersion: string
  javaVersion: string
  durationMs: number
  buildInfo: Record<string, string>
  tests: {
    unit?: { passed: number; failed: number }
    integration?: { passed: number; failed: number }
  }
  input: {
    rowCount?: number
    schema?: Array<{ name: string; type: string }>
  }
  output: {
    rowCount?: number
    schema?: Array<{ name: string; type: string }>
    sample?: Array<Record<string, any>>
  }
  plugin: {
    events?: string[]
    diagnostics?: string[]
  }
  transformationIR?: {
    captured?: boolean
    note?: string
    renderedPlan?: string
    lineage?: Array<{ output: string; sources: string[]; aggregated: boolean }>
    diagnostics?: string[]
  }
  contractVerification?: {
    status?: string
    contract?: string
    contractPath?: string
    explanation?: string
    inferredContractYaml?: string
    violations?: Array<{
      type: string
      message: string
      remediation: string
      column?: string
      location?: string
      expected?: string
      actual?: string
    }>
  }
  error?: string
}

const ReportViewer = () => {
  const [report, setReport] = useState<Report | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchReport = async () => {
      try {
        const response = await fetch('/api/report')
        if (!response.ok) {
          throw new Error('Report not found')
        }
        const data = await response.json()
        setReport(data)
        setError(null)
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load report')
        setReport(null)
      } finally {
        setLoading(false)
      }
    }

    fetchReport()
    const interval = setInterval(fetchReport, 2000)
    return () => clearInterval(interval)
  }, [])

  if (loading) {
    return (
      <div className={styles.container}>
        <div className={styles.loading}>Loading report...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className={styles.container}>
        <div className={styles.noReport}>
          <h2>No Report Available</h2>
          <p>{error}</p>
          <p className={styles.hint}>Run <code>./dev/test</code> to generate a report</p>
        </div>
      </div>
    )
  }

  if (!report) {
    return null
  }

  const isPass = report.status === 'PASS'
  const statusIcon = isPass ? '✓' : '✕'
  const statusClass = isPass ? styles.pass : styles.fail

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <h1>Invaract Plugin Report</h1>
      </header>

      <section className={`${styles.section} ${styles.status}`}>
        <div className={`${styles.statusBadge} ${statusClass}`}>
          <span className={styles.icon}>{statusIcon}</span>
          <span className={styles.text}>{report.status}</span>
        </div>
        <div className={styles.timestamp}>
          {new Date(report.timestamp).toLocaleString()}
        </div>
      </section>

      <section className={styles.section}>
        <h2>Build Information</h2>
        <div className={styles.grid}>
          <div className={styles.item}>
            <label>Plugin Version</label>
            <span>{report.pluginVersion}</span>
          </div>
          <div className={styles.item}>
            <label>Spark Version</label>
            <span>{report.sparkVersion.split(' ').pop()}</span>
          </div>
          <div className={styles.item}>
            <label>Java Version</label>
            <span>{report.javaVersion}</span>
          </div>
          <div className={styles.item}>
            <label>Build Duration</label>
            <span>{report.durationMs}ms</span>
          </div>
        </div>
      </section>

      {report.tests && (
        <section className={styles.section}>
          <h2>Test Results</h2>
          <div className={styles.testResults}>
            {report.tests.unit && (
              <div className={styles.testSuite}>
                <div className={styles.testName}>Unit Tests</div>
                <div className={styles.testStatus}>
                  {report.tests.unit.passed}/{report.tests.unit.passed + report.tests.unit.failed} ✓
                </div>
              </div>
            )}
            {report.tests.integration && (
              <div className={styles.testSuite}>
                <div className={styles.testName}>Integration Tests</div>
                <div className={styles.testStatus}>
                  {report.tests.integration.passed}/{report.tests.integration.passed + report.tests.integration.failed} ✓
                </div>
              </div>
            )}
          </div>
        </section>
      )}

      <section className={styles.section}>
        <h2>Input Data</h2>
        <div className={styles.dataInfo}>
          <div className={styles.item}>
            <label>Row Count</label>
            <span>{report.input.rowCount || '-'}</span>
          </div>
          {report.input.schema && (
            <div className={styles.schemaBox}>
              <label>Schema</label>
              <table className={styles.schemaTable}>
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                  </tr>
                </thead>
                <tbody>
                  {report.input.schema.map((field, i) => (
                    <tr key={i}>
                      <td>{field.name}</td>
                      <td>{field.type}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>

      <section className={styles.section}>
        <h2>Output Data</h2>
        <div className={styles.dataInfo}>
          <div className={styles.item}>
            <label>Row Count</label>
            <span>{report.output.rowCount || '-'}</span>
          </div>
          {report.output.schema && (
            <div className={styles.schemaBox}>
              <label>Schema</label>
              <table className={styles.schemaTable}>
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                  </tr>
                </thead>
                <tbody>
                  {report.output.schema.map((field, i) => (
                    <tr key={i}>
                      <td>{field.name}</td>
                      <td>{field.type}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {report.output.sample && (
            <div className={styles.sampleBox}>
              <label>Sample Rows (first 5)</label>
              <div className={styles.sampleRows}>
                {report.output.sample.map((row, i) => (
                  <div key={i} className={styles.row}>
                    <div className={styles.rowNum}>Row {i + 1}</div>
                    <div className={styles.rowData}>
                      {Object.entries(row).map(([key, val]) => (
                        <div key={key} className={styles.field}>
                          <span className={styles.fieldName}>{key}:</span>
                          <span className={styles.fieldValue}>{String(val)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </section>

      {report.transformationIR && (
        <section className={styles.section}>
          <h2>Transformation IR</h2>
          {report.transformationIR.renderedPlan ? (
            <div className={styles.irInfo}>
              <div className={styles.planBox}>
                <label>Translated Plan</label>
                <pre className={styles.planPre}>{report.transformationIR.renderedPlan}</pre>
              </div>

              {report.transformationIR.lineage && report.transformationIR.lineage.length > 0 && (
                <div className={styles.lineageBox}>
                  <label>Column Lineage</label>
                  <div className={styles.lineageList}>
                    {report.transformationIR.lineage.map((col, i) => (
                      <div key={i} className={styles.lineageRow}>
                        <div className={styles.lineageOutput}>
                          {col.output}
                          {col.aggregated && <span className={styles.aggregatedBadge}>aggregated</span>}
                        </div>
                        <div className={styles.lineageSources}>
                          {col.sources.length > 0 ? col.sources.join(', ') : '(no known source)'}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {report.transformationIR.diagnostics && report.transformationIR.diagnostics.length > 0 && (
                <div className={styles.diagnosticsBox}>
                  <label>Diagnostics</label>
                  <div className={styles.diagnosticsList}>
                    {report.transformationIR.diagnostics.map((d, i) => (
                      <div key={i} className={styles.diagnostic}>{d}</div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <p className={styles.hint}>
              {report.transformationIR.note || 'Transformation IR was not captured for this run.'}
            </p>
          )}
        </section>
      )}

      {report.contractVerification && Object.keys(report.contractVerification).length > 0 && (
        <section className={styles.section}>
          <h2>Contract Verification</h2>
          {(() => {
            const cv = report.contractVerification!
            const cvStatus = cv.status || 'UNKNOWN'
            const cvClass =
              cvStatus === 'PASSED' ? styles.pass : cvStatus === 'FAILED' ? styles.fail : styles.neutral
            return (
              <div className={styles.contractInfo}>
                <div className={styles.contractHeader}>
                  <div className={`${styles.statusBadge} ${styles.statusBadgeSmall} ${cvClass}`}>
                    <span className={styles.text}>{cvStatus}</span>
                  </div>
                  {cv.contract && <div className={styles.contractId}>{cv.contract}</div>}
                </div>
                {cv.contractPath && (
                  <div className={styles.contractPath}>contract file: {cv.contractPath}</div>
                )}

                {cvStatus === 'DRY_RUN' ? (
                  cv.inferredContractYaml ? (
                    <div className={styles.planBox}>
                      <label>Inferred Contract (from this run&apos;s actual inputs/outputs)</label>
                      <pre className={styles.planPre}>{cv.inferredContractYaml}</pre>
                    </div>
                  ) : (
                    <p className={styles.hint}>No write was recognized during this run, so no contract could be inferred.</p>
                  )
                ) : cv.violations && cv.violations.length > 0 ? (
                  <div className={styles.violationsList}>
                    {cv.violations.map((v, i) => (
                      <div key={i} className={styles.violation}>
                        <div className={styles.violationType}>{v.type}</div>
                        <div className={styles.violationMessage}>{v.message}</div>
                        {(v.column || v.location || v.expected || v.actual) && (
                          <div className={styles.violationDetail}>
                            {v.column && <span>column: {v.column}</span>}
                            {v.location && <span>location: {v.location}</span>}
                            {v.expected && <span>expected: {v.expected}</span>}
                            {v.actual && <span>actual: {v.actual}</span>}
                          </div>
                        )}
                        <div className={styles.violationRemediation}>→ {v.remediation}</div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className={styles.hint}>No violations.</p>
                )}
              </div>
            )
          })()}
        </section>
      )}

      {report.plugin?.events && report.plugin.events.length > 0 && (
        <section className={styles.section}>
          <h2>Plugin Events</h2>
          <div className={styles.eventsList}>
            {report.plugin.events.map((event, i) => (
              <div key={i} className={styles.event}>
                {event}
              </div>
            ))}
          </div>
        </section>
      )}

      {report.error && (
        <section className={`${styles.section} ${styles.errorBox}`}>
          <h2>Error</h2>
          <pre>{report.error}</pre>
        </section>
      )}
    </div>
  )
}

export default ReportViewer
