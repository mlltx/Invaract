import { readFileSync } from 'fs'
import { join } from 'path'
import { NextResponse } from 'next/server'

export async function GET() {
  try {
    const reportPath = join(process.cwd(), '..', '..', 'demo', 'output', 'report.json')
    const reportContent = readFileSync(reportPath, 'utf-8')
    const report = JSON.parse(reportContent)
    return NextResponse.json(report)
  } catch (error) {
    return NextResponse.json(
      { error: 'Report not found. Run ./dev/test first.' },
      { status: 404 }
    )
  }
}
