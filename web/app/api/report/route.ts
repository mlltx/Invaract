import { readFileSync } from 'fs'
import { join } from 'path'

export async function GET() {
  try {
    const reportPath = join(process.cwd(), '../../demo/output/report.json')
    const content = readFileSync(reportPath, 'utf-8')
    const report = JSON.parse(content)
    return Response.json(report)
  } catch (error) {
    return Response.json(
      { error: 'Report not found. Run ./dev/test to generate one.' },
      { status: 404 }
    )
  }
}
