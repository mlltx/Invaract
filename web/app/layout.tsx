import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Invaract Spark Plugin',
  description: 'Mobile-friendly results viewer for Spark plugin testing',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
