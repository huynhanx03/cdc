import type { Metadata } from 'next'

import './globals.css'

export const metadata: Metadata = {
  title: 'CDC UI Platform',
  description: 'Track topics, partitions, and change data capture flows.',
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
