import './globals.css'

export const metadata = {
  title: 'Strata ICP Screener v2',
  description: 'Research and score B2B SaaS companies for narrative gap severity',
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body className="bg-gray-50 text-gray-900">{children}</body>
    </html>
  )
}
