import type { Metadata } from "next"
import { JetBrains_Mono, Space_Grotesk } from "next/font/google"
import "./globals.css"

const sans = Space_Grotesk({
  variable: "--font-app-sans",
  subsets: ["latin"],
})

const mono = JetBrains_Mono({
  variable: "--font-app-mono",
  subsets: ["latin"],
})

export const metadata: Metadata = {
  title: "raft visualizer",
  description: "watch raft elect leaders and replicate logs on a simulated network",
}

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${sans.variable} ${mono.variable}`}>{children}</body>
    </html>
  )
}
