import type { Metadata } from "next";
import { Manrope, Space_Grotesk } from "next/font/google";
import { Analytics } from "@vercel/analytics/react";
import "./globals.css";

const displayFont = Space_Grotesk({
  variable: "--font-display",
  subsets: ["latin"],
  weight: ["500", "600", "700"],
});

const bodyFont = Manrope({
  variable: "--font-body",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
});

const siteUrl = process.env.NEXT_PUBLIC_SITE_URL?.trim() || "https://selun.sagitta.systems";
const siteTitle = "Selun | Crypto Portfolio Plan";
const siteDescription =
  "Payment-gated x402 endpoints for Sagitta AAA portfolio intelligence and SCE execution preflight. Includes allocation, market regime, policy envelope, asset scoring, rebalance, continuity mode, case relevance, and doctrine-based risk evaluation for autonomous agents.";

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: siteTitle,
  description: siteDescription,
  openGraph: {
    type: "website",
    url: "/",
    siteName: "Selun",
    title: siteTitle,
    description: siteDescription,
    images: [
      {
        url: "/opengraph-image",
        width: 1200,
        height: 630,
        alt: "Selun - Sagitta AAA Portfolio Infrastructure",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: siteTitle,
    description: siteDescription,
    images: ["/twitter-image"],
  },
  icons: {
    icon: [{ url: "/selun-mark.svg?v=2", type: "image/svg+xml" }],
    shortcut: [{ url: "/selun-mark.svg?v=2", type: "image/svg+xml" }],
    apple: [{ url: "/selun-mark.svg?v=2", type: "image/svg+xml" }],
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${displayFont.variable} ${bodyFont.variable}`}>
      <body>
        {children}
        <Analytics />
      </body>
    </html>
  );
}
