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

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  title: "Selun | Autonomous Portfolio Agent",
  description:
    "Selun is an autonomous agent built on top of the AAA API to help retail investors make calmer, structured portfolio decisions.",
  openGraph: {
    type: "website",
    url: "/",
    siteName: "Selun",
    title: "Selun | Autonomous Portfolio Agent",
    description:
      "Simple crypto allocation powered by market intelligence.",
    images: [
      {
        url: "/opengraph-image",
        width: 1200,
        height: 630,
        alt: "Selun - Autonomous Portfolio Agent",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Selun | Autonomous Portfolio Agent",
    description:
      "Simple crypto allocation powered by market intelligence.",
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
