import type { Metadata } from "next";
import { Manrope, Space_Grotesk } from "next/font/google";
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

export const metadata: Metadata = {
  title: "Selun | Autonomous Portfolio Agent",
  description:
    "Selun is an autonomous agent built on top of the AAA API to help retail investors make calmer, structured portfolio decisions.",
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
      <body>{children}</body>
    </html>
  );
}
