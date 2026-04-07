import type { MetadataRoute } from "next";

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      // Default: allow all public pages, block internal and API routes
      {
        userAgent: "*",
        allow: "/",
        disallow: [
          "/api/",
          "/admin",
        ],
      },
      // Block GPTBot (OpenAI training crawler) from all content
      {
        userAgent: "GPTBot",
        disallow: "/",
      },
      // Explicitly allow OAI-SearchBot (OpenAI search — retrieval, not training)
      {
        userAgent: "OAI-SearchBot",
        allow: "/",
        disallow: ["/api/", "/admin"],
      },
      // Allow Anthropic's crawler (retrieval / product context)
      {
        userAgent: "ClaudeBot",
        allow: "/",
        disallow: ["/api/", "/admin"],
      },
    ],
    sitemap: "https://selun.sagitta.systems/sitemap.xml",
  };
}
