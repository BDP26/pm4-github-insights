import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  async rewrites() {
    const api = process.env.API_URL ?? "http://api:8000";
    return [
      { source: "/api/:path*",    destination: `${api}/api/:path*` },
      { source: "/stream/:path*", destination: `${api}/stream/:path*` },
    ];
  },
};

export default nextConfig;
