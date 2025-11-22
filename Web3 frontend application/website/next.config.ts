import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: false, 
  output: "standalone",
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: '**', 
      },
    ],
  },
   
    experimental: {
        serverActions: {
        bodySizeLimit: '20mb',
        },
    },

    serverRuntimeConfig: {
        maxRequestBodySize: 10 * 1024 * 1024,
    },



};

export default nextConfig;
