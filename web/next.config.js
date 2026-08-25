/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  pageExtensions: ['ts', 'tsx'],
  // This repo already has one authoritative CLAUDE.md at the repo root
  // (see /CLAUDE.md); `next dev`'s default agentRules would otherwise
  // scaffold and keep re-generating a second, competing web/CLAUDE.md.
  agentRules: false,
}

module.exports = nextConfig
