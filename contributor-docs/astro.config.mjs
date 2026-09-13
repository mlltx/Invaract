// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { remarkBaseLinks } from './remark-base-links.mjs';

// Overridable for the same reason docs-site/astro.config.mjs's DOCS_BASE_PATH
// is: so a PR-preview build (see .github/workflows/preview-contributor-docs.yml)
// can deploy this site to its own sub-path without changing content.
// Deployed in production as a subfolder of the same GitHub Pages site
// docs-site/ uses (see .github/workflows/deploy-contributor-docs.yml), not
// a separate Pages site.
const base = process.env.CONTRIBUTOR_DOCS_BASE_PATH ?? '/Invaract/contributor';

export default defineConfig({
	site: 'https://mlltx.github.io',
	base,
	markdown: {
		remarkPlugins: [[remarkBaseLinks, base]],
	},
	integrations: [
		starlight({
			title: 'Invaract (Contributor Docs)',
			description:
				"Roadmap, architecture, and per-module design docs for people building Invaract itself — not Invaract's user documentation.",
			logo: {
				light: './src/assets/invaract-mark-light.svg',
				dark: './src/assets/invaract-mark-dark.svg',
				alt: 'Invaract',
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/mlltx/Invaract' },
			],
			// Only correct for hand-authored pages (currently just index.mdx) —
			// every generated page (see scripts/sync-docs.mjs) overrides this via
			// its own frontmatter `editUrl`, pointing at its real source file
			// instead of the generated, gitignored copy under src/content/docs/.
			editLink: {
				baseUrl: 'https://github.com/mlltx/Invaract/edit/main/contributor-docs/',
			},
			lastUpdated: true,
			pagination: true,
			favicon: '/favicon.svg',
			sidebar: [
				{
					label: 'Project',
					items: [
						{ label: 'Roadmap', slug: 'project/roadmap' },
						{ label: 'Architecture', slug: 'project/architecture' },
					],
				},
				{
					label: 'Design Docs',
					items: [
						{ label: 'Contract Model', slug: 'design/contract-model' },
						{ label: 'Transformation IR', slug: 'design/transformation-ir' },
						{ label: 'Spark Adapter', slug: 'design/spark-adapter' },
						{
							label: 'Semantic Lineage Fingerprinting',
							slug: 'design/semantic-lineage-fingerprinting',
						},
						{ label: 'Adding a Spark Connector', slug: 'design/adding-a-spark-connector' },
					],
				},
				{
					label: 'Connectors',
					items: [
						{ label: 'Delta Lake', slug: 'connectors/delta' },
						{ label: 'Iceberg', slug: 'connectors/iceberg' },
						{ label: 'Hive', slug: 'connectors/hive' },
						{ label: 'Parquet', slug: 'connectors/parquet' },
						{ label: 'Avro', slug: 'connectors/avro' },
						{ label: 'CSV', slug: 'connectors/csv' },
						{ label: 'ClickHouse', slug: 'connectors/clickhouse' },
					],
				},
				{
					label: 'Process',
					items: [
						{ label: 'Versioning', slug: 'process/versioning' },
						{ label: 'Compatibility', slug: 'process/compatibility' },
						{ label: 'Releasing', slug: 'process/releasing' },
						{ label: 'Governance', slug: 'process/governance' },
						{ label: 'CVE Remediation', slug: 'process/cve-remediation' },
					],
				},
			],
		}),
	],
});
