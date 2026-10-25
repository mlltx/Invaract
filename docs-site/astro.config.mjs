// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { remarkBaseLinks } from './remark-base-links.mjs';

// Overridable so CI can build a PR preview at its own sub-path
// (/Invaract/pr-preview/pr-<n>/) without changing content — see
// .github/workflows/deploy-docs.yml and remark-base-links.mjs.
const base = process.env.DOCS_BASE_PATH ?? '/Invaract';

export default defineConfig({
	site: 'https://mlltx.github.io',
	base,
	markdown: {
		remarkPlugins: [[remarkBaseLinks, base]],
	},
	integrations: [
		starlight({
			title: 'Invaract',
			description:
				'Verify Spark and Apache Beam data transformations against machine-readable data contracts — and stop the write before a violation ever lands.',
			logo: {
				light: './src/assets/invaract-mark-light.svg',
				dark: './src/assets/invaract-mark-dark.svg',
				alt: 'Invaract',
			},
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/mlltx/Invaract' },
			],
			editLink: {
				baseUrl: 'https://github.com/mlltx/Invaract/edit/main/docs-site/',
			},
			lastUpdated: true,
			pagination: true,
			favicon: '/favicon.svg',
			sidebar: [
				{
					label: 'Introduction',
					items: [
						{ label: 'What is Invaract?', slug: 'introduction/what-is-this' },
						{ label: 'Why use it?', slug: 'introduction/why-use-it' },
					],
				},
				{
					label: 'Getting Started',
					items: [
						{ label: 'Installation', slug: 'getting-started/installation' },
						{ label: 'Quick Start', slug: 'getting-started/quick-start' },
						{ label: 'Your First Contract', slug: 'getting-started/first-contract' },
					],
				},
				{
					label: 'Guides',
					items: [
						{
							label: 'Install',
							items: [
								{ label: 'Install on Spark', slug: 'guides/installing-the-enforcement-rule' },
								{ label: 'Use Prebuilt Jars Without sbt', slug: 'guides/using-prebuilt-jars-without-sbt' },
								{ label: 'Install and Configure in Code', slug: 'guides/calling-invaract-explicitly' },
								{ label: 'Use with Apache Beam (Java)', slug: 'guides/using-invaract-with-apache-beam' },
								{ label: 'Use with Beam Python Pipelines', slug: 'guides/using-invaract-with-beam-python' },
							],
						},
						{
							label: 'Write Contracts',
							items: [
								{ label: 'Infer a Starting Contract (Dry Run)', slug: 'guides/dry-run-mode' },
								{ label: 'Write a Contract', slug: 'guides/writing-a-contract' },
								{ label: 'Declare Input and Output Types', slug: 'guides/declaring-input-and-output-types' },
								{ label: 'Map Outputs to Their Inputs', slug: 'guides/mapping-outputs-to-inputs' },
								{ label: 'Enforce Transformation Shape Rules', slug: 'guides/enforcing-transformation-rules' },
								{ label: 'Enforce Row-Level DML Rules', slug: 'guides/enforcing-dml-rules' },
								{ label: 'Verify Static Data Quality', slug: 'guides/verifying-static-data-quality' },
								{ label: 'Require Catalog Registration', slug: 'guides/requiring-catalog-registration' },
								{ label: 'Check Contract Compatibility', slug: 'guides/checking-contract-compatibility' },
							],
						},
						{
							label: 'Run at Scale',
							items: [
								{ label: 'Resolve Dataset Locations at Runtime', slug: 'guides/resolving-dataset-locations' },
								{ label: 'Use a Contract Registry', slug: 'guides/using-a-contract-registry' },
								{ label: 'Enforce an Organizational Policy', slug: 'guides/enforcing-organizational-policy' },
								{ label: 'Configure a Notification Sink', slug: 'guides/notification-sinks' },
								{ label: 'Fingerprint a Transformation', slug: 'guides/fingerprinting-transformations' },
							],
						},
						{
							label: 'See It Work',
							items: [
								{ label: 'View Verification Results', slug: 'guides/viewing-results' },
								{ label: 'Prove Enforcement with the Regression Pack', slug: 'guides/running-the-regression-pack' },
							],
						},
					],
				},
				{
					label: 'Concepts',
					items: [
						{ label: 'Data Contracts', slug: 'concepts/data-contracts' },
						{ label: 'The Transformation IR', slug: 'concepts/transformation-ir' },
						{
							label: 'Verification vs. Enforcement',
							slug: 'concepts/verification-vs-enforcement',
						},
						{ label: 'Fail-Closed by Default', slug: 'concepts/fail-closed' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'What Each Engine Checks', slug: 'reference/what-each-engine-checks' },
						{ label: 'Contract Format', slug: 'reference/contract-format' },
						{ label: 'Configuration Settings', slug: 'reference/configuration' },
						{ label: 'Connector Support', slug: 'reference/connector-support' },
						{ label: 'Engine Version Support', slug: 'reference/spark-version-support' },
						{ label: 'Engine Capabilities', slug: 'reference/engine-capabilities' },
						{ label: 'Violation Types', slug: 'reference/violation-types' },
						{
							label: 'Organizational Policy Format',
							slug: 'reference/org-policy-format',
						},
						{ label: 'Dev Commands', slug: 'reference/dev-commands' },
					],
				},
				{
					label: 'Troubleshooting',
					items: [
						{ label: 'Common Problems', slug: 'troubleshooting/common-problems' },
						{ label: 'FAQ', slug: 'troubleshooting/faq' },
					],
				},
			],
		}),
	],
});
