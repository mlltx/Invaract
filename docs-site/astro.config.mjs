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
				'Verify Spark data transformations against machine-readable data contracts — and abort the write before a violation ever lands.',
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
						{ label: 'Write a Contract', slug: 'guides/writing-a-contract' },
						{
							label: 'Install the Enforcement Rule',
							slug: 'guides/installing-the-enforcement-rule',
						},
						{ label: 'Enforce Row-Level DML Rules', slug: 'guides/enforcing-dml-rules' },
						{ label: 'View Verification Results', slug: 'guides/viewing-results' },
						{
							label: 'Prove Enforcement with the Regression Pack',
							slug: 'guides/running-the-regression-pack',
						},
						{
							label: 'Check Contract Compatibility',
							slug: 'guides/checking-contract-compatibility',
						},
						{ label: 'Configure a Notification Sink', slug: 'guides/notification-sinks' },
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
						{ label: 'Contract Format', slug: 'reference/contract-format' },
						{ label: 'Connector Support', slug: 'reference/connector-support' },
						{ label: 'Spark Version Support', slug: 'reference/spark-version-support' },
						{ label: 'Violation Types', slug: 'reference/violation-types' },
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
