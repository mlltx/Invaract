#!/usr/bin/env node
// Regenerates src/content/docs/{project,design,connectors,process}/**
// from the repository's own source-of-truth developer docs (ROADMAP.md,
// ARCHITECTURE.md, docs/**). This is the *only* place that content is
// produced — nobody should hand-edit a generated page, since the next
// `npm run build`/`npm run dev` overwrites it. Edit the source file in the
// repo root or under docs/ instead, then re-run this script (it also runs
// automatically via the `predev`/`prebuild` npm scripts).
//
// This keeps the contributor site from drifting out of sync with the real
// docs the way a hand-copied Markdown page would: there is exactly one
// place each piece of content lives, and this script is a mechanical,
// re-runnable projection of it into Starlight's content-collection shape
// (frontmatter + rewritten relative links), not a second copy to maintain.
import { mkdirSync, readFileSync, writeFileSync, rmSync, existsSync } from 'node:fs';
import { dirname, join, posix, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, '..', '..');
const outRoot = join(__dirname, '..', 'src', 'content', 'docs');

const GITHUB_BLOB_BASE = 'https://github.com/mlltx/Invaract/blob/main';

// source path (repo-root-relative) -> { slug, section, label, order }
const PAGES = [
  { src: 'ROADMAP.md', slug: 'project/roadmap', section: 'Project', label: 'Roadmap', order: 1 },
  {
    src: 'ARCHITECTURE.md',
    slug: 'project/architecture',
    section: 'Project',
    label: 'Architecture',
    order: 2,
  },
  {
    src: 'docs/CONTRACT_MODEL.md',
    slug: 'design/contract-model',
    section: 'Design Docs',
    label: 'Contract Model',
    order: 1,
  },
  {
    src: 'docs/TRANSFORMATION_IR.md',
    slug: 'design/transformation-ir',
    section: 'Design Docs',
    label: 'Transformation IR',
    order: 2,
  },
  {
    src: 'docs/SPARK_ADAPTER.md',
    slug: 'design/spark-adapter',
    section: 'Design Docs',
    label: 'Spark Adapter',
    order: 3,
  },
  {
    src: 'docs/SEMANTIC_LINEAGE_FINGERPRINTING.md',
    slug: 'design/semantic-lineage-fingerprinting',
    section: 'Design Docs',
    label: 'Semantic Lineage Fingerprinting',
    order: 4,
  },
  {
    src: 'docs/ADDING_A_SPARK_CONNECTOR.md',
    slug: 'design/adding-a-spark-connector',
    section: 'Design Docs',
    label: 'Adding a Spark Connector',
    order: 5,
  },
  {
    src: 'docs/connectors/delta.md',
    slug: 'connectors/delta',
    section: 'Connectors',
    label: 'Delta Lake',
    order: 1,
  },
  {
    src: 'docs/connectors/iceberg.md',
    slug: 'connectors/iceberg',
    section: 'Connectors',
    label: 'Iceberg',
    order: 2,
  },
  {
    src: 'docs/connectors/hive.md',
    slug: 'connectors/hive',
    section: 'Connectors',
    label: 'Hive',
    order: 3,
  },
  {
    src: 'docs/connectors/parquet.md',
    slug: 'connectors/parquet',
    section: 'Connectors',
    label: 'Parquet',
    order: 4,
  },
  {
    src: 'docs/connectors/avro.md',
    slug: 'connectors/avro',
    section: 'Connectors',
    label: 'Avro',
    order: 5,
  },
  {
    src: 'docs/connectors/csv.md',
    slug: 'connectors/csv',
    section: 'Connectors',
    label: 'CSV',
    order: 6,
  },
  {
    src: 'docs/connectors/clickhouse.md',
    slug: 'connectors/clickhouse',
    section: 'Connectors',
    label: 'ClickHouse',
    order: 7,
  },
  {
    src: 'docs/VERSIONING.md',
    slug: 'process/versioning',
    section: 'Process',
    label: 'Versioning',
    order: 1,
  },
  {
    src: 'docs/COMPATIBILITY.md',
    slug: 'process/compatibility',
    section: 'Process',
    label: 'Compatibility',
    order: 2,
  },
  {
    src: 'docs/RELEASING.md',
    slug: 'process/releasing',
    section: 'Process',
    label: 'Releasing',
    order: 3,
  },
  {
    src: 'docs/GOVERNANCE.md',
    slug: 'process/governance',
    section: 'Process',
    label: 'Governance',
    order: 4,
  },
  {
    src: 'docs/CVE_REMEDIATION.md',
    slug: 'process/cve-remediation',
    section: 'Process',
    label: 'CVE Remediation',
    order: 5,
  },
];

// Every relative-link target across these files that resolves to one of the
// above gets rewritten to that page's in-site slug. Built from PAGES so the
// two can never drift apart. Keyed by every spelling a source file might
// plausibly use to refer to it (bare filename, docs/-prefixed, etc).
const slugByPath = new Map();
for (const page of PAGES) {
  slugByPath.set(page.src, page.slug);
  slugByPath.set(page.src.replace(/^docs\//, ''), page.slug);
  slugByPath.set('./' + page.src, page.slug);
}

function stripFrontH1(body) {
  const lines = body.split('\n');
  let i = 0;
  while (i < lines.length && lines[i].trim() === '') i++;
  let title = null;
  if (lines[i] && lines[i].startsWith('# ')) {
    title = lines[i].slice(2).trim();
    i++;
    while (i < lines.length && lines[i].trim() === '') i++;
    return { title, rest: lines.slice(i).join('\n') };
  }
  return { title: null, rest: body };
}

function firstParagraph(body) {
  const paragraphs = body.split(/\n\s*\n/);
  for (const p of paragraphs) {
    const trimmed = p.trim();
    if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('```')) continue;
    const plain = trimmed
      .replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
      .replace(/[`*_]/g, '')
      .replace(/\s+/g, ' ')
      .trim();
    return plain.length > 155 ? plain.slice(0, 152) + '...' : plain;
  }
  return '';
}

function yamlString(value) {
  return `'${value.replace(/'/g, "''")}'`;
}

// Rewrites every Markdown link target in `body`, which originally lived at
// repo-root-relative `sourcePath`: a target matching another page in PAGES
// becomes that page's site-relative slug (kept root-relative; Starlight's
// own base-path handling, mirrored by ./remark-base-links.mjs the same way
// docs-site does it, prefixes it at build time), and anything else that
// isn't already absolute/anchor/mailto is rewritten to a real GitHub blob
// URL so it keeps working from a page that no longer lives at that path.
function rewriteLinks(body, sourcePath) {
  const sourceDir = posix.dirname(sourcePath);
  return body.replace(/(!?\[[^\]]*\]\()([^)\s]+)(\s*(?:"[^"]*")?\))/g, (match, pre, target, post) => {
    if (/^(?:[a-z][a-z0-9+.-]*:|#|\/\/)/i.test(target)) return match; // absolute/protocol/anchor-only
    const [rawPath, anchor] = target.split('#');
    if (!rawPath) return match; // pure "#anchor" within the same page
    const resolved = posix.normalize(posix.join(sourceDir, rawPath));
    const slug = slugByPath.get(resolved) ?? slugByPath.get(rawPath);
    if (slug) {
      return `${pre}/${slug}/${anchor ? '#' + anchor : ''}${post}`;
    }
    const isDir = rawPath.endsWith('/');
    const base = isDir ? GITHUB_BLOB_BASE.replace('/blob/', '/tree/') : GITHUB_BLOB_BASE;
    const ghUrl = `${base}/${resolved}${anchor ? '#' + anchor : ''}`;
    return `${pre}${ghUrl}${post}`;
  });
}

// Only clear the section directories this script itself owns (project/,
// design/, connectors/, process/ — derived from PAGES, not hardcoded, so a
// new section added there is cleared too) — never the whole outRoot, which
// also holds index.mdx, the one hand-authored page in this collection.
const ownedDirs = new Set(PAGES.map((page) => page.slug.split('/')[0]));
for (const dir of ownedDirs) {
  const dirPath = join(outRoot, dir);
  if (existsSync(dirPath)) {
    rmSync(dirPath, { recursive: true, force: true });
  }
}
mkdirSync(outRoot, { recursive: true });

for (const page of PAGES) {
  const absSrc = join(repoRoot, page.src);
  const raw = readFileSync(absSrc, 'utf8');
  const { title, rest } = stripFrontH1(raw);
  const description = firstParagraph(rest) || `Contributor documentation: ${page.label}.`;
  const rewritten = rewriteLinks(rest, page.src);
  const frontmatter = [
    '---',
    `title: ${yamlString(title ?? page.label)}`,
    `description: ${yamlString(description)}`,
    `editUrl: ${yamlString(`${GITHUB_BLOB_BASE.replace('/blob/', '/edit/')}/${page.src}`)}`,
    `sidebar:`,
    `  order: ${page.order}`,
    '---',
    '',
    `<!-- GENERATED FILE — do not edit. Source: ${page.src}. Regenerate with`,
    `     \`npm run sync\` in contributor-docs/, or just run the build/dev`,
    `     scripts, which do it automatically. -->`,
    '',
  ].join('\n');
  const outPath = join(outRoot, ...page.slug.split('/')) + '.md';
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, frontmatter + rewritten.trimStart() + '\n');
}

console.log(`Synced ${PAGES.length} contributor-docs pages from repository sources.`);
