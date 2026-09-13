import { visit } from 'unist-util-visit';

/**
 * Prepends the site's configured base path to every root-relative internal
 * link authored in Markdown/MDX content (e.g. `[Guide](/project/roadmap/)`).
 *
 * Mirrors docs-site/remark-base-links.mjs exactly (see that file's own
 * comment for the full rationale) — duplicated here rather than imported
 * across the two independently-built Astro projects, since this site is
 * meant to build and deploy in isolation from docs-site/.
 *
 * Leaves external links (`https://...`), protocol-relative links (`//...`),
 * mailto/tel links, and same-page anchors (`#section`) untouched.
 */
export function remarkBaseLinks(base) {
  const prefix = base.endsWith('/') ? base.slice(0, -1) : base;
  return (tree) => {
    visit(tree, 'link', (node) => {
      if (typeof node.url === 'string' && node.url.startsWith('/') && !node.url.startsWith('//')) {
        node.url = prefix + node.url;
      }
    });
  };
}
