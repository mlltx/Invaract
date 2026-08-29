import { visit } from 'unist-util-visit';

/**
 * Prepends the site's configured base path to every root-relative internal
 * link authored in Markdown/MDX content (e.g. `[Guide](/guides/foo/)`).
 *
 * Content pages write internal links as plain root-relative paths, not
 * hardcoded with the production base — this is what lets the exact same
 * source build correctly at any base: `/Invaract` in production, or
 * `/Invaract/pr-preview/pr-<n>/` for a PR preview deploy (see
 * .github/workflows/deploy-docs.yml). Starlight's own generated links
 * (sidebar, pagination, "edit this page") already resolve against the
 * configured base on their own; this plugin covers the links authors write
 * by hand in page content, which Astro does not rewrite automatically.
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
