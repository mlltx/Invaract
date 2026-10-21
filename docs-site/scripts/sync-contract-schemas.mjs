// Publishes the contract and organizational-policy JSON Schemas on the docs site, at
// /schemas/contract/<file>, so an editor outside this repository can point at a stable URL.
//
// The source of truth stays contract/schema/ (what the parser's own tests check the schemas
// against); this copies it into public/ before every dev or build run, so the published copy is
// never edited by hand and cannot drift. The copied directory is gitignored.
import { copyFileSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const site = join(dirname(fileURLToPath(import.meta.url)), '..');
const source = join(site, '..', 'contract', 'schema');
const target = join(site, 'public', 'schemas', 'contract');

mkdirSync(target, { recursive: true });
const files = readdirSync(source).filter((f) => f.endsWith('.schema.json')).sort();
if (files.length === 0) {
  console.error(`sync-contract-schemas: no *.schema.json in ${source}`);
  process.exit(1);
}
for (const file of files) copyFileSync(join(source, file), join(target, file));
console.log(`sync-contract-schemas: published ${files.join(', ')}`);
