// Checks the published notification-event JSON Schema with Ajv, a real implementation of the
// JSON Schema specification: the schema itself compiles under strict mode, and every published
// example validates against it (as the event type it claims to be).
//
// The Scala test suite checks the same schema against the events the engine really emits, using a
// deliberately small validator (see spark-adapter's EventSchema.scala); this script is the second
// opinion that the schema means what that validator thinks it means. Runs automatically before
// `npm run build` (the `prebuild` script), so a broken schema or example fails the docs build.
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Ajv2020 from 'ajv/dist/2020.js';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', 'public', 'schemas', 'notification', 'v1');
const schema = JSON.parse(readFileSync(join(root, 'event.schema.json'), 'utf8'));

// allowUnionTypes: the schema deliberately says `"type": ["string", "null"]` for nullable fields.
const ajv = new Ajv2020({ strict: true, allErrors: true, allowUnionTypes: true });
const validateAny = ajv.compile(schema);

// One validator per event type, so an example is checked against the type it claims.
const perType = new Map();
for (const [name, def] of Object.entries(schema.$defs)) {
  const eventType = def.properties?.eventType?.const;
  if (eventType) perType.set(eventType, ajv.compile({ $ref: `${schema.$id}#/$defs/${name}` }));
}

const failures = [];
const seen = new Set();
const dir = join(root, 'examples');
for (const file of readdirSync(dir).filter((f) => f.endsWith('.json')).sort()) {
  const event = JSON.parse(readFileSync(join(dir, file), 'utf8'));
  const validateType = perType.get(event.eventType);
  if (!validateType) {
    failures.push(`${file}: unknown eventType '${event.eventType}'`);
    continue;
  }
  seen.add(event.eventType);
  if (!validateType(event)) failures.push(`${file}: ${ajv.errorsText(validateType.errors)}`);
  if (!validateAny(event)) failures.push(`${file}: does not validate against the top-level schema: ${ajv.errorsText(validateAny.errors)}`);
}
for (const eventType of perType.keys()) {
  if (!seen.has(eventType)) failures.push(`no example published for ${eventType}`);
}

if (failures.length > 0) {
  console.error(`check-schemas: ${failures.length} problem(s)\n  ${failures.join('\n  ')}`);
  process.exit(1);
}
console.log(`check-schemas: schema compiles; ${seen.size} event types, all examples valid`);
