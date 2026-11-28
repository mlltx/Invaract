# OpenLineage specification (vendored for tests)

The OpenLineage JSON Schemas Invaract's OpenLineage events are validated against in tests
(`OpenLineageSchema`), copied unchanged from the OpenLineage project at release `1.55.0`
(https://github.com/OpenLineage/OpenLineage/tree/1.55.0/spec): `OpenLineage.json` (spec 2-0-2) and
the facets Invaract publishes, under `facets/`. Licensed under the Apache License 2.0 by the
OpenLineage project contributors (https://github.com/OpenLineage/OpenLineage/blob/main/LICENSE).

To move to a newer release: copy the files again from that tag, and update the facet versions in
`OpenLineageMapper` to the `$id`s of the copied files; `OpenLineageSchemaSpec` fails until they match.
