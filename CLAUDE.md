# CLAUDE.md

Row-level Mixpanel proxy: receives Mixpanel SDK traffic (`/track`, `/engage`, `/groups`) and fans each record out to any combination of destinations (warehouses, lakes, queues, Mixpanel itself). Plain JavaScript + JSDoc, typechecked with `tsc` — **no TypeScript migration, no build step for the app itself** (esbuild is only used to bundle the lambda artifact).

## Commands

```bash
npm test            # typecheck + unit/contract/route/deadletter tests (no cloud creds needed)
npm run typecheck   # tsc -p jsconfig.json (checkJs over app/components/middleware)
npm run test:unit   # just the offline jest suites
npm run test:int    # per-destination integration tests against LIVE clouds (auto-skips destinations whose env vars are missing)
npm run test:e2e    # boots the server (tests/setup.js) and hits it over HTTP
npm run dev         # nodemon, NODE_ENV=test
npm start           # NODE_ENV=prod node server.js
npm run build:lambda # esbuild bundle → lambda/lambda.js
```

## Architecture

```
server.js          runtime shell only: GCP functions-framework / AWS serverless-http /
                   Azure handler / default app.listen — picked by RUNTIME
app.js             ALL express wiring + handleMixpanelRequest/handleDrop;
                   exports { app, handleMixpanelRequest, handleDrop } for tests
components/
  validate.js      boot-time env validation (fail-fast, registry-driven) + case mirroring
  passthrough.js   always-on proxies to Mixpanel: /lib.min.js /lib.js /record /flags
  bodyParse.js     json/urlencoded/text parsers (50mb)
  transforms.js    parseSDKData (every SDK payload format) + flattenAndRenameForWarehouse
  deadLetter.js    best-effort publish of failed inserts to pubsub_bad_topic; never throws
  queue.js, corsConfig.js, logger.js, retries.js
middleware/
  index.js         the destination REGISTRY (static require map — see deploy notes)
  pubsub.js        reference implementation of the Destination contract
  mixpanel.js bigquery.js snowflake.js redshift.js gcs.js s3.js azure.js
types.d.ts         Destination + DestinationRegistry + payload types
```

Request flow: CORS → passthrough (before body parsers!) → body parsers → optional queue → route → `parseSDKData` → record mutations (ip, token, time) → `flattenAndRenameForWarehouse` → fan out to every active destination → respond `[{ name, result }, ...]`.

## The Destination contract

`types.d.ts` → `Destination`: a **callable** send function plus lifecycle/validation metadata, attached with `Object.assign`:

```js
/** @type {import('../types').Destination} */
const destination = Object.assign(main, {
	init,                                    // idempotent; called at boot, fire-and-forget
	drop,                                    // destructive; only reachable outside prod
	requiredEnvVars: ['foo_project'],        // lowercase; drives boot validation AND int-test auto-skip
	validateEnv,                             // optional: (env) => string[] for conditional rules (see snowflake)
});
module.exports = destination;
```

### Adding a destination

1. Create `middleware/<name>.js` with the typed export tail above (copy `pubsub.js`, the reference implementation).
2. Add ONE static line to `middleware/index.js`.
3. Add its vars to `envSample` — `tests/contract.test.js` fails if any `requiredEnvVars` entry is missing from `envSample`.
4. Document it in `README.md`; add an int-test block in `tests/int.test.js` (it auto-skips without creds).

That's it: validation, activation, fan-out, and contract tests all key off the registry.

## Env model

- `DESTINATIONS` (default `MIXPANEL`) is the explicit activation list; every destination is optional, any combination works.
- Boot validation is fail-fast and **aggregated**: every missing var and every `validateEnv` message across all listed destinations comes back in ONE thrown error. An unknown name in `DESTINATIONS` is also a boot error.
- **Load-bearing:** `validate.js` mirrors every env var into `process.env` under both original and lowercase names. Destinations destructure lowercase names (`bigquery_project`, …) from `process.env`. Do not remove the mirroring loop.
- `pubsub_project` falls back to `bigquery_project` when unset.
- Destinations re-read their env inside `init` at call time (not module load), which is why int tests can flip vars per test.

## Deploy notes

- `middleware/index.js` must keep **static** requires — `npm run build:lambda` (esbuild) cannot bundle dynamic `require(variable)`.
- `server.js` is the entrypoint everywhere (Dockerfile, esbuild, scripts/); the runtime switch keys off `RUNTIME` (`GCP`/`AWS`/`AZURE`/default listen). Don't move platform exports into app.js.

## Do not break

- **Passthrough before body parsers** (`app.js` ordering): `/record`, `/flags`, `/lib*.js` stream the raw request to Mixpanel; a consumed body breaks the upstream call. `tests/routes.test.js` asserts the layer order.
- **Response shape** `[{ name, result }]` from `/track|/engage|/groups` — clients and e2e tests depend on it.
- **clone-vs-raw split** in `handleMixpanelRequest`: mixpanel gets the raw parsed `data`; every other destination gets `clone(flatData)` so mutations can't leak between destinations.
- **/drop prod guard**: `handleDrop` returns 403 when `NODE_ENV === 'prod'`.
- **parseSDKData** must keep accepting all SDK formats: JSON arrays/objects, base64, sendBeacon `data=<urlencoded base64>`, sendBeacon `data=<urlencoded JSON>`, raw base64 with `=` padding, surrounding whitespace. Tests in `tests/unit.test.js` (PARSING).

## Known state

- Snowflake module split (1k-line file → directory) is deferred: gated on green snowflake int tests, and the live connection currently fails for infra reasons.
- Redshift/S3 int tests auto-skip unless creds are present.
