# Known bugs

Each `.test` file here asserts the **correct** behaviour for a bug that is not
fixed yet, so it fails today. These files are not part of the normal suite
(`test/sql`), which keeps that suite green.

Check their status with:

```bash
scripts/check_known_bugs.sh
```

It runs each file and reports `still failing` or `FIXED`. It exits non-zero
when a file passes: move that file to `test/sql/` in the same change that fixes
the bug, so the fix stays covered.

| File | Problem |
|---|---|
| _(none at the moment)_ | |
