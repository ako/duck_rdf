#!/usr/bin/env bash
# Run each test in test/known_bugs and report whether the bug is still present.
# Exit status: 0 when every known bug still fails; 1 when any of them now
# passes (move that test to test/sql/ together with the fix); 2 on setup errors.
set -u
cd "$(dirname "$0")/.."

UNITTEST=${UNITTEST:-build/release/test/unittest}
if [ ! -x "$UNITTEST" ]; then
  echo "unittest binary not found at $UNITTEST (build with 'make release', or set UNITTEST)" >&2
  exit 2
fi

shopt -s nullglob
tests=(test/known_bugs/*.test)
if [ ${#tests[@]} -eq 0 ]; then
  echo "no known-bug tests in test/known_bugs"
  exit 0
fi

fixed=0
for test in "${tests[@]}"; do
  if "$UNITTEST" --test-dir . "$test" > /dev/null 2>&1; then
    echo "FIXED          $test  -> move it to test/sql/"
    fixed=1
  else
    echo "still failing  $test"
  fi
done
exit $fixed
