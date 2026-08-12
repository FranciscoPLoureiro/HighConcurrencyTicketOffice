#!/usr/bin/env sh
# Check the invariants in the source of truth after a load test.
#
# k6 counts the 202s it received. That is a fact about the generator, and it is
# not the invariant. The invariant is what the database holds afterwards: a
# hundred live tickets, each belonging to a different person, and a Redis that
# agrees. Those can only be checked here, and they are the checks the brief
# actually asks for.
#
# The difference is not pedantic. A system that answered 202 to a hundred and
# one callers and then lost one to a failed write would satisfy k6 and be
# broken; so would one that sold a hundred tickets to ninety-nine people.
set -eu

COMPOSE="${COMPOSE:-docker compose}"
CAMPAIGN_ID="${CAMPAIGN_ID:-queima-2026}"
EXPECTED="${TOTAL_TICKETS:-100}"

fail=0

psql() {
  $COMPOSE exec -T postgres psql -qtAX -U tickets -d tickets -c "$1" | tr -d '[:space:]'
}

check() {
  name="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    printf '  \033[32mok\033[0m   %-38s %s\n' "$name" "$got"
  else
    printf '  \033[31mFAIL\033[0m %-38s %s (want %s)\n' "$name" "$got" "$want"
    fail=1
  fi
}

echo "campaign invariants, read from the source of truth"

# Live, not confirmed. A pending ticket is already sold — the seat is gone the
# moment the row exists — so counting confirmed rows would undercount by
# however far the workers happen to be behind and call an oversell a pass.
check "live tickets" \
  "$(psql "SELECT count(*) FROM purchases WHERE campaign_id = '${CAMPAIGN_ID}' AND status <> 'cancelled';")" \
  "$EXPECTED"

# The fairness rule. The brief lists this as one of the thresholds that fails
# the build, and it is the one k6 cannot see at all: every virtual user has its
# own identity, so the generator would report a clean run even if the same
# person had been sold ten tickets.
check "people holding more than one ticket" \
  "$(psql "SELECT count(*) FROM (SELECT user_id FROM purchases WHERE campaign_id = '${CAMPAIGN_ID}' AND status <> 'cancelled' GROUP BY user_id HAVING count(*) > 1) d;")" \
  "0"

# The counter column and the rows are two representations of one fact. They are
# written in the same transaction, so a disagreement is evidence of a bug
# rather than of a race.
check "available column" \
  "$(psql "SELECT available FROM tickets WHERE campaign_id = '${CAMPAIGN_ID}';")" \
  "0"

# And Redis, which is where the invariant is actually enforced. If it disagrees
# with PostgreSQL the campaign is one restart away from selling the difference
# again.
check "redis stock" \
  "$($COMPOSE exec -T redis redis-cli GET "campaign:{${CAMPAIGN_ID}}:stock" | tr -d '[:space:]')" \
  "0"

check "redis buyers" \
  "$($COMPOSE exec -T redis redis-cli SCARD "campaign:{${CAMPAIGN_ID}}:buyers" | tr -d '[:space:]')" \
  "$EXPECTED"

if [ "$fail" -ne 0 ]; then
  echo
  echo "the campaign did not hold its invariants"
  exit 1
fi

echo "all invariants hold"
