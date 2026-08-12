// The campaign: every virtual student presses Buy once, at the same moment.
//
// Run against a freshly reset stack, otherwise the campaign is already sold out
// and the run measures nothing:
//
//   make reset && make load-test-campaign
//
// VUS defaults to the brief's 100, and has to exceed the campaign size to test
// anything: 100 requests against 100 tickets cannot oversell no matter how
// broken the code is, and cannot demonstrate a limit holding either. VUS=500 is
// what produced both columns of the comparison table in the README.
//
// Raising VUS past RATE_LIMIT_IP is worth knowing about: every virtual user
// comes from this one container, so the generator starts rate limiting itself
// and the tickets it fails to buy are counted under rejected_rate_limited
// rather than lost.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// k6 has no crypto.randomUUID, and pulling a library in from jslib.k6.io would
// make the load test need network access to a third party to start. Sixteen
// random bytes formatted as a v4 UUID is what the endpoint validates against.
function uuid() {
  const hex = '0123456789abcdef';
  let out = '';
  for (let i = 0; i < 36; i++) {
    if (i === 8 || i === 13 || i === 18 || i === 23) out += '-';
    else if (i === 14) out += '4';
    else if (i === 19) out += hex[(Math.random() * 4) | 8];
    else out += hex[(Math.random() * 16) | 0];
  }
  return out;
}

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const VUS = Number(__ENV.VUS || 100);

// Custom counters, because the interesting result is not latency but how many
// tickets came out the other end and why the rest were refused.
const sold = new Counter('tickets_sold');
const rejectedSoldOut = new Counter('rejected_stock_exhausted');
const rejectedDuplicate = new Counter('rejected_already_purchased');
const rejectedRateLimit = new Counter('rejected_rate_limited');

// What counts as a failed request, stated explicitly.
//
// By default k6 scores any 4xx as a failure, and in this campaign most requests
// are *supposed* to be 4xx: only 100 of them can win a ticket and the rest are
// correctly refused. Left alone, http_req_failed reports 100% on a run where
// the system behaved perfectly, and the threshold below becomes noise that
// everyone learns to ignore — the same way `status < 500` once turned a run
// with most connections refused into a pass.
//
// The refusals are answers. A status of 0 — no response at all — is not, and
// remains a failure, which keeps the threshold pointed at the generator and the
// network, which is the only thing it was ever able to measure.
http.setResponseCallback(http.expectedStatuses(200, 409, 429));

export const options = {
  scenarios: {
    midnight: {
      // One attempt per student. Repeating would only produce
      // already_purchased responses and tell us nothing new.
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: '60s',
    },
  },
  thresholds: {
    // With the response callback above, this asserts that the generator
    // reached the API — not that the API said yes.
    //
    // The stock invariant is not asserted here yet, even though it now holds:
    // this script is run by hand, and a threshold that nothing enforces is a
    // comment with extra syntax. Phase 4 wires the run into CI and adds the
    // thresholds that fail it — tickets_sold == 100, no user with two, no 5xx.
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  // Distinct identity per virtual user, so a duplicate refusal is a real
  // signal and not an artefact of the load test reusing one account.
  //
  // A fresh idempotency key per attempt, for the same reason. The endpoint
  // requires one, and reusing a key across virtual users would make most of
  // this run a replay of one purchase rather than a contest for a hundred.
  const res = http.post(`${BASE_URL}/api/v1/tickets/purchase`, null, {
    headers: {
      'X-User-ID': `student-${__VU}`,
      'Idempotency-Key': uuid(),
    },
  });

  if (res.status === 200) {
    sold.add(1);
  } else if (res.status === 429) {
    rejectedRateLimit.add(1);
  } else if (res.status === 409) {
    const code = res.json('error.code');
    if (code === 'stock_exhausted') rejectedSoldOut.add(1);
    if (code === 'already_purchased') rejectedDuplicate.add(1);
  }

  check(res, {
    // The lower bound is not padding. k6 reports status 0 for a request that
    // never got a response at all, and `status < 500` happily accepts it —
    // so a run where most connections were refused can report a perfect
    // score. That mistake hid a generator bottleneck here once already.
    'no server error': (r) => r.status >= 200 && r.status < 500,
    'answer is one of the documented outcomes': (r) =>
      [200, 409, 429].includes(r.status),
  });
}
