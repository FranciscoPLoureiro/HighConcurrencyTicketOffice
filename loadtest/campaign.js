// The campaign: every virtual student presses Buy once, at the same moment.
//
// Run against a freshly reset stack, otherwise the campaign is already sold out
// and the run measures nothing:
//
//   make reset && make load-test-campaign
//
// VUS defaults to the brief's 100. To see the phase 1 race through HTTP rather
// than through the integration test, it has to exceed the campaign size —
// 100 requests against 100 tickets cannot oversell no matter how broken the
// code is. VUS=500 is what produced the numbers in the README.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const VUS = Number(__ENV.VUS || 100);

// Custom counters, because the interesting result is not latency but how many
// tickets came out the other end and why the rest were refused.
const sold = new Counter('tickets_sold');
const rejectedSoldOut = new Counter('rejected_stock_exhausted');
const rejectedDuplicate = new Counter('rejected_already_purchased');
const rejectedRateLimit = new Counter('rejected_rate_limited');

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
    // Deliberately not asserting the invariant yet: phase 1 is expected to
    // break it, and a red run here would be reporting the known state of the
    // world rather than a regression. Phase 4 adds the thresholds that fail CI.
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  // Distinct identity per virtual user, so a duplicate refusal is a real
  // signal and not an artefact of the load test reusing one account.
  const res = http.post(`${BASE_URL}/api/v1/tickets/purchase`, null, {
    headers: { 'X-User-ID': `student-${__VU}` },
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
