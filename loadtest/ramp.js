// The load profile that gates CI: aggressive ramp, thirty second plateau, ramp
// down — and thresholds that fail the build rather than decorate the summary.
//
//   make reset && make load-test-ramp
//
// This is not the same test as campaign.js and does not replace it. That one is
// a single simultaneous burst, which is what the midnight moment actually looks
// like and what the phase-by-phase comparison table is measured with. This one
// holds pressure on the system for half a minute, which is what catches the
// things a burst cannot: a connection pool that leaks, a queue that grows
// without bound, a latency that degrades as the run goes on rather than
// starting bad.
//
// Read the two together. Neither is the whole picture.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// k6 has no crypto.randomUUID, and pulling a library from jslib.k6.io would
// make the load test need a third party to be reachable before it can start.
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

// Chosen from measurement, not from ambition.
//
// On the laptop declared in docs/MEASUREMENT.md — the API capped at 2 CPUs and
// 512 MB, with Prometheus and Grafana running alongside — the thresholds below
// hold at 50 across repeated runs and are unreliable above it. 100 passed once
// at 153 ms and then failed twice at 291 ms and 333 ms; 150 failed at 451 ms.
//
// The variance is the finding, not an inconvenience. A number that passes on a
// quiet machine and fails on a busy one is not a capacity figure, it is a
// coin toss, and a CI gate built on one teaches people to re-run the build
// until it goes green. 50 is where that measurement is repeatable.
//
// Repeatable on that machine. The same profile on a faster one passes 150 nine
// times out of nine, so this default describes the laptop rather than the
// system. That is the point rather than a defect — the number keeps CI honest
// on the hardware it was calibrated against, and raising it would only move the
// calibration to somebody else’s desk.
//
// The second machine did settle one thing. The hundred tickets go in the first
// fraction of a second of the ramp, so purchase_duration{outcome:sold} is
// sampled before the plateau arrives, and the faster the host the truer that
// gets — fast enough and the sale p99 falls below the refusal p99, which cannot
// describe a path that writes to PostgreSQL and publishes to RabbitMQ. Read it
// as a smoke check that the campaign sold out; the refusal p99 below, over two
// hundred thousand samples instead of a hundred, is the one that tracks load.
// docs/MEASUREMENT.md records the runs.
const PEAK_VUS = Number(__ENV.VUS || 50);

const sold = new Counter('tickets_sold');
const rejectedSoldOut = new Counter('rejected_stock_exhausted');
const rejectedDuplicate = new Counter('rejected_already_purchased');
const rejectedRateLimit = new Counter('rejected_rate_limited');
const serverErrors = new Counter('server_errors');
// Anything that is not one of the four documented answers. Counted rather than
// left to k6's own `checks` metric, whose threshold passes when no check ever
// ran — which is precisely the hole this file already fell through once.
const undocumented = new Counter('undocumented_answers');
// Every refusal, whatever the reason. Exists to be counted: a Trend cannot
// carry a count threshold, so the guard against an empty latency metric has to
// sit on a Counter next to it.
const refusals = new Counter('refusals');

// Latency recorded by hand, tagged with what the request turned out to be.
//
// The obvious approach — tagging the request and thresholding
// http_req_duration{outcome:sold} — does not work: k6 fixes a request's tags
// when it is made, and the outcome is only known from the response. Assigning
// to res.request.tags afterwards throws, which in an earlier version of this
// file aborted every iteration before its checks ran. The thresholds still
// reported green, because a threshold with no samples passes.
//
// A Trend takes its tags at add time, which is after the answer is known.
const purchaseDuration = new Trend('purchase_duration', true);

// Every documented outcome is an expected status. Left alone, k6 scores any 4xx
// as a failure, and in this campaign nearly every request is *supposed* to be
// refused — so http_req_failed would report ~99% on a run where the system
// behaved perfectly, and the threshold on it would become noise everybody
// learns to ignore.
//
// A status of 0 — no response at all — is not an answer and stays a failure,
// which keeps that threshold pointed at the generator and the network, the only
// thing it was ever able to measure.
http.setResponseCallback(http.expectedStatuses(202, 400, 409, 429));

export const options = {
  scenarios: {
    campaign: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        // Aggressive on purpose. A gentle ramp lets the pool warm and the
        // caches fill before the pressure arrives, which is the opposite of a
        // campaign opening at midnight.
        { duration: '5s', target: PEAK_VUS },
        { duration: '30s', target: PEAK_VUS },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  // Every threshold that could pass on an empty metric is paired with a
  // count. This is not belt and braces — a k6 threshold over a metric with no
  // samples *passes*, so a script that crashes before recording anything
  // reports a clean green sweep. That happened here, and the counts are what
  // make it impossible to happen quietly again.
  thresholds: {
    // The invariant. Not "about a hundred" — exactly a hundred is the only
    // number that means the system worked.
    tickets_sold: ['count==100'],

    // No request may fail to get an answer, and there must have been enough
    // requests for that to mean something.
    server_errors: ['count==0'],
    http_req_failed: ['rate<0.01'],
    http_reqs: ['count>1000'],

    // The purchase path, isolated. Thresholding the overall p99 would be
    // flattering nonsense: the hundred tickets are gone within a second and
    // the remaining twenty-nine are cheap refusals, so an aggregate p99
    // mostly measures how fast the system can say no.
    //
    // k6 will not take a count threshold on a trend, so the guard against an
    // empty latency metric is the counter beside it: tickets_sold==100 above
    // and refusals>1000 below. Without them, a run that sold nothing would
    // report a flawless p99 over an empty set.
    'purchase_duration{outcome:sold}': ['p(99)<200'],

    // Refusals should be far faster than sales, since they never reach
    // PostgreSQL or the broker. If this ever approaches the threshold above,
    // something is doing work on a path that is meant to be a lookup.
    refusals: ['count>1000'],
    'purchase_duration{outcome:refused}': ['p(99)<100'],

    // Every answer has to be one this API documents. `checks` is kept as a
    // threshold too, for the summary line, but the counter above is the one
    // that cannot be satisfied by an empty metric.
    undocumented_answers: ['count==0'],
    checks: ['rate==1'],
  },
};

export default function () {
  // A distinct identity per iteration rather than per virtual user. Over a
  // thirty second plateau a VU runs many times, and reusing its identity would
  // turn every iteration after the first into `already_purchased` — measuring
  // the duplicate check over and over instead of the contention.
  const userID = `student-${__VU}-${__ITER}`;

  const res = http.post(`${BASE_URL}/api/v1/tickets/purchase`, null, {
    headers: {
      'X-User-ID': userID,
      'Idempotency-Key': uuid(),
    },
  });

  // Classified from the answer, then recorded — which is the only order that
  // works, since the outcome is not knowable until the response arrives.
  if (res.status === 202) {
    sold.add(1);
    purchaseDuration.add(res.timings.duration, { outcome: 'sold' });
  } else if (res.status === 409) {
    purchaseDuration.add(res.timings.duration, { outcome: 'refused' });
    refusals.add(1);
    const code = res.json('error.code');
    if (code === 'stock_exhausted') rejectedSoldOut.add(1);
    if (code === 'already_purchased') rejectedDuplicate.add(1);
  } else if (res.status === 429) {
    purchaseDuration.add(res.timings.duration, { outcome: 'refused' });
    refusals.add(1);
    rejectedRateLimit.add(1);
  } else if (res.status >= 500) {
    serverErrors.add(1);
    undocumented.add(1);
  } else {
    undocumented.add(1);
  }

  check(res, {
    // The lower bound is not padding. k6 reports status 0 for a request that
    // never got a response, and `status < 500` accepts it happily — so a run
    // where most connections were refused can score perfectly. That mistake
    // hid a generator bottleneck in this project once already.
    'no server error': (r) => r.status >= 200 && r.status < 500,
    'answer is one of the documented outcomes': (r) =>
      [202, 409, 429].includes(r.status),
    // The only way to earn a 400 is a missing or malformed Idempotency-Key,
    // which would mean the generator is at fault rather than the API.
    'the request was well formed': (r) => r.status !== 400,
  });
}
