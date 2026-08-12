// Calibrate the load generator before believing anything it says about the API.
//
// The question this answers is not "how fast is the service?" but "how fast can
// this machine ask?". They are different numbers, and when the second is the
// smaller one every measurement of the first is really a measurement of k6.
//
// This project has already been caught by that once. Driving the API through
// its published port on Docker Desktop refused 57% of connections before a
// request reached the application, and the load test scored the run as a pass
// because k6 reports status 0 for a request that never got a response and the
// check was `status < 500`. Both the measurement and the thing measuring it
// were wrong, and nothing in the output said so.
//
//   make load-test-calibrate
//
// The number it prints goes in the README, and the VU count for the campaign is
// chosen from it rather than from ambition.
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// Ramps until throughput stops rising. Each stage holds long enough for the
// rate to settle — a stage shorter than a couple of seconds mostly measures the
// ramp itself.
export const options = {
  scenarios: {
    calibration: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '10s', target: 100 },
        { duration: '10s', target: 250 },
        { duration: '10s', target: 500 },
        { duration: '10s', target: 1000 },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    // Not a pass/fail on the service. A failure here means the generator or
    // the network gave up, which is exactly the finding this run exists to
    // produce — so it is recorded rather than tolerated.
    http_req_failed: ['rate<0.01'],
  },
};

// /health and nothing else. It touches PostgreSQL and Redis, so it is not free,
// but it takes no locks, writes nothing, and cannot be exhausted — which makes
// it the cheapest endpoint the API has and therefore the closest thing to a
// measurement of the generator alone.
//
// The honest caveat, stated because it matters: this is a *ceiling on this
// path*, not a pure generator benchmark. If it lands near the campaign figure,
// the campaign figure is suspect.
//
// Read http_reqs in the summary k6 prints. That rate is the ceiling.
export default function () {
  const res = http.get(`${BASE_URL}/health`);

  check(res, {
    'reached the api': (r) => r.status === 200,
  });
}
