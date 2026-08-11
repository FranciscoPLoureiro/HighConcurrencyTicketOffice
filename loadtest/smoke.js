// Smoke test for the running stack.
//
// This is not the campaign load test — that arrives with the purchase endpoint.
// It exists so the load testing loop itself is proven from day one: k6 runs, it
// reaches the API, and the thresholds are wired to fail rather than to decorate.
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
  vus: 10,
  duration: '10s',
  thresholds: {
    // A failure here means the stack is not actually up, so it should stop
    // the run rather than show up as a footnote in the summary.
    http_req_failed: ['rate==0'],
    checks: ['rate==1'],
  },
};

export default function () {
  const res = http.get(`${BASE_URL}/health`);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'every dependency is reachable': (r) => r.json('status') === 'ok',
  });
}
