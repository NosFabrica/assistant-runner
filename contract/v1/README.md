# Trigger contract, v1

The runner wakes a task's service with `POST /v1/run`. It's specified in
[`docs/PLAN.md`](../../docs/PLAN.md), "The trigger contract (v1)".

`vectors/` holds the JSON both sides test against:
- `wake-*.json` is a request the runner may send;
- `answer-*.json` is a `200` body a service may answer;
- `wake-invalid-*.json` is a request a service must answer `400`. Its `"why"` field names the rule.

A change that breaks a vector is `v2`, beside `v1`.
