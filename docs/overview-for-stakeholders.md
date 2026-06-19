# PolyGlider, explained without the jargon

## What this is

PolyGlider is a working demo of an order-processing system — the kind of thing that sits behind an "Place Order" button and makes sure that click turns into a recorded, trackable order, reliably, even when parts of the system are having a bad day.

It's built as a learning and portfolio project, not a production recommendation. The system is deliberately built using three different programming languages and a full set of supporting tools, specifically so the author (and anyone reading the code) can see several different real-world engineering approaches solving the same kind of problem, side by side, and actually run all of it on a laptop with no cloud account required. A real company building this for real customers would not normally mix three languages this way — that part is intentional and explained in [`docs/architecture.md`](architecture.md).

## What it does

In plain terms: someone places an order. That request needs to be:

1. **Accepted quickly** — the person clicking the button shouldn't have to wait around while the system does its bookkeeping.
2. **Never lost** — once accepted, the order has to actually get recorded, even if some downstream piece of the system briefly stumbles.
3. **Tracked** — there's a running tally (a "ledger") of how much of each item has been ordered, kept correct and consistent.

PolyGlider does all three: a request comes in, gets handed off to a queue almost instantly (so the response is fast), and a separate part of the system picks it up from that queue and records it in a database — and if anything along the way breaks, the system notices and recovers without losing the request, rather than silently dropping it or duplicating it.

## Why it's built this way

**Resilience — what happens when things go wrong.** Real systems fail in small ways constantly: a database hiccups for a few seconds, a network blips, a message gets sent twice by accident. PolyGlider is built to expect this and handle it automatically wherever possible — failed attempts get retried with increasing delays, genuinely broken messages get set aside for a human to look at instead of retried forever, and the system has even been deliberately broken on purpose (database killed mid-operation, network delays injected, duplicate and malformed messages sent) to verify the recovery behavior actually works as designed, not just in theory. The results of those deliberate failure tests are written up as "postmortems" — the same kind of after-the-fact report a real incident would get.

**Observability — nothing is a black box.** Every part of the system reports what it's doing: how many orders came in, how many failed and why, how long things took, and a full trace of any individual order's path through the system from the moment it was submitted to the moment it landed in the database. This is the same visibility a real operations team would want when something goes wrong at 2am.

**Security — basic but real guardrails.** Requests can be rate-limited (so one client can't overwhelm the system) and authenticated with an API key, both of which can be switched on or off depending on the situation — useful for a demo that needs to flex between "wide open for easy testing" and "locked down like it would be in production."

## What's been built

- **Three services, three languages** — a request-handling front door (C#), an order-processing engine (Scala), and a couple of small Python tools (a load-testing tool, and a tool that lets an AI assistant query and place orders directly).
- **Supporting infrastructure** — a message queue (so the front door and the processing engine don't have to talk to each other directly or wait on each other), a database (the permanent record), a cache (used for the rate limiter), a load balancer (so the front door can run as multiple copies for more capacity), and a dashboarding/monitoring stack (so all of the above is visible at a glance).
- **Two ways to run it** — a fast, single-instance mode for quick local iteration, and a fully "production-shaped" mode with multiple front-door instances behind a load balancer, for testing what happens under more realistic conditions.

## Current maturity and status

This is an actively maintained learning/demo project, not a production system serving real traffic. It has been:
- **Chaos-tested** — deliberately broken in several ways (database outages, network delays, malformed and duplicate data) to verify the recovery behavior, with the results documented rather than just assumed.
- **Documented at multiple levels** — there's a record of *why* each major design decision was made (not just what the code does), a runbook for "what do I do if X breaks," and the postmortems mentioned above.
- **Still growing** — a handful of known gaps and planned improvements are tracked openly as GitHub issues rather than hidden, which is itself part of treating this as a real, evolving piece of engineering rather than a one-off demo.
