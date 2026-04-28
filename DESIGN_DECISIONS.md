# Design Decisions

Architectural decisions captured during the signal-repair sprint (April 2026).

---

## Actor/Director Measurement Principle

Actors and directors will be tracked using the same signal architecture as movies — current attention measurement from Reddit, X (/counts/recent endpoint), YouTube velocity, Google Trends, and news mentions. No reliance on TMDb popularity stat or other static fame indicators as primary scoring inputs.

---

## Current Actor Scoring is Architecturally Broken

The current formula in fetch_data.py lines 1648-1674 produces systematically wrong rankings due to:

- Synthetic mentions composite (news + films×5 + x_mentions) where the films×5 bonus is arbitrary
- Reddit data not flowing into actor scoring at all
- TMDb popularity weighted at 25%, overpowering current attention measurement
- Cast members of the same film inheriting identical signal data and tying at identical ratings (verified: 6 Apex cast members all at 931)
- 800-999 min-max rescale floors all actors too high
- Result: Newcomer in #1 cultural moment (Jaafar Jackson in Michael) ranks below established stars in lower-attention films because he has low TMDb person popularity

---

## TMDb Role Clarification

TMDb is used only as a METADATA source (names, photos, cast lists, release dates, posters). NOT as a measurement source. All attention data comes from Reddit, X, YouTube, Trends, and news direct queries. Removing TMDb popularity from scoring eliminates the only place TMDb data affects ranking.

---

## IMDb Migration Scope

IMDb migration is a metadata source swap, not an architectural change. The signal queries don't depend on TMDb. When Ryan handles the migration on production server, the Hype Index can swap metadata sources independently. Not a blocker for actor refactor.

---

## Person as Navigation Entity, Performance as Measurement Entity

Both Hype Index and Mogul DFS use the same principle:

- The person (actor, director) is the master navigation entity. Cross-product links use person identifiers.
- Each product internally tracks performance-level signal data within person pages.
- Hype Index: actor.html shows person at top, individual performances with separate Hype trajectories below
- Mogul DFS: person page shows draftable performances grouped under person header
- Cross-product link: clicking "Anne Hathaway" on Hype Index lands on Mogul DFS's Anne Hathaway page showing her draftable performances

---

## Cross-Product Passthrough at Launch

Hype Index → Mogul DFS one-way passthrough only at Memorial Day launch. "Draft on Mogul DFS" buttons on Hype Index actor pages. Reverse direction (Mogul DFS → Hype Index) is post-launch fast-follow.

---

## Performance Tracking Refactor — Critical Path

Originally framed as polish/refinement. Now critical path because actor view is producing visibly wrong rankings that anyone in entertainment will notice. Estimated 3-4 days at revised scope (person stays as navigation entity, performance becomes signal-attribution layer within person pages).
