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

---

## Attention Attribution Principle

The Hype Index measures where cultural attention lands, not where it originates. A person — actor, director, public figure — becomes a vessel for attention regardless of whether that attention is specifically about their current work.

Examples:
- A film called Melania during a politically active moment for Melania Trump will absorb attention from the broader political conversation. The film inherits the cultural weight of its subject's news cycles.
- A Michael Jackson biopic during a moment of renewed Michael Jackson cultural conversation will absorb attention from that broader context.
- A Zendaya film during a year when Zendaya is culturally dominant will absorb attention from her celebrity heat, including her TV work and public appearances.

The pari passu redistribution model captures this. Per-performance queries measure film-specific attention. A person-level query captures total cultural attention. The unattributed remainder (total minus film-specific sum) is redistributed proportionally across the person's current performances — films with stronger measured signal absorb more of the celebrity halo.

This is intentional. The Hype Index does not try to algorithmically purify "movie-specific" attention from "celebrity halo" attention. Cultural attention doesn't disambiguate cleanly in real life, and the system doesn't pretend it does. Films live in their attention environments, and the index reports those environments honestly.

Editorial implication: When a film like Melania ranks higher than its quality alone would suggest, that's a feature, not a bug. The system is correctly identifying where attention is flowing. Industry observers and players will understand this.

Architectural implication: Future engineering decisions should preserve this principle. Algorithmic filters that try to remove "non-film mentions" from a film's signal would violate this design intent. The index is an attention measurement system, not an attention purification system.

---

## Reddit Signal Removal — Commercial ToS Compliance

Reddit's free API tier explicitly prohibits commercial use. The Hype Index feeds Mogul DFS, a real-money commercial DFS product. Continued use of free-tier Reddit access for signal collection represents a ToS violation that could result in cease-and-desist, IP banning, or legal action — particularly risky during or after public launch.

DECISION: Remove Reddit from all signal collection. Pending confirmation from Segev (Ron) on legal read.

Implementation scope:
- Remove fetch_reddit_mentions() and related code from fetch_data.py
- Remove fetch_reddit_for_title() from signal_fetchers.py
- Remove _reddit_volume() from score.py
- Remove "reddit_volume": 0.20 from WEIGHTS dict
- Remove reddit_posts/reddit_comments from public payload in update.py
- Remove subreddits and reddit_user_agent from config.json

Rebalanced weights after Reddit removal:
- YouTube velocity: 0.45 (up from 0.35)
- X mentions: 0.30 (up from 0.25)
- Google Trends: 0.15 (unchanged)
- News impact: 0.10 (up from 0.05)

Impact analysis (run 2026-04-28):
- Top 2 (Michael, Backrooms) hold position
- Middle of top 10 reshuffles within same band
- Spider-Man drops from #15 to #31 (5th highest Reddit signal removed)
- No catastrophic reordering — index remains functional and culturally accurate

Google Trends note: pytrends is unofficial scraper of Google Trends — no API key, no commercial license, gray-area. Keep for now, revisit if Google blocks more aggressively or Segev flags.

X note: paid commercial access via credits is explicitly licensed for commercial use. No issue.

YouTube note: YouTube Data API v3 commercial-acceptable terms. No issue.

News (RSS): public feeds, no commercial restriction. No issue.

Status: **Executed 2026-04-29.** Decision made by Stacy as a copyright/IP exposure call — does not require Segev sign-off (not a DFS regulatory matter).

Secondary benefit: pulse cycle expected to drop significantly from 3.5 hours, as Reddit was the rate-limit bottleneck across 430 movies (4 subreddits × 430 titles × 2s spacing with 429 backoffs).

Expected impact: top 20 stable, 31 lower-ranked titles drop 10+ positions (Reddit-heavy: Marvel TV, horror sequels, comedy IP). Archived Reddit data from past pulses preserved on disk for backtesting.
