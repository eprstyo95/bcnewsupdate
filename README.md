# BC News Monitor

Watches Indonesian customs & excise (Bea Cukai) news, pushes new articles to
Telegram, and publishes a dashboard to GitHub Pages.

## Layout

| Path | Role |
| --- | --- |
| `bc_news_update.py` | The whole app. Subcommands below. |
| `bc_news_upadte.py` | Shim for the historic misspelled entrypoint. |
| `seen.sqlite` | State: seen articles, source health, bot state, reactions. Committed. |
| `docs/index.html` | Generated dashboard, served by Pages. |
| `scripts/ci_commit_push.sh` | Commits the generated files and pushes, merging on rejection. |

## Subcommands

```
python bc_news_update.py [run]        # fetch feeds, send new articles
python bc_news_update.py poll         # answer Telegram bot commands
python bc_news_update.py dashboard    # regenerate docs/index.html
python bc_news_update.py digest       # daily digest to Telegram
python bc_news_update.py stats        # weekly stats
python bc_news_update.py leaderboard  # weekly source leaderboard
python bc_news_update.py report       # weekly PDF
python bc_news_update.py export       # CSV export
python bc_news_update.py backfill     # fill missing source/sentiment/hashtags
python bc_news_update.py merge-db X   # union another seen.sqlite into this one
python bc_news_update.py setup        # one-time Telegram setup
```

Secrets read from the environment: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`,
`TELEGRAM_PRIVATE_CHAT_ID`.

## Workflows

- `bcnews-hourly.yml` — every 5 min: run, poll, dashboard, commit + push.
- `bcnews-reports.yml` — digest daily 14:00 UTC, the weekly bundle Mon 01:00 UTC.

Both read `seen.sqlite` from the checkout. `actions/cache` is deliberately not
used for it: the cache save key carries the run id, so restores only ever hit a
prefix key — an older snapshot — which would overwrite the committed DB and make
already-sent articles look unseen.

## Known tradeoff: DB size in git

`seen.sqlite` (~20 MB) is committed on every run that finds news, ~70×/day. That
is what keeps state durable, and dropping a commit would re-send articles, so the
cost is accepted: about 18 MB of pack growth per week (125 MB as of 2026-07-25,
from 4.2k DB commits since 2026-06-04).

If the repo approaches GitHub's 1 GB advisory limit, the fix is to move the DB
out of git (release asset, or a small external store) rather than to commit it
less often. A row-retention policy on the `seen` table would also help — nothing
prunes it today.
