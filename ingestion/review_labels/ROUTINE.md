# Labelling run (scheduled cloud session)

Instructions for one scheduled labelling run on branch `data/review-labels`. Self-contained: the session starts with
no other context. Never touch `main` or any other branch, never open a PR, change no file outside
`ingestion/review_labels/labels/` and `ingestion/review_labels/work/` (the second one is never committed).

## Steps

1. `git fetch origin && git switch data/review-labels && git pull --ff-only`.
2. Read `ingestion/review_labels/guidelines.md` completely. It is the only labelling standard.
3. Look at 30 labelled examples: `head -31 ingestion/review_labels/labels/batch_000.csv` (hand-checked gold set).
4. `python3 ingestion/review_labels/labels.py status`. If no texts are open, stop: report "all labelled".
5. Note the start time (`date -u`). Repeat chunks of 100 until 45 minutes have passed or no text is open:
   1. `python3 ingestion/review_labels/labels.py next --limit 100 --out ingestion/review_labels/work/next.jsonl`
   2. Read `work/next.jsonl`. For every line write one JSON line to `ingestion/review_labels/work/labels.jsonl`:
      `{"key": "<key from the input>", "sentiment": "...", "topic": "...", "translation_en": "..."}`.
      Label each text yourself, by reading it, following the guidelines (the score is context; the text wins). Do not
      write a script, keyword rules or a model call to produce labels. Copy each `key` exactly.
   3. `python3 ingestion/review_labels/labels.py write ingestion/review_labels/work/labels.jsonl --labeled-by claude-code-routine`
      It writes a new `labels/batch_<UTC time>.csv` and validates every batch. On an error, fix the lines in
      `labels.jsonl` and run it again; never edit an existing batch file.
   4. `rm ingestion/review_labels/work/*.jsonl`, then commit the new batch file only:
      `git add ingestion/review_labels/labels/ && git commit -m "data: review labels, <N> reviews"` and
      `git push origin data/review-labels` (on a rejected push: `git pull --rebase`, then push again).
6. End with `python3 ingestion/review_labels/labels.py status` and report: texts labelled in this run, still open,
   any text you were unsure about (key and why).
