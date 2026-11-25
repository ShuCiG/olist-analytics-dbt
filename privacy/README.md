# Privacy module

A small model of analysis across two parties without exposing individuals. It is a demo, **not a certified privacy
system**. Decisions: D135.

| Piece | File | What it does |
|---|---|---|
| Tokenisation | `tokens.py` | HMAC-SHA256 of the normalised identifier with a secret salt. Linkage token (salt shared by the two parties) and pseudonym (own salt per party) are kept apart by a domain label |
| Matching | `matching.py` | each party tokenises its own table and shares only chosen columns; the matcher joins on equal tokens and never sees an identifier |
| Release | `release.py` | k-anonymity (default k = 5, in `config.toml`), l-diversity on a sensitive column (l = 2), differencing guard |
| Demo | `demo.py` | Olist: `orders` party (customers, order count) x `reviews` party (scores), average score per state and order band |

```bash
uv run python -m privacy.demo --csv-dir <folder with the Olist CSVs>     # 50 groups released, 4 suppressed
uv run pytest tests/test_privacy.py                                      # the attack tests
```

Salts: `PRIVACY_LINKAGE_SALT`, `PRIVACY_SALT_ORDERS`, `PRIVACY_SALT_REVIEWS` (hex, at least 32 bytes); random per run when unset.

## Threat model

Attacker: the analyst who receives the released tables, and anyone who gets the tokenised tables.

| Attack | Stopped by | Test |
|---|---|---|
| Hash every possible identifier of a known schema and compare with the tokens | secret salt: without it the candidate space is useless | `test_brute_force_of_a_known_schema_fails_without_the_salt` |
| Read an identifier out of a token | HMAC is one-way | same |
| Join the two parties' pseudonyms with each other | a separate salt per party | `test_different_party_salts_do_not_join_without_the_linkage_step` |
| Ask a query that narrows to one person | groups below k are dropped entirely | `test_a_query_narrowed_to_one_person_is_suppressed` |
| Learn a sensitive value from a group where everyone shares it | l-diversity | `test_a_group_of_distinct_people_but_one_sensitive_value_is_suppressed` |
| Ask two queries that differ by one filter and subtract | the second is refused when any group differs from an earlier one by 1 to k-1 people | `test_second_query_with_one_more_filter_is_refused` |
| Read a suppressed group back from a total | no totals are published | `test_release_has_no_totals_and_no_tokens` |

## Out of scope

- **A leaked salt.** With the linkage salt and a list of candidate identifiers an attacker recovers who is in the
  table (`test_the_same_brute_force_works_when_the_salt_leaks` shows it). Key management, rotation and an
  HSM are not modelled. A private set intersection protocol would remove the shared salt; it is not built.
- **The two parties themselves.** The linkage salt is shared between them, so each can tokenise any identifier it
  guesses and test whether the other holds it (membership inference between the parties).
- **Differential privacy.** Counts and means are exact. No noise, no privacy budget.
- **Tracker attacks beyond pairs.** The guard compares each released group with each earlier one. Three or more
  answers that combine into a small difference are not found. Averages over many queries are not limited.
- **The query log lives in the process.** A new `GuardedDataset` starts with an empty log; a service would need to
  persist it, and to tie it to the caller (an attacker with two identities can split the queries).
- **Quasi-identifiers in the data.** k counts people in a group of the chosen columns; it does not look at which
  columns are rare in combination elsewhere, and k-anonymity is not a defence against background knowledge.
- **Free-text columns, timing and side channels.**
- **Column-level access control**: #363.
