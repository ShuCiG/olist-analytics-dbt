# Review labelling guidelines (prompt version 1)

Each Olist review (Brazilian Portuguese, optional title and message, a 1–5 score) gets three labels. The same text is
the instruction for whoever labels: a person, a Claude Code session, or the API script for new reviews.

## sentiment

The customer's attitude in the text, not the score (they usually agree; when they don't, the text wins).

| value | when |
|---|---|
| `positive` | satisfied, praise, recommends |
| `negative` | complaint, problem, disappointment, not received |
| `mixed` | clear praise and a clear complaint in one review ("product is great but arrived late") |
| `neutral` | no attitude: a question, a fact, an unrelated remark, unreadable |

## topic

The one aspect the review is mainly about. With several, the one the complaint is about; for praise of several, the
first one named.

| value | when |
|---|---|
| `delivery` | delivery time, delay, not received yet, tracking, carrier, delivery condition of the parcel |
| `product_quality` | the product itself: quality, works or not, matches the description and photos, defects, size |
| `wrong_or_missing_item` | a different product, wrong colour or model, missing units or parts, only part of a multi-item order arrived |
| `seller_service` | the seller or store: communication, answers, exchange, refund, cancellation, invoice |
| `general` | overall satisfaction with no specific aspect ("Recomendo", "Ótimo", "Tudo certo", "Atende minha necessidade"), or no aspect at all (a product name, "4", gibberish) |
| `other` | a specific aspect outside the list: price and shipping cost, packaging, payment, the marketplace site, gifts |

Edge cases settled while labelling the first 300:

- Arrived in good or bad condition, on time or early, picked up at the post office instead of home delivery: `delivery`.
  Broken, damaged or used-looking product: `product_quality`.
- Different from the ad or the photos, does not fit: `product_quality`. A different model, colour or size than ordered,
  a missing part or unit: `wrong_or_missing_item`.
- Out of stock after payment, cancellation or refund not handled, no answer from the store: `seller_service`, even
  when the review also says the product never came.
- "Not received yet" is `negative` unless the text only says the customer is waiting to rate it (`neutral`).
- A text that contradicts its score (praise with 1 star) keeps the text's sentiment.

## translation_en

A faithful English translation of title and message (title first, joined with " — " when both exist). Keep the
meaning and tone, fix nothing, add nothing; no summary. Product and brand names stay as written.
