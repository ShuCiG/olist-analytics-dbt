"""Match two parties on linkage tokens without either side seeing the other's identifiers.

Each party turns its own table (one row per person) into a `TokenTable`: the identifier column is replaced by the
linkage token and never leaves the party. The matcher only sees tokens and the attributes the parties chose to
share, joins on equal tokens, and hands the joined rows to a `GuardedDataset`.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from privacy.release import PERSON, Record, Scalar
from privacy.tokens import Party


@dataclass(frozen=True)
class TokenTable:
    party: str
    attributes: Mapping[str, Mapping[str, Scalar]]  # linkage token -> shared attributes


def tokenise_table(
    party: Party, records: Sequence[Mapping[str, Scalar]], id_column: str, share: Sequence[str]
) -> TokenTable:
    """The party's own step: replace the identifier with a linkage token, keep only the columns it shares."""
    if id_column in share:
        raise ValueError("the identifier column cannot be shared")
    table: dict[str, Mapping[str, Scalar]] = {}
    for record in records:
        identifier = record.get(id_column)
        if not isinstance(identifier, str):
            raise ValueError(f"{party.name}: identifier column {id_column!r} must hold text")
        token = party.linkage_token(identifier)
        if token in table:
            raise ValueError(f"{party.name}: more than one row for one person (aggregate per person first)")
        table[token] = {column: record.get(column) for column in share}
    return TokenTable(party=party.name, attributes=table)


def match(left: TokenTable, right: TokenTable) -> list[Record]:
    """Rows for the people present on both sides; columns are prefixed with the party name."""
    if left.party == right.party:
        raise ValueError("two different parties are needed")
    joined: list[Record] = []
    for token in sorted(left.attributes.keys() & right.attributes.keys()):
        row: dict[str, Scalar] = {PERSON: token}
        for table in (left, right):
            row.update({f"{table.party}_{col}": val for col, val in table.attributes[token].items()})
        joined.append(row)
    return joined
