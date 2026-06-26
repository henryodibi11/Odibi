"""Cross-engine REGEX_MATCH semantics.

A ``REGEX_MATCH`` test must give the SAME verdict on every engine. Pandas uses
``Series.str.match`` which anchors at the start of the string (``re.match``
semantics). Spark ``rlike`` and Polars ``str.contains`` are unanchored substring
searches, so the identical pattern passes rows on Spark/Polars that Pandas
rejects — a silent false-negative (bad rows escape quarantine / validation).

To restore parity we anchor the Spark/Polars pattern at the start so it matches
Pandas. Users who want a full-string match keep adding a trailing ``$`` exactly
as they would on Pandas; this only fixes the leading anchor that ``str.match``
applies implicitly and the other engines did not.
"""


def anchor_match(pattern: str) -> str:
    """Anchor ``pattern`` at the start for rlike/str.contains parity with pandas ``str.match``.

    Wrapping in a non-capturing group keeps alternation correct, e.g.
    ``a|b`` -> ``^(?:a|b)`` (start-anchored either branch) rather than ``^a|b``
    (which would only anchor the first branch).
    """
    return f"^(?:{pattern})"
