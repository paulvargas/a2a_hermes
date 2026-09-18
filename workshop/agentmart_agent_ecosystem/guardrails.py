"""Guardrails: grounding (no invented SKUs/prices) and input validation
(prompt-injection detection, control-char stripping, length capping).

This module only imports from agentmart_ecosystem and catalog — it does not
modify them.
"""

import re
from agentmart_ecosystem import SKU_PATTERN
try:
    from catalog import query_products
except Exception:
    query_products = None

_INJECTION = re.compile(r"(ignore (all|your|previous) instructions|reveal (the )?system prompt|"
                        r"disregard .* rules|you are now|act as)", re.IGNORECASE)

# SKU_PATTERN (imported above) matches only the canonical AM-XXX-0000 shape
# (exactly 3 letters, 4 digits), so it won't even recognize a malformed
# hallucinated SKU like "AM-FAKE-9999" (4 letters) as SKU-shaped, and such
# tokens would slip through ground_response() unredacted. Grounding needs to
# catch anything that *looks* like a SKU so it can be checked against the
# real catalog, so scan with a more permissive shape here and rely on exact
# membership in `allowed_skus` (sourced from SKU_PATTERN-conformant catalog
# data via known_skus()) to decide what is genuine.
_SKU_LIKE = re.compile(r"\bAM-[A-Z0-9]+-[A-Z0-9]+\b", re.IGNORECASE)


def validate_input(text, max_len=2000):
    """Sanitize user input: strip control chars, cap length, flag injection phrases.

    Returns (clean_text, flags).
    """
    flags = []
    if _INJECTION.search(text or ""):
        flags.append("prompt_injection")
    clean = "".join(ch for ch in (text or "") if ch == "\n" or ch >= " ")
    clean = clean[:max_len]
    return clean, flags


def ground_response(text, allowed_skus):
    """Replace any SKU-looking token not in allowed_skus with a placeholder.

    Returns (safe_text, violations) where violations lists the invented SKUs found.
    """
    violations = []

    def repl(m):
        sku = m.group(0).upper()
        if sku in allowed_skus:
            return m.group(0)
        violations.append(sku)
        return "[unverified SKU]"

    safe = _SKU_LIKE.sub(repl, text or "")
    return safe, violations


def known_skus():
    """Return the set of SKUs present in the seeded catalog.

    Defensive: returns an empty set if the catalog is unavailable.
    """
    if not query_products:
        return set()
    try:
        return {p["sku"].upper() for p in query_products(limit=1000) if p.get("sku")}
    except Exception:
        return set()
