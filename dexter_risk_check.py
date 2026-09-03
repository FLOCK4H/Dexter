"""
dexter_risk_check.py

Optional insider-cluster / rug risk check via RiskDataApi (tnt-audit.com).

Standalone module, not wired into Dexter.py/dexter_strategy.py directly —
didn't want to guess at the right call site in your creator-analysis flow
without knowing the codebase well enough. Import check_token_risk() into
dexter_strategy.py (or wherever you gate an entry) if useful.

Traces top holders back to their first funder — catches supply
deliberately split across several wallets funded by the same source,
which raw holder-percentage checks can miss. Complements Dexter's
creator-tracking angle: a creator can look "clean" on their own history
while still bundling a launch across freshly-funded wallets.

5 free calls/day, no signup. Set RISK_API_KEY in your environment for
15 free calls/day (free, email only, no card).
Get a key: https://tnt-audit.com/risk-api

Usage:
    python dexter_risk_check.py <mint_address>
"""

import asyncio
import os
import sys
from dataclasses import dataclass

import aiohttp

RISK_API_URL = "https://tnt-audit.com/api/v1/token-risk"


@dataclass
class RiskCheckResult:
    ok: bool
    safety_score: int | None = None
    worst_cluster_percent: float = 0.0


async def check_token_risk(mint: str, max_cluster_percent: float = 50.0) -> RiskCheckResult:
    """Fails open (ok=True) on any API error — this is a safety-net check,
    not a hard dependency, and should never block an entry decision on its
    own if the upstream is down or rate-limited."""
    api_key = os.environ.get("RISK_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(RISK_API_URL, params={"mint": mint}, headers=headers) as response:
                if response.status != 200:
                    return RiskCheckResult(ok=True)
                data = await response.json()

        clusters = data.get("insider_clusters", [])
        worst = max((c.get("percent_of_supply", 0) for c in clusters), default=0.0)
        return RiskCheckResult(
            ok=worst <= max_cluster_percent,
            safety_score=data.get("safety_score"),
            worst_cluster_percent=worst,
        )
    except Exception:
        return RiskCheckResult(ok=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python dexter_risk_check.py <mint_address>")
        sys.exit(1)

    result = asyncio.run(check_token_risk(sys.argv[1]))
    print(f"mint: {sys.argv[1]}")
    print(f"safety_score: {result.safety_score}")
    print(f"worst insider cluster: {result.worst_cluster_percent:.1f}% of supply")
    print("-> PASS" if result.ok else "-> FAIL (would skip this entry)")
