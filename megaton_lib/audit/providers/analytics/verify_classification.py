"""CLI: Verify AA classification upload reflection.

Two-level verification:
  Level 1 (default): Export API — confirm values are stored in classification table.
  Level 2 (--report): Report API — confirm values appear in AA report breakdowns.

Usage::

    # Level 1 only (quick)
    python verify_classification.py \\
        --company-id wacoal1 --rsid wacoal-all \\
        --dimension evar29 --column 関係者 \\
        --creds-file ~/key/adobe_credentials.json \\
        --keys "A10000C414905=ブラック,A10000C3C8772=社員"

    # Level 1 + Level 2
    python verify_classification.py \\
        --company-id wacoal1 --rsid wacoal-all \\
        --dimension evar29 --column 関係者 \\
        --creds-file ~/key/adobe_credentials.json \\
        --diff-tsv data/diff.tsv --sample 5 --report --report-sample 10

Exit code 0 if all keys match, 1 if any NG.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AA 分類データの反映を確認する（Level 1: Export / Level 2: Report）",
    )
    parser.add_argument("--company-id", required=True, help="Adobe Analytics company ID")
    parser.add_argument("--rsid", required=True, help="Report suite ID")
    parser.add_argument("--dimension", required=True, help="AA dimension (e.g. evar29, prop10)")
    parser.add_argument("--column", required=True, help="Classification column name")
    parser.add_argument("--org-id", default="", help="Adobe org ID (default: ADOBE_ORG_ID env)")
    parser.add_argument("--token-cache", default="", help="Path to token cache file")
    parser.add_argument(
        "--creds-file",
        default="",
        help="JSON file with client_id, client_secret, org_id",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--keys",
        help="Comma-separated key=value pairs (e.g. A100012345=社員,A100067890=業者)",
    )
    group.add_argument("--diff-tsv", type=Path, help="TSV file with Key and column value")
    parser.add_argument("--sample", type=int, default=0, help="Sample N keys from diff-tsv (0=all)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Also verify via AA Reporting API (Level 2: breakdown check)",
    )
    parser.add_argument(
        "--report-sample",
        type=int,
        default=10,
        help="Keys to spot-check in report mode (default: 10)",
    )

    args = parser.parse_args()

    # ---- Parse expected values ------------------------------------------------
    expected: dict[str, str] = {}
    if args.keys:
        for pair in args.keys.split(","):
            pair = pair.strip()
            if "=" not in pair:
                print(f"[error] Invalid key=value pair: {pair!r}", file=sys.stderr)
                sys.exit(1)
            k, v = pair.split("=", 1)
            expected[k.strip()] = v.strip()
    elif args.diff_tsv:
        if not args.diff_tsv.exists():
            print(f"[error] File not found: {args.diff_tsv}", file=sys.stderr)
            sys.exit(1)
        with open(args.diff_tsv, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                key = (row.get("Key") or "").strip()
                val = (row.get(args.column) or "").strip()
                if key and val:
                    expected[key] = val

    if not expected:
        print("[error] No keys to verify", file=sys.stderr)
        sys.exit(1)

    # Sample if requested
    if args.sample > 0 and len(expected) > args.sample:
        import random

        keys = random.sample(list(expected), args.sample)
        expected = {k: expected[k] for k in keys}

    print(f"[info] Verifying {len(expected):,} keys...")

    # ---- Build client ---------------------------------------------------------
    from megaton_lib.audit.providers.adobe_auth import AdobeOAuthClient
    from megaton_lib.audit.providers.analytics.classifications import (
        ClassificationsClient,
        print_verify_results,
    )

    kwargs: dict = {}
    if args.creds_file:
        from megaton_lib.credentials import load_adobe_oauth_credentials

        creds = load_adobe_oauth_credentials(args.creds_file)
        for k in ("client_id", "client_secret", "org_id"):
            if creds.get(k):
                kwargs[k] = creds[k]
    if args.org_id:
        kwargs["org_id"] = args.org_id
    if args.token_cache:
        kwargs["token_cache_file"] = args.token_cache
    auth = AdobeOAuthClient(**kwargs)
    client = ClassificationsClient(auth=auth, company_id=args.company_id)

    # ---- Level 1: Export API check --------------------------------------------
    results = client.verify_column(
        rsid=args.rsid,
        dimension=args.dimension,
        column=args.column,
        expected=expected,
    )
    print_verify_results(results, label="Level 1: Classification Export API")

    failed = sum(1 for r in results.values() if r["match"] is False)

    # ---- Level 2: Report API check (optional) ---------------------------------
    if args.report:
        report_results = client.verify_column_via_report(
            rsid=args.rsid,
            dimension=args.dimension,
            column=args.column,
            expected=expected,
            sample_size=args.report_sample,
        )
        print_verify_results(report_results, label="Level 2: AA Reporting API")
        failed += sum(1 for r in report_results.values() if r["match"] is False)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    import sys as _sys
    from pathlib import Path as _Path

    # Enable direct execution (python verify_classification.py ...)
    _megaton_app = str(_Path(__file__).resolve().parents[4])
    if _megaton_app not in _sys.path:
        _sys.path.insert(0, _megaton_app)

    main()
