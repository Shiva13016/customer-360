"""
check_vars.py - Scan ADF JSON files for unresolved dollar-brace variable patterns.

Usage:
    python scripts/check_vars.py --path adf/

Fails with exit code 1 if any unresolved pattern like ${VAR_NAME} is found.
This prevents deployments with placeholder variables still in the ARM templates.
"""

import argparse
import glob
import json
import re
import sys
from pathlib import Path

# Pattern to detect unresolved variable placeholders
UNRESOLVED_VAR_PATTERN = re.compile(r'\$\{[A-Za-z_][A-Za-z0-9_]*\}')


def scan_file(filepath: str) -> list[dict]:
    """Scan a single JSON file for unresolved variable patterns.

    Returns a list of findings: [{file, line_num, line, match}]
    """
    findings = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                matches = UNRESOLVED_VAR_PATTERN.findall(line)
                for match in matches:
                    findings.append({
                        "file": filepath,
                        "line": line_num,
                        "content": line.rstrip(),
                        "match": match,
                    })
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"  WARNING: Could not parse {filepath}: {e}")
    return findings


def scan_directory(path: str) -> list[dict]:
    """Recursively scan all JSON files in a directory."""
    all_findings = []
    json_files = glob.glob(f"{path}/**/*.json", recursive=True)
    print(f"Scanning {len(json_files)} JSON file(s) in {path}...")
    for f in json_files:
        findings = scan_file(f)
        all_findings.extend(findings)
    return all_findings


def main():
    parser = argparse.ArgumentParser(
        description="Scan ADF JSON files for unresolved dollar-brace variable patterns."
    )
    parser.add_argument(
        "--path",
        required=True,
        help="Path to scan (directory or single JSON file)",
    )
    args = parser.parse_args()

    scan_path = args.path
    path_obj = Path(scan_path)

    if path_obj.is_file():
        findings = scan_file(scan_path)
    elif path_obj.is_dir():
        findings = scan_directory(scan_path)
    else:
        print(f"ERROR: Path not found: {scan_path}")
        sys.exit(1)

    if findings:
        print(f"\nFAIL: Found {len(findings)} unresolved variable pattern(s):\n")
        for f in findings:
            print(f'  [{f["file"]}] line {f["line"]}: {f["match"]}')
            print(f'    -> {f["content"]}')
        print()
        print("Please replace all ${VAR} placeholders before deploying.")
        sys.exit(1)
    else:
        print(f"PASS: No unresolved variable patterns found. Safe to deploy.")
        sys.exit(0)


if __name__ == "__main__":
    main()
