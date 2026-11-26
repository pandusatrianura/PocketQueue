#!/usr/bin/env python3
"""
PocketQueue Packaging Manifest Synchronizer
Updates Formula/pocketqueue.rb and packaging/scoop/pocketqueue.json with
release version and calculated SHA-256 archive hashes.
"""

import argparse
import hashlib
import json
import os
import re
import sys

def compute_sha256(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def parse_checksums_file(filepath: str) -> dict[str, str]:
    hashes = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                # Format: <hash>  <filename>
                sha = parts[0]
                filename = os.path.basename(parts[1])
                hashes[filename] = sha
    return hashes

def update_homebrew_formula(formula_path: str, version: str, hashes: dict[str, str]):
    if not os.path.exists(formula_path):
        print(f"Warning: {formula_path} not found, skipping Homebrew update")
        return

    with open(formula_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Update version
    content = re.sub(r'version\s+"[^"]+"', f'version "{version}"', content)

    # Helper to replace sha256 for a given archive URL pattern
    def replace_sha(archive_name: str, text: str) -> str:
        sha = hashes.get(archive_name)
        if not sha:
            print(f"Notice: hash for {archive_name} not found, retaining existing value")
            return text
        pattern = rf'({re.escape(archive_name)}[^\n]*\n\s*sha256\s+")[0-9a-fA-F]+(")'
        return re.sub(pattern, rf'\g<1>{sha}\g<2>', text)

    content = replace_sha("pocketqueue-darwin-arm64.tar.gz", content)
    content = replace_sha("pocketqueue-darwin-amd64.tar.gz", content)
    content = replace_sha("pocketqueue-linux-arm64.tar.gz", content)
    content = replace_sha("pocketqueue-linux-amd64.tar.gz", content)

    with open(formula_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"✅ Updated Homebrew formula at {formula_path} to version {version}")

def update_scoop_manifest(manifest_path: str, version: str, hashes: dict[str, str]):
    if not os.path.exists(manifest_path):
        print(f"Warning: {manifest_path} not found, skipping Scoop update")
        return

    with open(manifest_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    data["version"] = version
    
    # 64bit (Windows AMD64)
    win_amd64 = "pocketqueue-windows-amd64.zip"
    if "64bit" in data.get("architecture", {}):
        data["architecture"]["64bit"]["url"] = f"https://github.com/pandusatrianura/pocketqueue/releases/download/v{version}/{win_amd64}"
        if win_amd64 in hashes:
            data["architecture"]["64bit"]["hash"] = hashes[win_amd64]

    # arm64 (Windows ARM64)
    win_arm64 = "pocketqueue-windows-arm64.zip"
    if "arm64" in data.get("architecture", {}):
        data["architecture"]["arm64"]["url"] = f"https://github.com/pandusatrianura/pocketqueue/releases/download/v{version}/{win_arm64}"
        if win_arm64 in hashes:
            data["architecture"]["arm64"]["hash"] = hashes[win_arm64]

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"✅ Updated Scoop manifest at {manifest_path} to version {version}")

def main():
    parser = argparse.ArgumentParser(description="Synchronize package manifests with release archives.")
    parser.add_argument("--version", required=True, help="Release version (e.g. 0.7.0)")
    parser.add_argument("--checksums", help="Path to checksums.txt")
    parser.add_argument("--dist", help="Path to dist directory containing archives")
    parser.add_argument("--formula", default="Formula/pocketqueue.rb", help="Path to Formula/pocketqueue.rb")
    parser.add_argument("--scoop", default="packaging/scoop/pocketqueue.json", help="Path to Scoop manifest")

    args = parser.parse_args()

    # Clean version string (strip leading 'v' if provided)
    version = args.version.lstrip("v")
    hashes = {}

    if args.checksums and os.path.exists(args.checksums):
        hashes = parse_checksums_file(args.checksums)
    elif args.dist and os.path.exists(args.dist):
        for fname in os.listdir(args.dist):
            fpath = os.path.join(args.dist, fname)
            if os.path.isfile(fpath) and (fname.endswith(".tar.gz") or fname.endswith(".zip") or fname.endswith(".exe") or not "." in fname):
                hashes[fname] = compute_sha256(fpath)
    else:
        print("Error: Either --checksums or --dist must be provided with valid files", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(hashes)} target artifact checksums:")
    for k, v in hashes.items():
        print(f"  {k}: {v}")

    update_homebrew_formula(args.formula, version, hashes)
    update_scoop_manifest(args.scoop, version, hashes)

if __name__ == "__main__":
    main()
