#!/usr/bin/env bash
# Compare build-multiarch output against a reference GitHub release.
# Ensures filenames (by pattern), approximate sizes, and binary metadata (stripped, version) match.
#
# Usage:
#   tools/verify-release-build.sh <release-tag> [dist-dir]
#   CI_TAG=v20260224.0.42919 make build-multiarch && tools/verify-release-build.sh v20260224.0.42919 dist
#
# Requires: curl, jq. Set GITHUB_TOKEN to avoid API rate limits.
set -euo pipefail

RELEASE_TAG="${1:-${RELEASE_TAG:-}}"
DIST_DIR="${2:-dist}"
REF_DIR="${REF_DIR:-/tmp/kopia-release-ref-$$}"
GITHUB_REPO="${GITHUB_REPO:-kopia/kopia-test-builds}"

# CLI-only artifacts from build-multiarch (no kopia-ui, no macOS/Windows from other jobs)
CLI_PATTERN='^kopia_[0-9].*_linux_.*\.deb$|^checksums\.txt$|^kopia-[0-9].*\.(x86_64|aarch64|armhfp)\.rpm$|^kopia-[0-9].*-(linux|freebsd-experimental|openbsd-experimental)-(x64|arm|arm64)\.tar\.gz$'

if [[ -z "$RELEASE_TAG" ]]; then
  echo "Usage: $0 <release-tag> [dist-dir]"
  echo "Example: CI_TAG=v20260224.0.42919 make build-multiarch && $0 v20260224.0.42919 dist"
  exit 1
fi

mkdir -p "$REF_DIR"
echo "=== Reference: $GITHUB_REPO $RELEASE_TAG ===="
echo "=== Compare with: $DIST_DIR ===="
echo ""

# Download reference CLI assets (build-multiarch set only). Use GITHUB_TOKEN if set to avoid rate limits.
CURL_AUTH=()
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  CURL_AUTH=(-H "Authorization: token $GITHUB_TOKEN")
fi
echo "Downloading reference assets (build-multiarch set only)..."
curl -sS "${CURL_AUTH[@]}" "https://api.github.com/repos/${GITHUB_REPO}/releases/tags/${RELEASE_TAG}" | \
  jq -r '.assets[] | "\(.name)\t\(.browser_download_url)"' | \
  while IFS=$'\t' read -r name url; do
    if echo "$name" | grep -qE "$CLI_PATTERN"; then
      curl -sSL "${CURL_AUTH[@]}" -o "$REF_DIR/$name" "$url"
      echo "  $name"
    fi
  done
echo ""

# Compare file set by pattern (normalize version to V so we can match)
get_pattern() { echo "$1" | sed -E 's/[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/V/g'; }
ref_patterns=$(cd "$REF_DIR" && ls -1 2>/dev/null | while read -r f; do get_pattern "$f"; done | sort -u)
dist_patterns=$(cd "$DIST_DIR" && ls -1 2>/dev/null | while read -r f; do get_pattern "$f"; done | sort -u)

echo "--- Filename patterns ---"
if [[ -z "$ref_patterns" ]]; then
  echo "ERROR: No reference assets downloaded (bad tag, rate limit, or API error)." >&2
  exit 1
fi
missing=$(comm -23 <(echo "$ref_patterns") <(echo "$dist_patterns"))
if [[ -n "$missing" ]]; then
  echo "Missing patterns in $DIST_DIR:"
  echo "$missing" | sed 's/^/  /'
  exit 1
fi
echo "All expected filename patterns present."
echo ""

# Size comparison: for each ref file, find matching dist file by pattern and compare size
echo "--- Size comparison (within 15% tolerance) ---"
SIZE_FAIL=0
while IFS= read -r ref_name; do
  [[ -z "$ref_name" ]] && continue
  pat=$(get_pattern "$ref_name")
  # Find dist file with same pattern
  act_name=$(cd "$DIST_DIR" && ls -1 2>/dev/null | while read -r f; do if [[ "$(get_pattern "$f")" == "$pat" ]]; then echo "$f"; break; fi; done)
  if [[ -z "$act_name" || ! -f "$DIST_DIR/$act_name" ]]; then
    echo "  $ref_name: no matching file in $DIST_DIR"
    SIZE_FAIL=1
    continue
  fi
  ref_size=$(stat -f%z "$REF_DIR/$ref_name" 2>/dev/null || stat -c%s "$REF_DIR/$ref_name" 2>/dev/null)
  act_size=$(stat -f%z "$DIST_DIR/$act_name" 2>/dev/null || stat -c%s "$DIST_DIR/$act_name" 2>/dev/null)
  if [[ "$ref_name" == checksums.txt ]]; then
    echo "  $pat: ref=$ref_size actual=$act_size (checksums may differ)"
  else
    diff_pct=0
    if [[ $ref_size -gt 0 ]]; then
      diff_pct=$(( (act_size - ref_size) * 100 / ref_size ))
    fi
    if (( diff_pct < -15 || diff_pct > 15 )); then
      echo "  $pat: ref=$ref_size actual=$act_size (${diff_pct}%) MISMATCH"
      SIZE_FAIL=1
    else
      echo "  $pat: ref=$ref_size actual=$act_size (${diff_pct}%) OK"
    fi
  fi
done < <(ls -1 "$REF_DIR" 2>/dev/null | grep -E '^kopia-|^kopia_|^checksums\.txt$' || true)
if [[ $SIZE_FAIL -ne 0 ]]; then exit 1; fi
echo ""

# Binary: stripped and version stamp (sample one Linux binary from tarball)
echo "--- Binary: stripped + version stamp ---"
# Pick first linux-x64 tarball in dist
sample_tgz=$(cd "$DIST_DIR" && ls -1 kopia-*-linux-x64.tar.gz 2>/dev/null | head -1)
if [[ -n "$sample_tgz" && -f "$DIST_DIR/$sample_tgz" ]]; then
  tmpdir=$(mktemp -d)
  cleanup() { rm -rf "$tmpdir"; }
  trap cleanup EXIT
  tar -xzf "$DIST_DIR/$sample_tgz" -C "$tmpdir" --strip-components=1
  bin="$tmpdir/kopia"
  if [[ -f "$bin" ]]; then
    file_out=$(file "$bin")
    if echo "$file_out" | grep -q "not stripped"; then
      echo "  Binary is NOT stripped (build should use -ldflags '-s -w')"
      exit 1
    fi
    echo "  Stripped: OK"
    ver_out=$("$bin" --version 2>&1 || true)
    echo "  Version output: $ver_out"
    if ! echo "$ver_out" | grep -qE 'version|Version'; then
      echo "  Warning: --version did not show version string"
    fi
  fi
else
  echo "  No kopia-*-linux-x64.tar.gz in $DIST_DIR to sample"
fi
echo "=== Verification done ==="
