#!/bin/sh -e
for attempt in 1 2 3; do
  echo "+ $@ (attempt $attempt)"
  if "$@"; then
    exit 0
  fi
done
exit 1
