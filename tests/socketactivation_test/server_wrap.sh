#!/bin/sh
export LISTEN_PID=$$
exec $KOPIA_ORIG_EXE "${@}"
