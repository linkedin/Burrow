#!/bin/sh

set -euo pipefail

/etc/burrow/configure
/app/burrow --config-dir /etc/burrow
