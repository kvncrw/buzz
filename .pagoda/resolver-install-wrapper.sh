#!/usr/bin/env bash
# Install as the EXISTING buzz-fork-resolver/resolve.sh, retaining its cron.
# The Python entry point takes the same .lock with OS flock for every command.
set -euo pipefail
resolver_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
export PATH="${HOME}/.local/bin:/usr/local/bin:/usr/bin:/bin"
unset PYTHONHOME
while IFS='=' read -r key _; do
  case "$key" in GIT_CONFIG*) unset "$key" ;; esac
done < <(env)
set -a
# A different agent signs the wakeup because a self-mention cannot wake Astra.
# shellcheck source=/dev/null
source "${resolver_dir}/../fable-seat/buzz-env"
set +a
exec python3 "${resolver_dir}/resolver.py" --state "$resolver_dir" tick "$@"
