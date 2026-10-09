#!/usr/bin/env bash
# Point the Railway relay service at a new image and wait for it to come up.
#
#   railway-deploy.sh --image ghcr.io/kvncrw/buzz:pagoda@sha256:...   # deploy
#   railway-deploy.sh --dry-run                                        # read-only
#
# --dry-run only runs the serviceInstance query (current image, latest
# deployment) and proves the token and ids work. It never mutates anything.
#
# Env (all required unless noted):
#   RAILWAY_TOKEN           API token (never printed)
#   RAILWAY_SERVICE_ID      service id (default: the Pagoda relay)
#   RAILWAY_ENVIRONMENT_ID  environment id (default: production)
#   RELAY_READINESS_URL     optional, GET must return 200 after deploy
#   DEPLOY_TIMEOUT_SECONDS  optional, default 900
# shellcheck disable=SC2016  # GraphQL $vars are intentionally literal
set -euo pipefail

API=https://backboard.railway.com/graphql/v2
SERVICE_ID="${RAILWAY_SERVICE_ID:-9effa3d6-0135-4a53-86dc-566368e1f5f3}"
ENV_ID="${RAILWAY_ENVIRONMENT_ID:-8ad906d2-9445-408e-9ae9-fbd7b70494b6}"
READINESS="${RELAY_READINESS_URL:-}"
TIMEOUT="${DEPLOY_TIMEOUT_SECONDS:-900}"

MODE=""
IMAGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) MODE=dry-run ;;
    --image) IMAGE="$2"; shift ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
  shift
done
if [[ "$MODE" != "dry-run" ]]; then
  [[ -n "$IMAGE" ]] || { echo "need --image <ref@digest> or --dry-run" >&2; exit 1; }
  MODE=deploy
fi
[[ -n "${RAILWAY_TOKEN:-}" ]] || { echo "RAILWAY_TOKEN is not set" >&2; exit 1; }

# gql '<query>' '<variables json>' -> response json on stdout; exits 1 on errors.
gql() {
  local resp
  resp=$(jq -n --arg q "$1" --argjson v "${2:-null}" '{query:$q, variables:$v}' \
    | curl -sS --fail-with-body --max-time 60 -X POST "$API" \
        -H "Authorization: Bearer $RAILWAY_TOKEN" -H 'Content-Type: application/json' -d @-)
  if [[ "$(jq -r '.errors // empty | length' <<<"$resp")" != "" ]]; then
    echo "railway: GraphQL error:" >&2
    jq -r '.errors[].message' <<<"$resp" >&2
    return 1
  fi
  printf '%s' "$resp"
}

VARS=$(jq -n --arg s "$SERVICE_ID" --arg e "$ENV_ID" '{serviceId:$s, environmentId:$e}')

echo "railway: service=$SERVICE_ID environment=$ENV_ID"
current=$(gql 'query($serviceId:String!,$environmentId:String!){
  serviceInstance(serviceId:$serviceId, environmentId:$environmentId){
    id source { image } latestDeployment { id status createdAt } } }' "$VARS")
jq -r '.data.serviceInstance | "railway: current image=\(.source.image)\nrailway: latest deployment=\(.latestDeployment.id) status=\(.latestDeployment.status) at \(.latestDeployment.createdAt)"' <<<"$current"

if [[ "$MODE" == "dry-run" ]]; then
  echo "railway: dry-run, no changes made"
  exit 0
fi

echo "railway: setting source image to $IMAGE"
gql 'mutation($serviceId:String!,$environmentId:String!,$image:String!){
  serviceInstanceUpdate(serviceId:$serviceId, environmentId:$environmentId, input:{source:{image:$image}}) }' \
  "$(jq --arg i "$IMAGE" '. + {image:$i}' <<<"$VARS")" >/dev/null

echo "railway: triggering deploy"
deploy_id=$(gql 'mutation($serviceId:String!,$environmentId:String!){
  serviceInstanceDeployV2(serviceId:$serviceId, environmentId:$environmentId) }' "$VARS" \
  | jq -r '.data.serviceInstanceDeployV2')
[[ -n "$deploy_id" && "$deploy_id" != "null" ]] || { echo "railway: deploy returned no id" >&2; exit 1; }
echo "railway: deployment id=$deploy_id"

deadline=$((SECONDS + TIMEOUT))
status=""
while (( SECONDS < deadline )); do
  status=$(gql 'query($id:String!){ deployment(id:$id){ status } }' "$(jq -n --arg id "$deploy_id" '{id:$id}')" \
    | jq -r '.data.deployment.status')
  echo "railway: $(date -u +%H:%M:%SZ) status=$status"
  case "$status" in
    SUCCESS) break ;;
    FAILED|CRASHED|REMOVED|SKIPPED)
      echo "railway: deployment $deploy_id ended in $status" >&2
      exit 1 ;;
  esac
  sleep 15
done
[[ "$status" == "SUCCESS" ]] || { echo "railway: timed out after ${TIMEOUT}s (last status $status)" >&2; exit 1; }

if [[ -n "$READINESS" ]]; then
  for _ in $(seq 1 20); do
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 15 "$READINESS" || echo 000)
    echo "railway: readiness $READINESS -> $code"
    [[ "$code" == "200" ]] && exit 0
    sleep 10
  done
  echo "railway: readiness never returned 200" >&2
  exit 1
fi
echo "railway: deployed $IMAGE"
