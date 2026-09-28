#!/usr/bin/env bash
# create-confluent-cloud-key.sh
#
# Creates the Confluent Cloud API key Terraform needs (confluent_cloud_api_key /
# confluent_cloud_api_secret in terraform/terraform.tfvars, used by the
# provider block in terraform/versions.tf) and writes it straight into tfvars
# (gitignored, chmod 600). The secret is never printed.
#
# What it creates, following Confluent's recommended practice:
#   - a service account (default name "fleet-terraform", --prefix to namespace it)
#   - an org-scoped OrganizationAdmin role binding for it. Terraform creates an
#     environment, service accounts and role bindings, which needs org-level admin
#   - a Cloud resource management API key ("--resource cloud") owned by it
#
# Then (unless --no-test) calls the Confluent Cloud API with the new key,
# retrying while the key and role binding propagate.
#
# Uses the org you're currently logged in to with the Confluent CLI - switch
# with `confluent login --organization <org-id>` first if you have several.
# Needs the confluent CLI (logged in as an OrganizationAdmin), python3, curl.
#
# This key has full control of the org: delete it (--delete) when you're done,
# but only AFTER `terraform destroy` - Terraform needs it to tear things down.
#
# --user instead creates a Cloud API key owned by YOU (the CLI login) and
# writes it to provider_integration_cloud_api_key / _secret. Terraform uses it
# only to create Tableflow's provider integrations (S3 in terraform/tableflow.tf,
# Azure in terraform/onelake.tf). Confluent employees whose Tableflow bucket is
# in a Confluent-owned AWS account (e.g. the SE account) need this: an
# integration created by a service account can't reach Confluent's own AWS org,
# and every Tableflow sync fails with "Unable to assume the IAM role" (see
# terraform/tableflow.tf). --test-only, --rotate
# and --delete work on that key when combined with --user.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TFVARS="$ROOT/terraform/terraform.tfvars"
TFVARS_EXAMPLE="$ROOT/terraform/terraform.tfvars.example"
SA_NAME="fleet-terraform"
ROLE="OrganizationAdmin"
API="https://api.confluent.cloud"

PREFIX=""
MODE="create"   # create | delete | test
USER_KEY=false
ROTATE=false
TEST=true
ASSUME_YES=false
LAST_CODE=""

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [options]
      create (or reuse) the service account, create a new Cloud API key, write it to tfvars, test it
  $(basename "$0") --test-only [options]
      re-test the key already in tfvars; creates nothing
  $(basename "$0") --delete [options]
      delete the service account, its API keys and its role binding
      (run 'terraform destroy' FIRST - Terraform needs this key to tear down)
  $(basename "$0") --user [--test-only | --rotate | --delete] [options]
      same, but for a Cloud API key owned by YOUR user, written to
      provider_integration_cloud_api_key/_secret (Tableflow's provider
      integrations - needed for a bucket in a Confluent-owned AWS account)

Options:
  --name NAME      service account name (default: $SA_NAME)
  --prefix P       prefix for the service account name, e.g. --prefix rafa -> rafa-$SA_NAME
                   (handy in shared orgs; pass the same prefix to --delete)
  --tfvars PATH    tfvars file to write (default: terraform/terraform.tfvars)
  --user           work on your user-owned provider_integration_cloud_api_key instead of the
                   service account's key (--name/--prefix are ignored)
  --rotate         replace the service account's existing Cloud API key(s) - the old key
                   stops working immediately (with --user: replace the key in tfvars)
  --no-test        skip the test API call
  -y, --yes        don't ask for confirmation
  -h, --help       show this help

Uses the organization you're logged in to with the Confluent CLI ('confluent login').
EOF
}

die() { echo "error: $*" >&2; exit 1; }
log() { echo "==> $*" >&2; }

need_value() { [[ $# -ge 2 && -n $2 ]] || die "$1 needs a value (see --help)"; }

while [[ $# -gt 0 ]]; do
  case $1 in
    --name) need_value "$@"; SA_NAME=$2; shift 2 ;;
    --prefix) need_value "$@"; PREFIX=$2; shift 2 ;;
    --tfvars) need_value "$@"; TFVARS=$2; shift 2 ;;
    --rotate) ROTATE=true; shift ;;
    --no-test) TEST=false; shift ;;
    --test-only) MODE="test"; shift ;;
    --delete) MODE="delete"; shift ;;
    --user) USER_KEY=true; shift ;;
    -y|--yes) ASSUME_YES=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

if [[ -n $PREFIX ]]; then
  SA_NAME="${PREFIX%-}-$SA_NAME"   # tolerate a trailing dash: "rafa-" and "rafa" give the same name
fi
sa_name_re='^[A-Za-z0-9._-]+$'
[[ $SA_NAME =~ $sa_name_re && ${#SA_NAME} -le 64 ]] \
  || die "invalid service account name '$SA_NAME' (allowed: letters, digits and . _ -, max 64 chars)"

# The tfvars variables this run reads/writes.
if $USER_KEY; then
  VAR_KEY="provider_integration_cloud_api_key"
  VAR_SECRET="provider_integration_cloud_api_secret"
else
  VAR_KEY="confluent_cloud_api_key"
  VAR_SECRET="confluent_cloud_api_secret"
fi

confirm() {
  $ASSUME_YES && return 0
  local answer
  read -r -p "$1 [y/N] " answer || return 1
  [[ $answer == y || $answer == Y ]]
}

# Runs a python expression against JSON on stdin (bound to `d`); extra args
# are available as sys.argv[1:].
py_json() {
  local code=$1
  shift
  python3 -c "import sys, json; d = json.load(sys.stdin); $code" "$@"
}

# Reads a quoted string variable from tfvars; prints nothing if absent.
tfvar() {
  [[ -f $TFVARS ]] || return 0
  sed -nE "s/^[[:space:]]*$1[[:space:]]*=[[:space:]]*\"([^\"]*)\".*/\1/p" "$TFVARS" | tail -n 1
}

# Service account ID for $SA_NAME, empty if it doesn't exist.
find_sa() {
  confluent iam service-account list --display-name "$SA_NAME" -o json \
    | py_json 'print(next((s["id"] for s in d if s["name"] == sys.argv[1]), ""))' "$SA_NAME"
}

# API keys owned by the service account ($2 = optional --resource filter).
list_keys() {
  if [[ -n ${2:-} ]]; then
    confluent api-key list --service-account "$1" --resource "$2" -o json
  else
    confluent api-key list --service-account "$1" -o json
  fi | py_json 'print(" ".join(k["key"] for k in d))'
}

has_role() {
  confluent iam rbac role-binding list --principal "User:$1" -o json \
    | py_json 'print("yes" if any(b.get("role") == sys.argv[1] for b in d) else "")' "$ROLE"
}

# Replaces (or appends) the $VAR_KEY / $VAR_SECRET lines in tfvars. The
# secret is passed to awk through the environment, so no sed escaping.
write_tfvars() {
  if [[ ! -f $TFVARS ]]; then
    if [[ -f $TFVARS_EXAMPLE ]]; then
      cp "$TFVARS_EXAMPLE" "$TFVARS"
      log "created $TFVARS from terraform.tfvars.example - fill in bedrock_aws_access_key_id / _secret too"
    else
      : > "$TFVARS"
    fi
  fi
  local tmp
  tmp=$(mktemp "$TFVARS.XXXXXX")
  chmod 600 "$tmp"
  CC_KEY_ID="$KEY_ID" CC_KEY_SECRET="$KEY_SECRET" CC_VAR_KEY="$VAR_KEY" CC_VAR_SECRET="$VAR_SECRET" awk '
    BEGIN {
      id = ENVIRON["CC_KEY_ID"]; secret = ENVIRON["CC_KEY_SECRET"]
      kv = ENVIRON["CC_VAR_KEY"]; sv = ENVIRON["CC_VAR_SECRET"]
      w = length(sv) > length(kv) ? length(sv) : length(kv)   # align the "="s
      key_line = sprintf("%-" w "s = \"%s\"", kv, id)
      secret_line = sprintf("%-" w "s = \"%s\"", sv, secret)
    }
    $0 ~ "^[[:space:]]*" kv "[[:space:]]*=" { print key_line; seen_id = 1; next }
    $0 ~ "^[[:space:]]*" sv "[[:space:]]*=" { print secret_line; seen_secret = 1; next }
    { print }
    END {
      if (!seen_id)     print key_line
      if (!seen_secret) print secret_line
    }' "$TFVARS" > "$tmp"
  mv "$tmp" "$TFVARS"
}

# GET /org/v2/environments with the new key, the same Cloud API the Terraform
# provider uses. Credentials go to curl on stdin (--config -), not argv, so
# they never show up in the process list. Retries for ~90s on 401/403 while
# the new key and role binding propagate.
test_key() {
  local code attempt
  for attempt in $(seq 1 18); do
    code=$(printf 'user = "%s:%s"\n' "$KEY_ID" "$KEY_SECRET" \
      | curl -s -o /dev/null -w '%{http_code}' --config - "$API/org/v2/environments?page_size=1") || code="000"
    [[ $code == 200 ]] && return 0
    [[ $code == 401 || $code == 403 || $code == 000 ]] || break
    [[ $attempt -lt 18 ]] && sleep 5
  done
  LAST_CODE=$code
  return 1
}

run_test() {
  log "testing key $KEY_ID against $API/org/v2/environments (may take up to ~90s while it propagates)"
  if test_key; then
    log "OK - the key authenticates against the Confluent Cloud API"
    return 0
  fi
  case $LAST_CODE in
    401) log "test failed: HTTP 401 - key not recognised (deleted, or still propagating: retry with --test-only in a minute)" ;;
    403) if $USER_KEY; then
           log "test failed: HTTP 403 - key works but lacks permission; your user needs $ROLE (Terraform creates the integration in the environment it creates)"
         else
           log "test failed: HTTP 403 - key works but lacks permission; check the service account has $ROLE"
         fi ;;
    000) log "test failed: couldn't reach $API (network/DNS)" ;;
    *)   log "test failed: HTTP $LAST_CODE from $API" ;;
  esac
  return 2
}

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool not found"
done

if [[ $MODE == test ]]; then
  KEY_ID=$(tfvar "$VAR_KEY")
  KEY_SECRET=$(tfvar "$VAR_SECRET")
  [[ -n $KEY_ID && -n $KEY_SECRET && $KEY_ID != REPLACE_ME ]] \
    || die "no $VAR_KEY/secret in $TFVARS - run without --test-only first"
  run_test
  exit $?
fi

command -v confluent >/dev/null 2>&1 || die "confluent CLI not found - https://docs.confluent.io/confluent-cli/current/install.html"
org=$(confluent organization describe -o json 2>/dev/null) || die "not logged in to Confluent Cloud - run: confluent login"
read -r ORG_ID ORG_NAME <<<"$(py_json 'print(d["id"], d.get("name", ""))' <<<"$org")"

# Only the key recorded in tfvars is touched, never other keys you own.
if $USER_KEY; then
  LOGIN=$(confluent context describe --username 2>/dev/null || true)
  OLD_KEY=$(tfvar "$VAR_KEY")
  if [[ $OLD_KEY == REPLACE_ME ]]; then OLD_KEY=""; fi
fi

# Clearing the tfvars lines makes Terraform fall back to confluent_cloud_api_key,
# and any provider integration it recreates after that breaks Tableflow again.
if $USER_KEY && [[ $MODE == delete ]]; then
  [[ -n $OLD_KEY ]] || die "no $VAR_KEY in $TFVARS - nothing to delete"
  log "run this after 'terraform destroy' - a provider integration recreated with the service-account key breaks Tableflow again"
  confirm "Delete your Cloud API key $OLD_KEY and clear $VAR_KEY/$VAR_SECRET in $TFVARS?" || die "aborted"
  confluent api-key delete "$OLD_KEY" --force >/dev/null
  log "deleted API key $OLD_KEY"
  KEY_ID=""
  KEY_SECRET=""
  write_tfvars
  log "cleared $VAR_KEY / $VAR_SECRET in $TFVARS - Terraform falls back to confluent_cloud_api_key"
  exit 0
fi

if [[ $MODE == delete ]]; then
  SA_ID=$(find_sa)
  [[ -n $SA_ID ]] || die "service account '$SA_NAME' not found in org $ORG_NAME ($ORG_ID)"
  log "make sure you've run 'terraform destroy' first - Terraform needs this key to tear down what it created"
  confirm "Delete service account '$SA_NAME' ($SA_ID), all its API keys and its $ROLE binding in org $ORG_NAME ($ORG_ID)?" || die "aborted"
  tf_key=$(tfvar confluent_cloud_api_key)
  stale=false
  for key in $(list_keys "$SA_ID"); do
    confluent api-key delete "$key" --force >/dev/null
    log "deleted API key $key"
    [[ $key == "$tf_key" ]] && stale=true
  done
  confluent iam rbac role-binding delete --principal "User:$SA_ID" --role "$ROLE" --force >/dev/null 2>&1 || true
  confluent iam service-account delete "$SA_ID" --force >/dev/null
  log "deleted service account '$SA_NAME' ($SA_ID)"
  if $stale; then
    log "$TFVARS still holds deleted key $tf_key - Terraform can't manage this deployment until you set a new one"
  fi
  exit 0
fi

if [[ -z $(confluent iam rbac role-binding list --current-user -o json 2>/dev/null \
  | py_json 'print("yes" if any(b.get("role") == "OrganizationAdmin" for b in d) else "")' 2>/dev/null) ]]; then
  if $USER_KEY; then
    log "warning: your CLI login has no direct OrganizationAdmin role binding - Terraform will likely fail to create the provider integration in the environment it creates"
  else
    log "warning: your CLI login has no direct OrganizationAdmin role binding - granting $ROLE will likely fail"
  fi
fi

if $USER_KEY; then
  if [[ $LOGIN != *@confluent.io ]]; then
    log "warning: logged in as '${LOGIN:-unknown}', not an @confluent.io user - a user-owned key only matters for Confluent employees (see --help)"
  fi
  if [[ -n $OLD_KEY ]] && ! $ROTATE; then
    die "$TFVARS already has $VAR_KEY ($OLD_KEY) - re-run with --rotate to replace it"
  fi

  log "org:    $ORG_NAME ($ORG_ID)"
  log "owner:  ${LOGIN:-your CLI login} (user-owned Cloud API key, used only for Tableflow's provider integrations)"
  log "tfvars: $TFVARS"
  confirm "Create a Cloud API key owned by you and write it into tfvars?" || die "aborted"

  # No --service-account: the key is owned by the logged-in user.
  creds=$(confluent api-key create --resource cloud \
    --description "Terraform (provider_integration_cloud_api_key) for the fleet telemetry demo's Tableflow provider integrations" -o json)
  read -r KEY_ID KEY_SECRET <<<"$(py_json 'print(d.get("api_key") or d.get("key", ""), d.get("api_secret") or d.get("secret", ""))' <<<"$creds")"
  unset creds
  [[ -n $KEY_ID && -n $KEY_SECRET ]] || die "api-key create returned no key"
  log "created Cloud API key $KEY_ID"

  write_tfvars
  log "wrote $VAR_KEY / $VAR_SECRET to $TFVARS (chmod 600)"

  if [[ -n $OLD_KEY ]]; then
    if confluent api-key delete "$OLD_KEY" --force >/dev/null; then
      log "deleted old API key $OLD_KEY (--rotate)"
    else
      log "warning: couldn't delete old API key $OLD_KEY - delete it in the Cloud Console"
    fi
  fi

  if $TEST; then
    run_test || exit $?
  fi
  log "done - Terraform now creates Tableflow's provider integrations with this key. If the S3 one already exists, recreate it: terraform apply -replace=confluent_provider_integration.tableflow_s3"
  exit 0
fi

log "org:             $ORG_NAME ($ORG_ID)"
log "service account: $SA_NAME  (role: $ROLE, org-wide)"
log "tfvars:          $TFVARS"
confirm "Create/update this service account and write a new Cloud API key into tfvars?" || die "aborted"

SA_ID=$(find_sa)
if [[ -n $SA_ID ]]; then
  log "service account '$SA_NAME' already exists ($SA_ID) - reusing it"
else
  SA_ID=$(confluent iam service-account create "$SA_NAME" \
    --description "Terraform admin for the fleet telemetry demo (scripts/create-confluent-cloud-key.sh)" -o json \
    | py_json 'print(d["id"])')
  log "created service account '$SA_NAME' ($SA_ID)"
fi

if [[ -n $(has_role "$SA_ID") ]]; then
  log "$ROLE role binding already present"
else
  confluent iam rbac role-binding create --principal "User:$SA_ID" --role "$ROLE" -o json >/dev/null
  log "granted $ROLE to $SA_ID"
fi

existing=$(list_keys "$SA_ID" cloud)
if [[ -n $existing ]] && ! $ROTATE; then
  die "'$SA_NAME' already has Cloud API key(s): $existing - re-run with --rotate to replace them"
fi

# The new key is created before any old one is deleted, so a failure here
# never leaves Terraform without a working key.
creds=$(confluent api-key create --resource cloud --service-account "$SA_ID" \
  --description "Terraform (confluent_cloud_api_key) for the fleet telemetry demo" -o json)
read -r KEY_ID KEY_SECRET <<<"$(py_json 'print(d.get("api_key") or d.get("key", ""), d.get("api_secret") or d.get("secret", ""))' <<<"$creds")"
unset creds
[[ -n $KEY_ID && -n $KEY_SECRET ]] || die "api-key create returned no key"
log "created Cloud API key $KEY_ID"

write_tfvars
log "wrote confluent_cloud_api_key / confluent_cloud_api_secret to $TFVARS (chmod 600)"

for key in $existing; do
  confluent api-key delete "$key" --force >/dev/null
  log "deleted old API key $key (--rotate)"
done

if $TEST; then
  run_test || exit $?
fi
log "done - next: README Setup Steps 1-2 (Bedrock setup, then terraform apply)"
log "Confluent employees with Tableflow's bucket in a Confluent-owned AWS account: also run $(basename "$0") --user"
