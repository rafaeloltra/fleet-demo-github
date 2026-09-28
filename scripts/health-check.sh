#!/usr/bin/env bash
# health-check.sh
#
# Read-only health check of the deployed demo, end to end. One section per
# layer, each check printed as PASS / WARN / FAIL / SKIP:
#
#   local      terraform.tfvars, fleet-context-engine/.env and state files are
#              gitignored and untracked, secret files are chmod 600, the .env
#              values still match the terraform outputs
#   confluent  Kafka cluster, the demo's topics and their Schema Registry subjects
#   flink      compute pool; statements (CREATEs COMPLETED, INSERTs RUNNING,
#              no recent exceptions); the Bedrock connection
#   dataflow   records received per topic over METRICS_WINDOW (Metrics API)
#   rtce       RTCE topics ACTIVE; MCP listTopics and queryData with the
#              Global key the console's chat uses
#   tableflow  Tableflow topics RUNNING and synced; catalog integrations CONNECTED
#   aws        fresh Iceberg metadata under each table path in the S3 bucket;
#              the Glue tables (Glue needs AWS credentials: AWS_PROFILE or
#              --aws-profile)
#   bedrock    a 5-token Converse call with the Bedrock IAM user (the key
#              Flink's connection and the chat both use)
#   fabric     lakehouse shortcuts and each table's Delta conversion status
#              (needs terraform-fabric/ applied and a current `az login`)
#   engine     fleet-context-engine at ENGINE_URL: live mode, consumer caught
#              up, and it doesn't serve tfvars, tfstate or .env
#
# Opt-in extras: --with-sql (terraform-fabric/queries/validate.sql via sqlcmd),
# --with-ask (one question through the console's chat: RTCE + Bedrock), and
# --with-plan (terraform plan -detailed-exitcode, to spot drift).
#
# Every ID, endpoint and key comes from `terraform output` in TF_DIR and
# FABRIC_TF_DIR, plus the Cloud API key and Bedrock key in terraform.tfvars.
# Nothing is hardcoded, so it works for any deployment of this repo. It
# creates and changes nothing, and never prints a secret: credentials reach
# curl as -K config through a pipe, and the aws CLI through its environment.
#
# Needs terraform, jq and curl; aws for the aws/bedrock sections, az for
# fabric, sqlcmd for --with-sql. Reference, and what each WARN/FAIL means:
# docs/health-check.md.
#
# Exit status: 0 if nothing failed (warnings allowed, unless --strict), 1 if
# something did, 2 on bad usage or missing prerequisites.

set -uo pipefail   # not -e: a failing check is reported and the run carries on

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${TF_DIR:-$ROOT/terraform}"
FABRIC_TF_DIR="${FABRIC_TF_DIR:-$ROOT/terraform-fabric}"
ENV_FILE="${ENV_FILE:-$ROOT/fleet-context-engine/.env}"
ENGINE_URL="${ENGINE_URL:-http://localhost:8787}"
METRICS_WINDOW="${METRICS_WINDOW:-PT15M}"      # ISO-8601 duration
STALE_MINUTES="${STALE_MINUTES:-30}"           # Tableflow commits low-volume topics ~every 15 min
EXCEPTION_MINUTES="${EXCEPTION_MINUTES:-60}"   # Flink exceptions newer than this are WARNs
CURL_TIMEOUT="${CURL_TIMEOUT:-30}"
EXPECTED_TOPICS="${EXPECTED_TOPICS:-}"         # default: the RTCE topics in terraform state
RTCE_PROBE_TOPIC="${RTCE_PROBE_TOPIC:-}"       # default: vehicle.telemetry if expected, else the first
ASK_QUESTION="${ASK_QUESTION:-How many vehicles are reporting telemetry right now?}"

API="https://api.confluent.cloud"
METRICS_API="https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
FABRIC_API="https://api.fabric.microsoft.com/v1"
ONELAKE_DFS="https://onelake.dfs.fabric.microsoft.com"
ALL_SECTIONS="local confluent flink dataflow rtce tableflow aws bedrock fabric engine"

SKIP_SECTIONS=""
ONLY_SECTIONS=""
WITH_SQL=false
WITH_ASK=false
WITH_PLAN=false
STRICT=false
VERBOSE=false
COLOR=true

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Checks every layer of the deployed demo (read-only) and prints PASS / WARN /
FAIL per check, then a summary. Sections: $ALL_SECTIONS

Options:
  --skip SECTION     skip a section (repeatable, or comma-separated)
  --only SECTION     run only these sections (repeatable, or comma-separated)
  --with-sql         also run terraform-fabric/queries/validate.sql through sqlcmd
  --with-ask         also ask the console's chat one question (/api/ask: RTCE + Bedrock)
  --with-plan        also run terraform plan -detailed-exitcode to spot drift (slow;
                     needs the same AWS credentials as terraform apply)
  --aws-profile P    AWS profile for the Glue check (default: AWS_PROFILE, else the
                     default credential chain)
  --strict           exit 1 on warnings as well as failures
  -v, --verbose      print details under each check
  --no-color         plain output (also: NO_COLOR=1)
  -h, --help         show this help

Environment (defaults in brackets):
  TF_DIR             main Terraform root [terraform/]
  FABRIC_TF_DIR      Fabric Terraform root [terraform-fabric/]
  ENV_FILE           console .env [fleet-context-engine/.env]
  ENGINE_URL         where the console runs [$ENGINE_URL]
  METRICS_WINDOW     look-back for records per topic, ISO-8601 [$METRICS_WINDOW]
  STALE_MINUTES      Tableflow data older than this is a WARN [$STALE_MINUTES]
  EXCEPTION_MINUTES  Flink exceptions newer than this are a WARN [$EXCEPTION_MINUTES]
  CURL_TIMEOUT       seconds per API call [$CURL_TIMEOUT]
  EXPECTED_TOPICS    space-separated topics [the RTCE topics in terraform state]
  RTCE_PROBE_TOPIC   topic for the RTCE queryData probe [vehicle.telemetry]
  ASK_QUESTION       question for --with-ask

Examples:
  $(basename "$0")                         # everything but the opt-in extras
  $(basename "$0") --only flink,dataflow -v
  $(basename "$0") --skip fabric,bedrock
  AWS_PROFILE=my-profile $(basename "$0") --with-plan
EOF
}

die() { echo "error: $*" >&2; exit 2; }

need_value() { [[ $# -ge 2 && -n $2 ]] || die "$1 needs a value (see --help)"; }

check_sections() {
  local s
  for s in ${1//,/ }; do
    case " $ALL_SECTIONS " in *" $s "*) ;; *) die "unknown section '$s' (sections: $ALL_SECTIONS)" ;; esac
  done
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --skip) need_value "$@"; check_sections "$2"; SKIP_SECTIONS="$SKIP_SECTIONS ${2//,/ }"; shift 2 ;;
    --only) need_value "$@"; check_sections "$2"; ONLY_SECTIONS="$ONLY_SECTIONS ${2//,/ }"; shift 2 ;;
    --with-sql) WITH_SQL=true; shift ;;
    --with-ask) WITH_ASK=true; shift ;;
    --with-plan) WITH_PLAN=true; shift ;;
    --aws-profile) need_value "$@"; export AWS_PROFILE=$2; shift 2 ;;
    --strict) STRICT=true; shift ;;
    -v|--verbose) VERBOSE=true; shift ;;
    --no-color) COLOR=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

enabled() {
  case " $SKIP_SECTIONS " in *" $1 "*) return 1 ;; esac
  [[ -z $ONLY_SECTIONS ]] && return 0
  case " $ONLY_SECTIONS " in *" $1 "*) return 0 ;; esac
  return 1
}

for cmd in terraform jq curl; do
  command -v "$cmd" >/dev/null 2>&1 || die "$cmd is required"
done

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# --- output --------------------------------------------------------------------

if $COLOR && [[ -t 1 && -z ${NO_COLOR:-} ]]; then
  C_PASS=$'\033[32m' C_WARN=$'\033[33m' C_FAIL=$'\033[31m' C_DIM=$'\033[2m' C_BOLD=$'\033[1m' C_OFF=$'\033[0m'
else
  C_PASS="" C_WARN="" C_FAIL="" C_DIM="" C_BOLD="" C_OFF=""
fi

PASSES=0 WARNS=0 FAILS=0 SKIPS=0
PROBLEMS=""
SECTION=""

section() { SECTION=$1; printf '\n%s%s%s %s%s%s\n' "$C_BOLD" "$1" "$C_OFF" "$C_DIM" "$2" "$C_OFF"; }
pass() { PASSES=$((PASSES + 1)); printf '  %sPASS%s %s\n' "$C_PASS" "$C_OFF" "$*"; }
warn() {
  WARNS=$((WARNS + 1)); PROBLEMS="$PROBLEMS  ${C_WARN}WARN${C_OFF} [$SECTION] $*"$'\n'
  printf '  %sWARN%s %s\n' "$C_WARN" "$C_OFF" "$*"
}
fail() {
  FAILS=$((FAILS + 1)); PROBLEMS="$PROBLEMS  ${C_FAIL}FAIL${C_OFF} [$SECTION] $*"$'\n'
  printf '  %sFAIL%s %s\n' "$C_FAIL" "$C_OFF" "$*"
}
skip() { SKIPS=$((SKIPS + 1)); printf '  %sSKIP%s %s\n' "$C_DIM" "$C_OFF" "$*"; }

# Indents stdin under the last check; only with --verbose.
detail() {
  if $VERBOSE; then sed "s/^/       /"; else cat >/dev/null; fi
}

words() { set -f; set -- $1; set +f; echo $#; }

# Items of list $1 that aren't in list $2 (both space-separated).
missing_from() {
  local item out=""
  for item in $1; do
    case " $2 " in *" $item "*) ;; *) out="$out $item" ;; esac
  done
  printf '%s' "${out# }"
}

# Minutes since an ISO-8601 UTC timestamp ("Z" or "+00:00", optional
# fractional seconds); prints nothing if it can't be parsed.
age_min() {
  jq -rn --arg t "$1" '$t | sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z")
    | try ((now - fromdateiso8601) / 60 | floor) catch empty'
}

# --- HTTP ----------------------------------------------------------------------
# Each call sets CODE (HTTP status, 000 if curl itself failed) and BODY.
# Credentials go in a curl -K config read from a pipe, never argv; quotes and
# backslashes are escaped for the config syntax.

cfg_quote() { local v=$1; v=${v//\\/\\\\}; v=${v//\"/\\\"}; printf '"%s"' "$v"; }

_http() {
  local cfg=$1 resp
  shift
  resp=$(curl -sS --max-time "$CURL_TIMEOUT" -w '\n%{http_code}' -K <(printf '%s\n' "$cfg") "$@" 2>&1)
  CODE=${resp##*$'\n'}
  BODY=${resp%$'\n'*}
}

basic() { local u=$1 p=$2; shift 2; _http "user = $(cfg_quote "$u:$p")" "$@"; }
bearer() { local t=$1; shift; _http "header = $(cfg_quote "Authorization: Bearer $t")" "$@"; }
anon() { _http "" "$@"; }

ok() { [[ $CODE == 2* ]]; }

# One-line summary of a failed call, for FAIL/WARN messages.
http_err() {
  local msg
  msg=$(jq -r '[.errors[]? | (.detail // .title // empty)] + [.error.message? // .error? // .message? // empty | strings] | first // empty' \
    <<<"$BODY" 2>/dev/null)
  [[ -n $msg ]] || msg=$BODY
  printf 'HTTP %s: %s' "$CODE" "$(printf '%s' "$msg" | tr -s '\r\n\t ' ' ' | cut -c1-180)"
}

# --- configuration -------------------------------------------------------------

TFVARS="$TF_DIR/terraform.tfvars"

# A quoted string variable from terraform.tfvars; nothing if absent. Never printed.
tfvar() {
  [[ -f $TFVARS ]] || return 0
  sed -nE "s/^[[:space:]]*$1[[:space:]]*=[[:space:]]*\"([^\"]*)\".*/\1/p" "$TFVARS" | tail -n 1
}

# A variable from the console's .env (optional export, optional quotes); nothing if absent.
envvar() {
  [[ -f $ENV_FILE ]] || return 0
  sed -nE "s/^[[:space:]]*(export[[:space:]]+)?$1[[:space:]]*=(.*)$/\2/p" "$ENV_FILE" | tail -n 1 \
    | sed -E "s/[[:space:]]+\$//; s/^\"(.*)\"\$/\1/; s/^'(.*)'\$/\1/"
}

TF_OUT=$(terraform -chdir="$TF_DIR" output -json 2>"$TMP/tf.err") \
  || die "terraform output failed in $TF_DIR: $(head -n 3 "$TMP/tf.err")"
[[ $(jq 'length' <<<"$TF_OUT") -gt 0 ]] || die "no terraform outputs in $TF_DIR - has it been applied?"

# A terraform output (strings raw, maps/lists as compact JSON); nothing if absent.
out() { jq -r --arg k "$1" '.[$k].value // empty | if type == "string" then . else tojson end' <<<"$TF_OUT"; }

ENV_ID=$(out flink_environment_id)
ORG_ID=$(out flink_organization_id)
LKC=$(out kafka_cluster_id)
FLINK_REST=$(out flink_rest_endpoint)
BEDROCK_REGION=$(out bedrock_region)
BEDROCK_MODEL=$(out bedrock_model_id)
[[ -n $ENV_ID && -n $LKC ]] || die "terraform outputs in $TF_DIR lack flink_environment_id / kafka_cluster_id"

if [[ -z $EXPECTED_TOPICS ]]; then
  EXPECTED_TOPICS=$(terraform -chdir="$TF_DIR" state list 2>/dev/null \
    | sed -nE 's/^confluent_rtce_topic\.fleet\["([^"]+)"\]$/\1/p' | sort | tr '\n' ' ')
  EXPECTED_TOPICS=${EXPECTED_TOPICS% }
fi
[[ -n $EXPECTED_TOPICS ]] \
  || die "no confluent_rtce_topic.fleet resources in terraform state - set EXPECTED_TOPICS to the demo's topics"
TABLEFLOW_TOPICS=$(jq -r '.tableflow_table_paths.value // {} | keys[]' <<<"$TF_OUT" | tr '\n' ' ')
TABLEFLOW_TOPICS=${TABLEFLOW_TOPICS% }

CLOUD_KEY=$(tfvar confluent_cloud_api_key)
CLOUD_SECRET=$(tfvar confluent_cloud_api_secret)
have_cloud_key() { [[ -n $CLOUD_KEY && -n $CLOUD_SECRET ]]; }

printf '%sFleet demo health check%s  %s\n' "$C_BOLD" "$C_OFF" "$(date -u '+%Y-%m-%d %H:%M UTC')"
printf '  environment %s, cluster %s (%s), %s expected topics\n' \
  "$ENV_ID" "$LKC" "$(sed -nE 's#^https://flink\.([^.]+)\..*#\1#p' <<<"$FLINK_REST")" "$(words "$EXPECTED_TOPICS")"

# --- local ---------------------------------------------------------------------

IN_GIT=false
git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 && IN_GIT=true

# check_file PATH LABEL REQUIRED(true|false) SECRET(true|false: must be chmod 600)
check_file() {
  local f=$1 label=$2 required=$3 secret=$4 rel mode
  rel=${f#"$ROOT"/}
  if [[ ! -f $f ]]; then
    if $required; then fail "$label missing: $rel"; else skip "$label: $rel not present"; fi
    return
  fi
  if $IN_GIT; then
    if git -C "$ROOT" ls-files --error-unmatch "$f" >/dev/null 2>&1; then
      fail "$label $rel is TRACKED by git (git rm --cached it, then rotate what's in it)"
      return
    fi
    git -C "$ROOT" check-ignore -q "$f" || { fail "$label $rel is not gitignored"; return; }
  fi
  mode=$(stat -c %a "$f" 2>/dev/null || stat -f %Lp "$f" 2>/dev/null)
  if $secret && [[ $mode != 600 && $mode != 400 ]]; then
    warn "$label $rel is mode $mode (chmod 600 it)"
  elif $secret; then
    pass "$label $rel: mode $mode, gitignored, untracked"
  else
    pass "$label $rel: gitignored, untracked"
  fi
}

# .env variable -> where its value comes from (terraform output, or tfvars:<name>).
ENV_SOURCES="KAFKA_BOOTSTRAP_ENDPOINT=kafka_bootstrap_endpoint
KAFKA_REST_ENDPOINT=kafka_rest_endpoint
KAFKA_CLUSTER_ID=kafka_cluster_id
KAFKA_API_KEY=app_manager_kafka_api_key
KAFKA_API_SECRET=app_manager_kafka_api_secret
SCHEMA_REGISTRY_ENDPOINT=schema_registry_rest_endpoint
SCHEMA_REGISTRY_API_KEY=app_manager_schema_registry_api_key
SCHEMA_REGISTRY_API_SECRET=app_manager_schema_registry_api_secret
RTCE_URL=rtce_mcp_url
RTCE_API_KEY=rtce_api_key
RTCE_API_SECRET=rtce_api_secret
BEDROCK_AWS_ACCESS_KEY_ID=tfvars:bedrock_aws_access_key_id
BEDROCK_AWS_SECRET_ACCESS_KEY=tfvars:bedrock_aws_secret_access_key
BEDROCK_REGION=?bedrock_region"

check_local() {
  section local "config files and secrets"
  check_file "$TFVARS" "terraform tfvars" true true
  check_file "$TF_DIR/terraform.tfstate" "terraform state" false false
  check_file "$ENV_FILE" "console .env" true true
  check_file "$FABRIC_TF_DIR/terraform.tfvars" "fabric tfvars" false true
  check_file "$FABRIC_TF_DIR/terraform.tfstate" "fabric state" false false

  local v missing=""
  for v in confluent_cloud_api_key confluent_cloud_api_secret bedrock_aws_access_key_id bedrock_aws_secret_access_key; do
    [[ -n $(tfvar "$v") ]] || missing="$missing $v"
  done
  if [[ -n $missing ]]; then fail "terraform.tfvars has no value for:$missing"; else pass "terraform.tfvars has the Cloud API and Bedrock keys"; fi

  [[ -f $ENV_FILE ]] || return
  local line name src optional want have empty="" differs="" checked=0
  while IFS= read -r line; do
    name=${line%%=*} src=${line#*=} optional=false
    if [[ $src == \?* ]]; then optional=true; src=${src#\?}; fi
    have=$(envvar "$name")
    if [[ -z $have ]]; then
      $optional || empty="$empty $name"
      continue
    fi
    if [[ $src == tfvars:* ]]; then want=$(tfvar "${src#tfvars:}"); else want=$(out "$src"); fi
    checked=$((checked + 1))
    [[ -z $want || $have == "$want" ]] || differs="$differs $name(${src#tfvars:})"
  done <<<"$ENV_SOURCES"
  [[ -z $empty ]] || fail ".env has no value for:$empty (LIVE mode and the chat need them)"
  if [[ -n $differs ]]; then
    warn ".env differs from terraform for:$differs - regenerate it from the outputs (fleet-context-engine/SETUP.md)"
  elif [[ -z $empty ]]; then
    pass ".env: all $checked values match the terraform outputs / tfvars"
  fi
}

# --- confluent -------------------------------------------------------------------

check_confluent() {
  section confluent "Kafka cluster, topics, Schema Registry"
  if have_cloud_key; then
    basic "$CLOUD_KEY" "$CLOUD_SECRET" "$API/cmk/v2/clusters/$LKC?environment=$ENV_ID"
    if ok; then
      local phase
      phase=$(jq -r '.status.phase' <<<"$BODY")
      if [[ $phase == PROVISIONED ]]; then
        pass "cluster $LKC: $(jq -r '"\(.spec.config.kind) \(.spec.cloud) \(.spec.region)"' <<<"$BODY"), $phase"
      else
        fail "cluster $LKC is $phase"
      fi
    else
      fail "cluster lookup: $(http_err)"
    fi
  else
    skip "cluster status: no confluent_cloud_api_key in terraform.tfvars"
  fi

  basic "$(out app_manager_kafka_api_key)" "$(out app_manager_kafka_api_secret)" \
    "$(out kafka_rest_endpoint)/kafka/v3/clusters/$LKC/topics"
  if ok; then
    local topics miss
    topics=$(jq -r '[.data[] | select(.is_internal | not) | .topic_name] | sort | join(" ")' <<<"$BODY")
    miss=$(missing_from "$EXPECTED_TOPICS" "$topics")
    if [[ -n $miss ]]; then fail "topics missing: $miss"; else pass "all $(words "$EXPECTED_TOPICS") topics exist (Kafka REST, app-manager key)"; fi
    printf '%s\n' $topics | detail
  else
    fail "Kafka REST topic list: $(http_err)"
  fi

  basic "$(out app_manager_schema_registry_api_key)" "$(out app_manager_schema_registry_api_secret)" \
    "$(out schema_registry_rest_endpoint)/subjects"
  if ok; then
    local subjects want="" t miss
    subjects=$(jq -r 'join(" ")' <<<"$BODY")
    for t in $EXPECTED_TOPICS; do want="$want $t-value"; done
    miss=$(missing_from "$want" "$subjects")
    if [[ -n $miss ]]; then fail "Schema Registry subjects missing: $miss"; else pass "all $(words "$want") <topic>-value subjects registered"; fi
  else
    fail "Schema Registry subjects: $(http_err)"
  fi
}

# --- flink -----------------------------------------------------------------------

check_flink() {
  section flink "compute pool, statements, Bedrock connection"
  local pool fk fs
  pool=$(out flink_compute_pool_id)
  fk=$(out flink_api_key) fs=$(out flink_api_secret)

  if have_cloud_key; then
    basic "$CLOUD_KEY" "$CLOUD_SECRET" "$API/fcpm/v2/compute-pools/$pool?environment=$ENV_ID"
    if ok; then
      local phase cur max
      phase=$(jq -r '.status.phase' <<<"$BODY") cur=$(jq -r '.status.current_cfu // 0' <<<"$BODY") max=$(jq -r '.spec.max_cfu' <<<"$BODY")
      if [[ $phase != PROVISIONED ]]; then
        fail "compute pool $pool is $phase"
      elif [[ $cur -ge $max ]]; then
        warn "compute pool $pool at its max CFU ($cur/$max) - statements may fall behind (raise flink_cfu)"
      else
        pass "compute pool $pool: $phase, $cur/$max CFU"
      fi
    else
      fail "compute pool lookup: $(http_err)"
    fi
  else
    skip "compute pool: no confluent_cloud_api_key in terraform.tfvars"
  fi

  # All statements in the environment (paged), then only the ones Terraform
  # runs as the flink_runner service account.
  local url="$FLINK_REST/sql/v1/organizations/$ORG_ID/environments/$ENV_ID/statements?page_size=100" all='[]'
  while [[ -n $url ]]; do
    basic "$fk" "$fs" "$url"
    ok || { fail "Flink statement list: $(http_err)"; return; }
    all=$(jq -c --argjson acc "$all" '$acc + .data' <<<"$BODY")
    url=$(jq -r '.metadata.next // empty' <<<"$BODY")
  done
  jq -c --arg p "$(out flink_principal_id)" '[.[] | select(.spec.principal == $p) | {
      name,
      phase: .status.phase,
      detail: (.status.detail // "" | gsub("\\s+"; " ")),
      kind: (.spec.statement | ascii_upcase | capture("^\\s*(?<k>[A-Z]+\\s+[A-Z]+)").k // "OTHER" | gsub("\\s+"; " ")),
      ai: (.spec.statement | test("ML_PREDICT"; "i")),
      target: (.spec.statement
        | capture("(?i)^\\s*(create\\s+(table|model)(\\s+if\\s+not\\s+exists)?|insert\\s+into)\\s+(?<id>\\S+)").id // ""
        | (capture("`(?<t>[^`]+)`$").t // (split(".") | last)))
    }]' <<<"$all" >"$TMP/statements.json"

  local n creates inserts ai bad
  n=$(jq 'length' "$TMP/statements.json")
  if [[ $n -eq 0 ]]; then
    fail "no Flink statements for the flink_runner principal - has terraform created them?"
    return
  fi
  creates=$(jq '[.[] | select(.kind | startswith("CREATE"))] | length' "$TMP/statements.json")
  inserts=$(jq '[.[] | select(.kind == "INSERT INTO")] | length' "$TMP/statements.json")
  ai=$(jq '[.[] | select(.kind == "INSERT INTO" and .ai)] | length' "$TMP/statements.json")
  bad=$(jq -r '.[] | select(
      ((.kind | startswith("CREATE")) and .phase != "COMPLETED")
      or (.kind == "INSERT INTO" and .phase != "RUNNING")
      or (((.kind | startswith("CREATE") or . == "INSERT INTO") | not) and (.phase | test("FAIL|DEGRADED")))
    ) | "\(.kind) \(.target): \(.phase)\(if .detail != "" then " - " + .detail[0:160] else "" end)"' "$TMP/statements.json")
  if [[ -n $bad ]]; then
    while IFS= read -r line; do
      case $line in *": PENDING"*) warn "statement $line" ;; *) fail "statement $line" ;; esac
    done <<<"$bad"
  else
    pass "$creates CREATE statements COMPLETED, $inserts INSERT statements RUNNING ($ai calling Bedrock via ML_PREDICT)"
  fi
  jq -r '.[] | "\(.phase)\t\(.kind) \(.target)\t\(.name)"' "$TMP/statements.json" | sort | detail

  local dup
  dup=$(jq -r '[.[] | select(.kind == "INSERT INTO" and .phase == "RUNNING")] | group_by(.target)
    | map(select(length > 1) | "\(.[0].target) (\(length))") | join(", ")' "$TMP/statements.json")
  [[ -z $dup ]] || warn "more than one RUNNING INSERT into: $dup - duplicate writes (a leftover statement?)"

  # Recent exceptions on the running INSERTs (e.g. Bedrock throttling or auth).
  local name target exc_count=0 recent age msg
  while IFS=$'\t' read -r name target; do
    [[ -n $name ]] || continue
    basic "$fk" "$fs" "$FLINK_REST/sql/v1/organizations/$ORG_ID/environments/$ENV_ID/statements/$name/exceptions"
    ok || { warn "exceptions for INSERT INTO $target: $(http_err)"; continue; }
    recent=$(jq -c '(.data // []) | sort_by(.timestamp // "") | last // empty' <<<"$BODY")
    [[ -n $recent ]] || continue
    age=$(age_min "$(jq -r '.timestamp // empty' <<<"$recent")")
    msg=$(jq -r '.message // .name // "" | gsub("\\s+"; " ") | .[0:160]' <<<"$recent")
    if [[ -z $age || $age -le $EXCEPTION_MINUTES ]]; then
      exc_count=$((exc_count + 1))
      warn "INSERT INTO $target: exception ${age:-?} min ago: $msg"
    else
      echo "INSERT INTO $target: last exception $age min ago: $msg" | detail
    fi
  done < <(jq -r '.[] | select(.kind == "INSERT INTO" and .phase == "RUNNING") | "\(.name)\t\(.target)"' "$TMP/statements.json")
  [[ $exc_count -gt 0 ]] || pass "no exceptions on the running INSERTs in the last $EXCEPTION_MINUTES min"

  basic "$fk" "$fs" "$FLINK_REST/sql/v1/organizations/$ORG_ID/environments/$ENV_ID/connections"
  if ok; then
    local conn want_ep
    want_ep="https://bedrock-runtime.$BEDROCK_REGION.amazonaws.com/model/$BEDROCK_MODEL/invoke"
    conn=$(jq -r '[.data[] | select(.spec.connection_type == "BEDROCK")] | first // empty | "\(.name)\t\(.spec.endpoint)"' <<<"$BODY")
    if [[ -z $conn ]]; then
      fail "no BEDROCK Flink connection in the environment (terraform/flink.tf)"
    elif [[ ${conn#*$'\t'} != "$want_ep" ]]; then
      warn "Bedrock connection ${conn%%$'\t'*} points at ${conn#*$'\t'}, terraform expects $want_ep"
    else
      pass "Bedrock connection ${conn%%$'\t'*}: $BEDROCK_MODEL in $BEDROCK_REGION"
    fi
  else
    fail "Flink connection list: $(http_err)"
  fi
}

# --- dataflow --------------------------------------------------------------------

check_dataflow() {
  section dataflow "records received per topic, last $METRICS_WINDOW"
  if ! have_cloud_key; then skip "Metrics API: no confluent_cloud_api_key in terraform.tfvars"; return; fi
  local q
  q=$(jq -nc --arg lkc "$LKC" --arg iv "$METRICS_WINDOW/now|m" '{
    aggregations: [{metric: "io.confluent.kafka.server/received_records"}],
    filter: {field: "resource.kafka.id", op: "EQ", value: $lkc},
    granularity: "ALL", group_by: ["metric.topic"], intervals: [$iv], limit: 1000}')
  basic "$CLOUD_KEY" "$CLOUD_SECRET" -H 'Content-Type: application/json' -X POST "$METRICS_API" -d "$q"
  ok || { fail "Metrics API query: $(http_err)"; return; }

  local t n total=0 live=0 idle=""
  for t in $EXPECTED_TOPICS; do
    n=$(jq --arg t "$t" '[.data[] | select(.["metric.topic"] == $t) | .value] | add // 0 | floor' <<<"$BODY")
    total=$((total + n))
    if [[ $n -gt 0 ]]; then live=$((live + 1)); else idle="$idle $t"; fi
    printf '%10s  %s\n' "$n" "$t" | detail
  done
  if [[ $live -eq 0 ]]; then
    warn "no records on any topic in the last $METRICS_WINDOW - start the console (npm start) or seed data"
  else
    pass "$live/$(words "$EXPECTED_TOPICS") topics received records ($total in the last $METRICS_WINDOW)"
    [[ -z $idle ]] || warn "no records in the last $METRICS_WINDOW on:$idle"
  fi
}

# --- rtce ------------------------------------------------------------------------

MCP_SESSION=""

# mcp JSON-RPC-body: POSTs to the RTCE MCP endpoint; BODY is the JSON-RPC
# response (unwrapped from its SSE "data:" line).
mcp() {
  local sess=() sid
  [[ -n $MCP_SESSION ]] && sess=(-H "Mcp-Session-Id: $MCP_SESSION")
  basic "$(out rtce_api_key)" "$(out rtce_api_secret)" -D "$TMP/mcp-headers" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    ${sess[@]+"${sess[@]}"} -X POST "$(out rtce_mcp_url)" -d "$1"
  sid=$(tr -d '\r' <"$TMP/mcp-headers" 2>/dev/null | sed -n 's/^[Mm]cp-[Ss]ession-[Ii]d: *//p' | tail -n 1)
  [[ -z $sid ]] || MCP_SESSION=$sid
  if [[ $BODY == *"data: "* ]]; then BODY=$(printf '%s\n' "$BODY" | sed -n 's/^data: //p' | tail -n 1); fi
}

# Sets TOOL_TEXT to the JSON text a tool call returned; FAILs and returns 1 on error.
TOOL_TEXT=""
mcp_tool_result() {
  local label=$1
  TOOL_TEXT=""
  if ! ok; then fail "RTCE $label: $(http_err)"; return 1; fi
  if jq -e '.error or .result.isError' <<<"$BODY" >/dev/null 2>&1; then
    fail "RTCE $label: $(jq -r '(.error.message // .result.content[0].text // "error") | .[0:180]' <<<"$BODY")"
    return 1
  fi
  TOOL_TEXT=$(jq -r '.result.content[0].text // empty' <<<"$BODY")
}

check_rtce() {
  section rtce "Real-Time Context Engine (the chat's MCP server)"
  if have_cloud_key; then
    basic "$CLOUD_KEY" "$CLOUD_SECRET" "$API/rtce/v1/rtce-topics?environment=$ENV_ID&spec.kafka_cluster=$LKC"
    if ok; then
      local active miss inactive
      active=$(jq -r '[.data[] | select(.status.phase == "ACTIVE") | .spec.topic_name] | join(" ")' <<<"$BODY")
      miss=$(missing_from "$EXPECTED_TOPICS" "$active")
      inactive=$(jq -r '[.data[] | select(.status.phase != "ACTIVE") | "\(.spec.topic_name)=\(.status.phase)"] | join(" ")' <<<"$BODY")
      if [[ -n $miss ]]; then
        fail "RTCE topics not ACTIVE: $miss${inactive:+ ($inactive)}"
      else
        pass "all $(words "$EXPECTED_TOPICS") RTCE topics ACTIVE"
      fi
    else
      fail "RTCE topic list: $(http_err)"
    fi
  else
    skip "RTCE topic status: no confluent_cloud_api_key in terraform.tfvars"
  fi

  if [[ -z $(out rtce_mcp_url) || -z $(out rtce_api_key) ]]; then
    fail "no rtce_mcp_url / rtce_api_key terraform outputs (terraform/rtce.tf)"
    return
  fi
  mcp '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"health-check","version":"1"}}}'
  if ! ok || jq -e '.error' <<<"$BODY" >/dev/null 2>&1; then
    fail "RTCE MCP initialize: $(http_err)$([[ $CODE == 401 ]] && echo ' - the key must be Global-scoped')"
    return
  fi
  local server
  server=$(jq -r '.result.serverInfo | "\(.name) \(.version)"' <<<"$BODY")
  mcp '{"jsonrpc":"2.0","method":"notifications/initialized"}'

  local text n offline
  mcp '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"listTopics","arguments":{}}}'
  if mcp_tool_result listTopics; then
    text=$TOOL_TEXT
    n=$(jq '.rows.topics // [] | length' <<<"$text")
    offline=$(jq -r '[.rows.topics // [] | .[] | select(.status != "online") | "\(.name)=\(.status)"] | join(" ")' <<<"$text")
    if [[ -n $offline ]]; then
      warn "RTCE listTopics: $n topics, not online: $offline"
    else
      pass "MCP ($server) with the Global key: listTopics returns $n topics, all online"
    fi
  fi

  local probe=$RTCE_PROBE_TOPIC rows q
  if [[ -z $probe ]]; then
    case " $EXPECTED_TOPICS " in *" vehicle.telemetry "*) probe=vehicle.telemetry ;; *) probe=${EXPECTED_TOPICS%% *} ;; esac
  fi
  q=$(jq -nc --arg t "$probe" '{jsonrpc: "2.0", id: 3, method: "tools/call", params: {name: "queryData",
    arguments: {topic_name: $t, query: "SELECT * FROM `\($t)` LIMIT 1", max_result_rows: 1}}}')
  mcp "$q"
  if mcp_tool_result "queryData $probe"; then
    text=$TOOL_TEXT
    rows=$(jq '.rows.data // [] | length' <<<"$text")
    if [[ $rows -gt 0 ]]; then pass "queryData on $probe returns rows"; else warn "queryData on $probe returned no rows (no data yet?)"; fi
  fi
}

# --- tableflow -------------------------------------------------------------------

check_tableflow() {
  section tableflow "Tableflow topics and catalog integrations"
  if [[ -z $TABLEFLOW_TOPICS ]]; then skip "no tableflow_table_paths output - Tableflow not deployed"; return; fi
  local tk ts
  tk=$(out tableflow_api_key) ts=$(out tableflow_api_secret)

  basic "$tk" "$ts" "$API/tableflow/v1/tableflow-topics?environment=$ENV_ID&spec.kafka_cluster=$LKC"
  if ok; then
    local t st formats phase err sync good=0
    for t in $TABLEFLOW_TOPICS; do
      st=$(jq -c --arg t "$t" '[.data[] | select(.spec.display_name == $t)] | first // empty' <<<"$BODY")
      if [[ -z $st ]]; then fail "$t: not a Tableflow topic"; continue; fi
      formats=$(jq -r '.spec.table_formats | join(",")' <<<"$st")
      phase=$(jq -r '.status.phase' <<<"$st")
      err=$(jq -r '.status.error_message // "" | select(. != "none") | .[0:160]' <<<"$st")
      sync=$(jq -r '[.status.catalog_sync_statuses[]? | select(.sync_status != "SYNCED") | "\(.catalog_integration_id)=\(.sync_status)"] | join(" ")' <<<"$st")
      if [[ $phase != RUNNING ]]; then
        fail "$t: Tableflow $phase${err:+ - $err}"
      elif [[ -n $err || -n $sync ]]; then
        warn "$t: RUNNING but ${err:+error: $err }${sync:+catalog sync: $sync}"
      else
        good=$((good + 1))
      fi
      echo "$t: $phase, $formats, sync: $(jq -r '[.status.catalog_sync_statuses[]? | "\(.catalog_integration_id)=\(.sync_status)"] | join(" ")' <<<"$st")" | detail
    done
    [[ $good -eq 0 ]] || pass "$good/$(words "$TABLEFLOW_TOPICS") Tableflow topics RUNNING with catalogs synced"
  else
    fail "Tableflow topic list: $(http_err)"
  fi

  basic "$tk" "$ts" "$API/tableflow/v1/catalog-integrations?environment=$ENV_ID&spec.kafka_cluster=$LKC"
  if ok; then
    local line name kind phase last err age
    [[ $(jq '.data | length' <<<"$BODY") -gt 0 ]] || { skip "no catalog integrations"; return; }
    while IFS=$'\t' read -r name kind phase last err; do
      [[ $err != - ]] || err=""
      age=$(age_min "$last")
      if [[ $phase != CONNECTED ]]; then
        fail "catalog integration $name ($kind): $phase${err:+ - $err}"
      elif [[ -n $age && $age -gt $STALE_MINUTES ]]; then
        warn "catalog integration $name ($kind): CONNECTED but last sync $age min ago"
      else
        pass "catalog integration $name ($kind): CONNECTED${age:+, last sync $age min ago}"
      fi
    done < <(jq -r '.data[] | [.spec.display_name, .spec.config.kind, .status.phase, (.status.last_sync_at // "-"),
      (.status.error_message // "-" | if . == "none" or . == "" then "-" else .[0:160] end)] | @tsv' <<<"$BODY")
  else
    fail "catalog integration list: $(http_err)"
  fi
}

# --- aws -------------------------------------------------------------------------

# Runs the aws CLI with the given access key pair only (no profile or session token).
aws_as() {
  local k=$1 s=$2
  shift 2
  env -u AWS_PROFILE -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN \
    AWS_ACCESS_KEY_ID="$k" AWS_SECRET_ACCESS_KEY="$s" aws "$@"
}

check_aws() {
  section aws "S3 bucket and Glue (the Athena side)"
  if [[ -z $TABLEFLOW_TOPICS ]]; then skip "no tableflow_table_paths output - Tableflow not deployed"; return; fi
  command -v aws >/dev/null 2>&1 || { skip "aws CLI not installed"; return; }
  local bucket region rk rs
  bucket=$(out tableflow_s3_bucket) region=$(out tableflow_s3_region)
  rk=$(out fabric_s3_access_key_id) rs=$(out fabric_s3_secret_access_key)

  # S3: the read-only Fabric reader key when it exists (terraform/fabric_s3.tf), else your AWS credentials.
  local who="the fabric-reader key"
  s3_newest() {
    local args=(s3api list-objects-v2 --region "$region" --bucket "$bucket" --prefix "$1"
      --query 'max_by(Contents || `[]`, &LastModified).LastModified' --output text)
    if [[ -n $rk ]]; then aws_as "$rk" "$rs" "${args[@]}"; else aws "${args[@]}"; fi
  }
  [[ -n $rk ]] || who="your AWS credentials"

  local t path newest age fresh=0 worst=0 s3_ok=true
  for t in $TABLEFLOW_TOPICS; do
    path=$(jq -r --arg t "$t" '.tableflow_table_paths.value[$t]' <<<"$TF_OUT")
    if ! newest=$(s3_newest "${path#s3://"$bucket"/}/metadata/" 2>"$TMP/aws.err"); then
      skip "S3 listing with $who failed: $(head -n 1 "$TMP/aws.err" | cut -c1-160)"
      s3_ok=false
      break
    fi
    age=$(age_min "$newest")
    if [[ -z $age ]]; then
      warn "$t: no Iceberg metadata under its table path yet"
    elif [[ $age -gt $STALE_MINUTES ]]; then
      warn "$t: newest Iceberg metadata is $age min old (> $STALE_MINUTES) - is the topic getting data?"
    else
      fresh=$((fresh + 1))
      [[ $age -le $worst ]] || worst=$age
    fi
    echo "$t: newest metadata ${age:-none} min ago" | detail
  done
  if $s3_ok && [[ $fresh -gt 0 ]]; then
    pass "s3://$bucket: Iceberg metadata fresh for $fresh/$(words "$TABLEFLOW_TOPICS") tables (oldest newest-commit $worst min, via $who)"
  fi

  # Glue needs your own credentials (the reader key has S3 access only).
  local account glue_db tables want="" miss
  if ! account=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
    skip "Glue tables: no AWS credentials (set AWS_PROFILE or pass --aws-profile)"
    return
  fi
  glue_db=$(out tableflow_glue_database)
  [[ -n $glue_db ]] || { skip "Glue tables: no tableflow_glue_database output"; return; }
  if ! tables=$(aws glue get-tables --region "$region" --database-name "$glue_db" \
      --query 'TableList[].Name' --output text 2>"$TMP/aws.err"); then
    fail "Glue get-tables on $glue_db (account $account): $(head -n 1 "$TMP/aws.err" | cut -c1-160)"
    return
  fi
  # Compare with dots/dashes folded to underscores, whichever way Glue named them.
  tables=$(printf '%s\n' $tables | tr 'A-Z.-' 'a-z__' | tr '\n' ' ')
  for t in $TABLEFLOW_TOPICS; do want="$want $(printf '%s' "$t" | tr 'A-Z.-' 'a-z__')"; done
  miss=$(missing_from "$want" "$tables")
  if [[ -n $miss ]]; then fail "Glue database $glue_db lacks tables: $miss"; else pass "Glue database $glue_db has all $(words "$want") tables (Athena can query them)"; fi
}

# --- bedrock ---------------------------------------------------------------------

check_bedrock() {
  section bedrock "Amazon Bedrock model access"
  command -v aws >/dev/null 2>&1 || { skip "aws CLI not installed"; return; }
  local k s resp
  k=$(tfvar bedrock_aws_access_key_id) s=$(tfvar bedrock_aws_secret_access_key)
  if [[ -z $k || -z $s ]]; then skip "no bedrock_aws_access_key_id / _secret_access_key in terraform.tfvars"; return; fi
  if ! resp=$(aws_as "$k" "$s" bedrock-runtime converse --region "$BEDROCK_REGION" --model-id "$BEDROCK_MODEL" \
      --messages '[{"role":"user","content":[{"text":"Reply with the single word OK."}]}]' \
      --inference-config '{"maxTokens":5}' --output json 2>"$TMP/aws.err"); then
    fail "Converse with $BEDROCK_MODEL in $BEDROCK_REGION: $(grep -m 1 -v '^$' "$TMP/aws.err" | cut -c1-200)"
    return
  fi
  pass "Converse with $BEDROCK_MODEL in $BEDROCK_REGION: $(jq -r '"\(.stopReason), \(.metrics.latencyMs) ms"' <<<"$resp")"
}

# --- fabric ----------------------------------------------------------------------

check_fabric() {
  section fabric "Microsoft Fabric lakehouse (S3 shortcuts)"
  local fout
  if [[ ! -f $FABRIC_TF_DIR/terraform.tfstate ]] || ! fout=$(terraform -chdir="$FABRIC_TF_DIR" output -json 2>/dev/null) \
      || [[ $(jq 'length' <<<"$fout") -eq 0 ]]; then
    skip "terraform-fabric/ not applied ($FABRIC_TF_DIR)"
    return
  fi
  command -v az >/dev/null 2>&1 || { skip "az CLI not installed"; return; }
  local ws lh token
  ws=$(jq -r '.workspace_id.value' <<<"$fout") lh=$(jq -r '.lakehouse_id.value' <<<"$fout")
  if ! token=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv 2>"$TMP/az.err"); then
    warn "no Fabric token from az: $(grep -m 1 -oE 'AADSTS[0-9]+: [^.]*' "$TMP/az.err" || head -n 1 "$TMP/az.err" | cut -c1-120) - run az login (terraform-fabric/README.md)"
    return
  fi

  local want have miss
  want=$(jq -r '.tables.value | [.[]] | join(" ")' <<<"$fout")
  bearer "$token" "$FABRIC_API/workspaces/$ws/items/$lh/shortcuts"
  if ok; then
    have=$(jq -r '[.value[].name] | join(" ")' <<<"$BODY")
    miss=$(missing_from "$want" "$have")
    if [[ -n $miss ]]; then fail "lakehouse shortcuts missing: $miss"; else pass "all $(words "$want") table shortcuts exist in the lakehouse"; fi
  else
    fail "Fabric shortcut list: $(http_err)"
  fi

  # Fabric converts each Iceberg table's metadata to Delta; its log says whether that worked.
  local stoken t status good=0
  if stoken=$(az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv 2>/dev/null); then
    for t in $want; do
      bearer "$stoken" -H 'x-ms-version: 2023-11-03' "$ONELAKE_DFS/$ws/$lh/Tables/$t/_delta_log/latest_conversion_log.txt"
      if [[ $CODE == 404 ]]; then warn "$t: no Delta conversion log yet"; continue; fi
      ok || { fail "$t conversion log: $(http_err)"; continue; }
      status=$(sed -n 's/.*Status: *\([A-Za-z]*\).*/\1/p' <<<"$BODY" | head -n 1)
      if [[ $status == Succeeded ]]; then
        good=$((good + 1))
      else
        fail "$t: Delta conversion ${status:-unknown}: $(grep -m 1 -i 'error' <<<"$BODY" | tr -s ' \t' ' ' | cut -c1-160)"
      fi
    done
    [[ $good -eq 0 ]] || pass "$good/$(words "$want") tables converted to Delta (latest_conversion_log: Succeeded)"
  else
    warn "no OneLake storage token from az - conversion logs not checked"
  fi

  $WITH_SQL || return 0
  command -v sqlcmd >/dev/null 2>&1 || { skip "--with-sql: sqlcmd not installed (brew install sqlcmd)"; return; }
  bearer "$token" "$FABRIC_API/workspaces/$ws/lakehouses/$lh"
  ok || { fail "lakehouse lookup: $(http_err)"; return; }
  local host db
  host=$(jq -r '.properties.sqlEndpointProperties.connectionString // empty' <<<"$BODY")
  db=$(jq -r '.displayName' <<<"$BODY")
  [[ -n $host ]] || { warn "lakehouse has no SQL endpoint yet"; return; }
  if sqlcmd -S "$host" -d "$db" --authentication-method ActiveDirectoryDefault -W -s '|' -b \
      -i "$FABRIC_TF_DIR/queries/validate.sql" >"$TMP/sql.out" 2>&1; then
    pass "validate.sql on the SQL endpoint:"
    grep -v '^$' "$TMP/sql.out" | sed 's/^/       /'
  else
    fail "validate.sql: $(grep -m 1 -E 'Msg|rror' "$TMP/sql.out" | cut -c1-200)"
  fi
}

# --- engine ----------------------------------------------------------------------

check_engine() {
  section engine "fleet-context-engine console at $ENGINE_URL"
  local CURL_TIMEOUT=5
  anon "$ENGINE_URL/api/health"
  if [[ $CODE == 000 ]]; then
    warn "not reachable at $ENGINE_URL - start it: cd fleet-context-engine && npm start"
    return
  fi
  ok || { fail "/api/health: $(http_err)"; return; }
  local mode
  mode=$(jq -r '.mode' <<<"$BODY")
  if [[ $mode == live ]]; then pass "running in LIVE mode (consuming from Confluent Cloud)"; else warn "running in $(printf '%s' "$mode" | tr a-z A-Z) mode - the .env Kafka variables aren't set"; fi

  anon "$ENGINE_URL/api/fleet-summary"
  if ok; then
    local caught
    caught=$(jq -r '.consumerCaughtUp' <<<"$BODY")
    if [[ $caught == true ]]; then
      pass "consumer caught up: $(jq -r '"\(.eventsPerSecond) events/s, \(.openRecommendations) open AI recommendations (\(.recommendationCounts | to_entries | map("\(.key) \(.value)") | join(", ")))"' <<<"$BODY")"
    else
      warn "consumer still catching up (consumerCaughtUp=$caught)"
    fi
  else
    fail "/api/fleet-summary: $(http_err)"
  fi

  # The console serves an allowlist from the repo root; none of these may come back.
  local p served=""
  for p in /terraform/terraform.tfvars /terraform/terraform.tfstate /fleet-context-engine/.env /.env \
      /terraform-fabric/terraform.tfvars /terraform-fabric/terraform.tfstate /fonts/../terraform/terraform.tfvars; do
    anon -o /dev/null --path-as-is "$ENGINE_URL$p"
    [[ $CODE != 200 ]] || served="$served $p"
  done
  if [[ -n $served ]]; then fail "the console SERVES secret files:$served"; else pass "doesn't serve tfvars, tfstate or .env"; fi

  $WITH_ASK || return 0
  local CURL_TIMEOUT=120 body
  body=$(jq -nc --arg q "$ASK_QUESTION" '{conversation: [{role: "user", content: $q}]}')
  anon -H 'Content-Type: application/json' -X POST "$ENGINE_URL/api/ask" -d "$body"
  if ok && [[ -n $(jq -r '.answer // empty' <<<"$BODY") ]]; then
    pass "chat answered \"$ASK_QUESTION\" (tools: $(jq -r '[.trace[]? | .tool // empty] | join(", ") | if . == "" then "none" else . end' <<<"$BODY"))"
    jq -r '.answer' <<<"$BODY" | fold -s -w 100 | sed 's/^/       /'
  else
    fail "/api/ask: $(http_err)"
  fi
}

# --- drift -----------------------------------------------------------------------

check_plan() {
  section drift "terraform plan in ${TF_DIR#"$ROOT"/}"
  local rc
  terraform -chdir="$TF_DIR" plan -detailed-exitcode -lock=false -input=false -no-color >"$TMP/plan.out" 2>&1
  rc=$?
  case $rc in
    0) pass "no changes: the infrastructure matches the configuration" ;;
    2) warn "drift: $(grep -m 1 '^Plan:' "$TMP/plan.out")"
       grep -E '^  # .* (will|must) be' "$TMP/plan.out" | sed 's/^  # //' | detail ;;
    *) local err
       err=$(grep -m 1 -A 1 '^Error:' "$TMP/plan.out" | sed 's/^Error: //' | tr -s '\n ' ' ' | cut -c1-200)
       case $err in *[Cc]redential*) err="$err(set AWS_PROFILE as for terraform apply)" ;; esac
       fail "plan failed: $err" ;;
  esac
}

# --- run -------------------------------------------------------------------------

enabled local && check_local
enabled confluent && check_confluent
enabled flink && check_flink
enabled dataflow && check_dataflow
enabled rtce && check_rtce
enabled tableflow && check_tableflow
enabled aws && check_aws
enabled bedrock && check_bedrock
enabled fabric && check_fabric
enabled engine && check_engine
$WITH_PLAN && check_plan

printf '\n%sSummary%s  %s%d passed%s, %s%d warning(s)%s, %s%d failed%s, %d skipped\n' "$C_BOLD" "$C_OFF" \
  "$C_PASS" "$PASSES" "$C_OFF" "$C_WARN" "$WARNS" "$C_OFF" "$C_FAIL" "$FAILS" "$C_OFF" "$SKIPS"
[[ -z $PROBLEMS ]] || printf '%s' "$PROBLEMS"

if [[ $FAILS -gt 0 ]] || { $STRICT && [[ $WARNS -gt 0 ]]; }; then exit 1; fi
exit 0
