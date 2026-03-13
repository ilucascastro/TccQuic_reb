#!/usr/bin/env bash

set -euo pipefail

PROGRAM_NAME=$0
showUsage() {
    echo "Usage: $PROGRAM_NAME [--pilot|--reduced|--full] <IP>"
}

PROFILE="reduced"
IP=
while [[ "$#" -gt 0 ]]; do
    case "$1" in
    --pilot)   PROFILE="pilot" ; shift ;;
    --reduced) PROFILE="reduced" ; shift ;;
    --full)    PROFILE="full" ; shift ;;
    -*)        showUsage ; exit 1 ;;
    *)         IP="$1" ; shift ;;
    esac
done

if [[ -z "$IP" ]]; then
    showUsage
    exit 1
fi

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"

LOG_NUMBER=1
while true; do
    SUPER_LOG_DIR=$(printf "../../logs/%s-%03d/" \
        "$(basename "${PROGRAM_NAME%.*}")" "$LOG_NUMBER")
    if [[ ! -e "$SUPER_LOG_DIR" ]]; then
        break
    fi
    LOG_NUMBER=$((LOG_NUMBER+1))
done
mkdir -p "$SUPER_LOG_DIR"

BASE_LATENCIES=(500)
SCHEDULERS=(fifo sp wfq)
ABRS=(bola legacy)
SCENARIOS=(
    "1 10 24"
    "3 10 16"
    "6 30 10"
)

case "$PROFILE" in
pilot)
    BASE_LATENCIES=(10)
    SCHEDULERS=(wfq)
    ABRS=(bola legacy)
    SCENARIOS=("1 10 24")
    ;;
reduced)
    # The paper indicates initial playback delay can be fixed arbitrarily for long sessions.
    BASE_LATENCIES=(500)
    ;;
full)
    BASE_LATENCIES=(10 50 100 200 300 400 500 1000)
    ;;
*)
    echo "Unknown profile: $PROFILE"
    exit 1
    ;;
esac

launchScenario() {
    local scenario="$1"
    local load="$2"
    local delay="$3"
    local parallelism=120
    local loss=2
    local bw=100

    for base_latency in "${BASE_LATENCIES[@]}"; do
        for scheduler in "${SCHEDULERS[@]}"; do
            for abr in "${ABRS[@]}"; do
                LOG_DIR=$(printf "%s/scenario%s-baselatency%s/%s/%s/" \
                    "$SUPER_LOG_DIR" "$scenario" "$base_latency" "$scheduler" "$abr")
                mkdir -p "$LOG_DIR"
                cat > "$LOG_DIR/experiment.env" <<EOF
scenario=$scenario
delay_ms=$delay
background_load_pct=$load
server_mode=$scheduler
abr_mode=$abr
base_latency_ms=$base_latency
server_bw_mbps=$bw
client_bw_mbps=$bw
loss_pct=$loss
parallelism=$parallelism
EOF

                PARAMS=(-o "$LOG_DIR" "--$scheduler" --abr "$abr" --sbw "$bw" --cbw "$bw" \
                    --loss "$loss" -p "$parallelism" --delay "$delay" --load "$load" \
                    --baselatency "$base_latency")
                printf "%s\n" "${PARAMS[*]}" > "$LOG_DIR/parameters"
                echo "[INFO] scenario=$scenario scheduler=$scheduler abr=$abr base_latency=$base_latency"
                ./server_scheduler_test.sh "${PARAMS[@]}" "$IP"
            done
        done
    done
}

echo "[INFO] profile=$PROFILE scenarios=${#SCENARIOS[@]} schedulers=${#SCHEDULERS[@]} abrs=${#ABRS[@]} base_latencies=${#BASE_LATENCIES[@]}"
for scenario_def in "${SCENARIOS[@]}"; do
    # shellcheck disable=SC2086
    launchScenario $scenario_def
done

python3 resources/collect_abr_comparison_results.py \
    "$SUPER_LOG_DIR" "$SUPER_LOG_DIR/abr-comparison-summary.csv"
python3 resources/plot_abr_comparison_summary.py \
    "$SUPER_LOG_DIR/abr-comparison-summary.csv" \
    "$SUPER_LOG_DIR/abr-comparison-summary.png"

echo "[INFO] Logs: $(cd "$SUPER_LOG_DIR" && pwd)"
