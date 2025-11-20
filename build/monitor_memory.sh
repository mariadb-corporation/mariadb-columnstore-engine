#!/bin/bash
# Monitor memory usage for regression test
# Usage: monitor_memory.sh <test_name>

TEST_NAME="${1:?Test name required}"
LOG_FILE="/regression-results/memory-monitor-${TEST_NAME}"

INTERVAL=5
PREV_MEM_FILE="/tmp/mem_snapshot_${TEST_NAME}.txt"
GROWTH_THRESHOLD_MB=200

# Get process list sorted by memory
ps_by_memory() {
  ps aux --sort=-rss 2>/dev/null || ps aux | sort -k4 -rn
}

printf "\n=== Memory Monitor Started for test: %s at %s ===\n\n" "${TEST_NAME}" "$(date)" >> "$LOG_FILE"

while true; do
    
    {
        echo "========================================"
        echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "========================================"
        printf "\n--- System Memory ---\n"
        free -h
        
        printf "\n--- Top 10 Processes by memory ---\n"
        PS_OUTPUT=$(ps_by_memory)
        CURRENT_SNAPSHOT=$(echo "$PS_OUTPUT" | awk 'NR>1 {printf "%s %s %d\n", $2, $11, $6}' | head -n 20)
        echo "$PS_OUTPUT" | head -n 11
        
        if [[ -f "$PREV_MEM_FILE" ]]; then
            printf "\n--- Memory Growth (since last check) ---\n"
            
            # Compare snapshots and show processes with significant growth
            while read -r PID CMD CURR_RSS; do
                
                # Find previous RSS for this PID
                PREV_RSS=$(grep "^$PID " "$PREV_MEM_FILE" 2>/dev/null | awk '{print $3}')
                
                if [[ -n "$PREV_RSS" ]]; then
                    DELTA_MB=$(((CURR_RSS - PREV_RSS) / 1024))
                    ABS_DELTA=$((DELTA_MB < 0 ? -DELTA_MB : DELTA_MB))
                    
                    if [[ $ABS_DELTA -gt 10 ]]; then
                        printf "  PID %-7s %-20s %+6d MB" "$PID" "$CMD" "$DELTA_MB"
                        [[ $DELTA_MB -gt $GROWTH_THRESHOLD_MB ]] && printf " [!] ALERT\n" || printf "\n"
                    fi
                fi
            done <<< "$CURRENT_SNAPSHOT"
        fi
        
        # Save current snapshot for next iteration
        echo "$CURRENT_SNAPSHOT" > "$PREV_MEM_FILE"
        echo
    } >> "$LOG_FILE"
    
    sleep "$INTERVAL"
done
