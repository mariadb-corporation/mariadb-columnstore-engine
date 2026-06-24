#!/usr/bin/env sh

DIR_NAME=$1

# Cores listed here belong to a test the Drone config marked tolerated (run_regression.sh --ignore-cores,
# e.g. test400). They are still gdb-formatted and published as artifacts; we just don't fail the stage
# on them. Any OTHER core (a real crash in another test) still fails the stage, as do sanitizer reports.
#
# .expected_cores stores the raw core path as it existed when the test ran. By the time this gate runs,
# core_dump_format.sh has already gzipped the core (adding .gz) and written .html/.dump siblings, so
# normalise before matching: skip the .html/.dump artifacts, and strip a trailing .gz so the gzipped
# core still matches the raw name recorded in .expected_cores.
EXPECTED="/$DIR_NAME/.expected_cores"

for f in /"$DIR_NAME"/*; do
        case "$f" in
                *.html|*.dump)
                        continue
                        ;;
                *_core_dump.*)
                        core="${f%.gz}"
                        if [ -f "$EXPECTED" ] && grep -qxF "$core" "$EXPECTED"; then
                                echo "$core: expected core from a tolerated test (kept & published), not failing the stage"
                                continue
                        fi
                        echo "$(basename "${core%%_*}")" "aborted (core dumped)"
                        exit 1
                        ;;
        esac
done

exit 0
