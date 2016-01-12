#!/bin/bash
#
# Run this program in a window or the background, and it'll notify you if you 
# save something which is causing your tests to fail.  Expects to have nose
# and notify-send installed.
#
# Note this is only necessary if you want tests to auto-run when any source
# file is modified. NOT required to run general tests which can be done
# manually from the root of the tree simply by running "nosetests"
#
# Colourized output can be enabled by installing the "rednose" module with PIP
#

NOSETESTS=""

main () {
    NOSETESTS=$(find_nosetests)
    # determine the root of our git tree
    PRJROOT="$(git rev-parse --show-toplevel)"
    # set the python path
    export PYTHONPATH="$PRJROOT/lib:$PRJROOT/bin:$PYTHONPATH"
    do_test
    while true; do
        # Use inotify to run tests
        inotifywait "$PRJROOT" -r -e modify --exclude '(\.swp$|/\.git/|\.py[cod]$)' > /dev/null 2>&1
        do_test
        sleep 1
    done
}

do_test () {
    # Visually separate from previous invocation.
    for f in 1 1 1 1 1 1 1 1; do echo ''; done
    echo '################################# TEST END ################################'
    for f in 1 1 1 1 1 1 1 1; do echo ''; done
    python3 -m rednose 2> /dev/null && $NOSETESTS --rednose || $NOSETESTS
    if [ $? -eq 0 ]; then
        notify-send 'Tests passed' "Project: ${PWD##*/}" -i '/usr/share/icons/Adwaita/48x48/emotes/face-smile.png' > /dev/null 2>&1
    else
        notify-send 'Tests FAILED' "Project: ${PWD##*/}" -i '/usr/share/icons/Adwaita/48x48/status/dialog-warning.png' > /dev/null 2>&1
    fi
}

find_nosetests () {
    for n in nosetests-3.4 nosetests-3 nosetests3 nosetests; do
        which $n > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "$n"
            return 0
        fi
    done
    echo "Failed to find nosetests program - please ensure it is installed and in the PATH" 1>&2
    exit 2
}

main "$@"

