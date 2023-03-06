#!/bin/sh

DIR=$1
echo "Using directory $DIR"
CMD=$2
QU=../qu/target/release/qu

set -e

mkdir -p $DIR

restore() {
    FILE="$1"
    echo "Restoring $FILE..."
    $QU --pem-file ~/.config/dfx/identity/dev/identity.pem \
        raw $(cat .dfx/local/canister_ids.json | jq -r ".taggr.local") "stable_mem_write" --args-file "$FILE" | $QU send --yes --raw - > /dev/null
}

if [ "$CMD" == "restore" ]; then
    export IC_URL=http://localhost:8080
    PAGE=0
    while true; do
        for _ in {1..10}; do
            FILE="$DIR/page$PAGE.bin"
            restore $FILE &
            PAGE=$((PAGE + 1))
        done
        wait
        if [ "$(stat -f%z $FILE)" == "18" ]; then break; fi
    done
    dfx canister call taggr stable_to_heap
    exit 0
fi

fetch() {
    FILE="$1"
    $QU --pem-file ~/.config/dfx/identity/prod/identity.pem \
        raw "6qfxa-ryaaa-aaaai-qbhsq-cai" "stable_mem_read" --args "($PAGE:nat64)" --query |\
        $QU send --yes --raw - > $FILE
}

git rev-parse HEAD > $DIR/commit.txt

PAGE=0
while true; do
    for _ in {1..10}; do
        FILE="$DIR/page$PAGE.bin"
        echo "Fetching page $PAGE..."
        fetch $FILE &
        PAGE=$((PAGE + 1))
    done
    wait
    if [ "$(stat -f%z $FILE)" == "18" ]; then break; fi
done

