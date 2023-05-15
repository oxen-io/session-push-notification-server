#!/bin/bash

if [ "$1" == "--check" ]; then
    cf_args=(--dry-run -Werror)
    black_args=(--check)
elif [ "$#" -eq 0 ]; then
    cf_args=(-i)
    black_args=()
else
    echo "Usage: $0 [--check]" >&2
    exit 1
fi

CLANG_FORMAT_DESIRED_VERSION=14

CLANG_FORMAT=$(command -v clang-format-$CLANG_FORMAT_DESIRED_VERSION 2>/dev/null)
if [ $? -ne 0 ]; then
    CLANG_FORMAT=$(command -v clang-format-mp-$CLANG_FORMAT_DESIRED_VERSION 2>/dev/null)
fi
if [ $? -ne 0 ]; then
    CLANG_FORMAT=$(command -v clang-format 2>/dev/null)
    if [ $? -ne 0 ]; then
        echo "Please install clang-format version $CLANG_FORMAT_DESIRED_VERSION and re-run this script." >&2
        exit 1
    fi
    version=$(clang-format --version)
    if [[ ! $version == *"clang-format version $CLANG_FORMAT_DESIRED_VERSION"* ]]; then
        echo "Please install clang-format version $CLANG_FORMAT_DESIRED_VERSION and re-run this script." >&2
        exit 1
    fi
fi

BLACK=$(command -v black 2>/dev/null)
if [ $? -ne 0 ]; then
    echo "Please install the 'black' python3 package and make sure it is available in your path"
fi

shopt -s globstar
bad=0

$CLANG_FORMAT "${cf_args[@]}" spns/**/*.[ch]pp
if [ $? -ne 0 ]; then
    bad=1
fi

black "${black_args[@]}" spns/**/*.py
if [ $? -ne 0 ]; then
    bad=1
fi

exit $bad
