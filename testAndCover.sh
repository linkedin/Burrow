#!/bin/bash

# This script tests multiple packages and creates a consolidated cover profile
# See https://gist.github.com/hailiang/0f22736320abe6be71ce for inspiration.

function die() {
  echo $*
  exit 1
}

# Initialize profile.cov
echo "mode: count" > profile.cov

# Initialize error tracking
ERROR=""

# Get package list
PACKAGES=$(find core -type d -not -path '*/\.*')

# Test each package and append coverage profile info to profile.cov
# Note this is just for coverage. We run the race detector separately because it won't work with count
for pkg in $PACKAGES
do
    go test --timeout 5s -covermode=count -coverprofile=profile_tmp.cov github.com/linkedin/Burrow/$pkg || ERROR="Error testing $pkg"
    if [ -f profile_tmp.cov ]
    then
        tail -n +2 profile_tmp.cov >> profile.cov || die "Unable to append coverage for $pkg"
        rm profile_tmp.cov
    fi
done

if [ ! -z "$ERROR" ]
then
    die "Encountered error, last error was: $ERROR"
fi
