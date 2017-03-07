#!/bin/bash

red="$(printf '\033[0;31m')"
green="$(printf '\033[0;32m')"
normal="$(printf '\033[0;39m')"

test_dirs=$(find . -name '*_test.go' | xargs -n1 dirname |uniq)
for test_dir in ${test_dirs}; do
  echo "Running tests in ${test_dir}"
  go test -v ${test_dir} | sed -e "s/PASS/${green}PASS${normal}/g" -e "s/FAIL/${red}FAIL${normal}/g"
  echo
done
