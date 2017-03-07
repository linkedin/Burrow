#!/bin/bash

test_dirs=$(find . -name '*_test.go' | xargs -n1 dirname |uniq)
for test_dir in ${test_dirs}; do
  echo "Running tests in ${test_dir}"
  go test -v ${test_dir}
  echo
done
