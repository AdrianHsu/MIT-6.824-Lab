#!/bin/bash
go run wc.go master kjv12.txt sequential
sort -n -k2 mrtmp.kjv12.txt | tail -10 | diff - mr-testout.txt > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):"
  cat diff.out
else
  echo "Passed test"
fi

