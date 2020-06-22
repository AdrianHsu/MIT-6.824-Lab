#!/bin/sh

rm out.txt

for i in {1..3}

do 
  echo "round: $i" | tee -a ./out.txt
  go test | tee -a out.txt
  sed -i '' '/unexpected EOF/d' ./out.txt
  sed -i '' '/write unix ->/d' ./out.txt
  sed -i '' '/read unix ->/d' ./out.txt
  sed -i '' '/connection is/d' ./out.txt
  sed -i '' '/rpc.Register/d' ./out.txt
  sed -i '' '/paxos Dial() failed/d' ./out.txt
done
cat out.txt
