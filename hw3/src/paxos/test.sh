#!/bin/sh

rm out.txt

for i in {1..10}; 
do go test | tee -a out.txt; done

sed -i '' '/unexpected EOF/d' ./out.txt
sed -i '' '/write unix ->/d' ./out.txt
sed -i '' '/read unix ->/d' ./out.txt
sed -i '' '/connection is/d' ./out.txt
sed -i '' '/rpc.Register/d' ./out.txt
sed -i '' '/paxos Dial() failed/d' ./out.txt

cat out.txt
