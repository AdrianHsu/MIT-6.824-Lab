#!/bin/sh

rm out.txt

for i in {1..10}; 
do go test | tee -a out.txt; done

sed -i '' '/ForwardTest: NOT CURRENT PRIMARY/d' ./out.txt
sed -i '' '/PutAppend: NOT THE PRIMARY YET/d' ./out.txt

sed -i '' '/unexpected EOF/d' ./out.txt
sed -i '' '/write unix/d' ./out.txt
sed -i '' '/connection is shut down/d' ./out.txt
sed -i '' '/rpc.Register/d' ./out.txt
sed -i '' '/connection reset by peer/d' ./out.txt
