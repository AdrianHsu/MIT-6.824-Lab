#!/bin/sh

DATE=$(date)
#echo "$DATE"

git add .
git commit -m "[UPDATED] $DATE :bulb:" # :tada:
git push
