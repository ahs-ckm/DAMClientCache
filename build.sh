#!/usr/bin/bash
go build -ldflags "-X main.gBuild=`date -u +.%Y%m%d.%H%M%S`" ./DAMClientCache.go 
scp ./DAMClientCache coni@beeby.ca:~/
cp ./DAMClientCache /mnt/nas/Dump/Install/DAMClientCache/
./DAMClientCache -v
