#since the file was too large i used this bash script to insert the insertion.sql file
#pwd:AVNS_4T0M1r4SdTxtoZAMUPL
mysql -h mysql-c314d3b-reveliolab-assignment1.e.aivencloud.com -P 24878 -u avnadmin -p --ssl-mode=REQUIRED INGESTION < ingestion.sql --force

