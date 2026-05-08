# LOG COLLECTOR SYSTEM

> [!NOTE]  
> To do this exercise, candidate must have docker/podman installed in the system and have `go` installed in the system.


## OBJECTIVE
Objective of this challenge is to try to build a scalable and a reliable system. The challenge includes an agent which collect logs from a log file and sends the logs data over the wire to a aggregator service which then writes to a DB.
We want to collect binary log data which are continuously getting dumped to multiple files from a distributed service deployed over multiple nodes. The collected log data should then be served over the wire and to a collector which processes it and stores it in a DB.

## LOG FILE FORMAT
Each node contains 10 files which are rotated over based on number of log lines each file contains. For example, If the Distributed service was configured to store 10000 log lines then each node will contain 10000 lines over 10 files. When all the file are filled with 1000 lines then the next log line will be written to a new file and the oldest file will be removed. Each file name is in this format - ts_log_<timestamp>
Each log line can be of 100MB of zlib compressed binary data. The format of each line of the data is 
```
<TIMESTAMP> <UUID> <BINARY_DATA>
```
There is an example input file in the repo present for you to test it out. 

## GENERATE INPUT 
To generate the test input, run 
```bash
make generate
```
This will generate 10 log files in the input dir.


> Assumption : The server takes around 30secs to process each log line and store it in the DB. There can be 1000+ services sending data to collector service 


## CONSTRAINTS
There are some restrictions impose on the systemd service. You have to add these lines to the systemd service unit file.
```
CPUWeight=100
MemoryMax=1G
```
This makes the service run in 1Core and 1GB Memory only. Beyond that the process will be killed with OOM.


## SOLUTION
You should write a go program that reads from a directory which holds the 10 log files and captures the lines and format each part of the log line to a JSON field such as 
```json
[
{
        "id": "<UUID>",
        "time": "<TIMESTAMP>",
        "data": "<BINARY_DATA>"
}
]
```
You have to make sure your service is running continuously and monitoring the file changes as well. You also have to take the Constraint into consideration. Keep in mind that there shouldn't be any data loss.
