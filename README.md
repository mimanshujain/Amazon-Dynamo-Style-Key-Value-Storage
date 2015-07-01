## Amazon-Dynamo-Style-Key-Value-Storage

A simplified version of Dynamo. 

There are three main pieces: 

1) Partitioning

2) Replication

3) Failure handling

The main goal is to provide both availability and Linearizability at the same time. 
It should always perform read and write operations successfully even under failures. 
At the same time, a read operation should always return the most recent value. SHA-1 hash function was used to generate keys.
