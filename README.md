SyncKV
======

SyncKV is a key,value store based on h2-mvstore and jgroups.
His main characteristic is, as his name implies, to be able to 
synchronize/replicate his whole content transparently between multiple 
instances thanks to jgroups.

It has the following limitations:

 - only put (insert/update)
 - no delete operation
 - key are string, value are byte array
 - it's for relatively small database
 - untested :D
 
 
 
 Synchronization strategy
 ------------------------
 
 Distributed put: when a put is done in a node, a `putRequest` is sent to everybody. So generally, after the first
 synchronization the data should be aligned.
 
 
 Full/partial synchronization: In a jgroup channel, there is a leader (the first element of the channel) 
 and the followers.
 
 1. The leader, each 10 seconds, make a `requestForSyncPayload` call
 2. The followers, will receive a call to `handleRequestForSyncPayload` call back the leader 
    with `syncPayloadForLeader` 
 3. The leader, receive the various calls through `handleSyncPayloadForLeader` save in a map Address, List of TableMetadata
    the message
 4. In the same job as point 1, each 10 seconds it will handle the map of `List of TableMetadata`:
    for the tables that are missing, it will send a "full sync" message, for the ones that have
    a different bloom filter a "partial sync" message. 
    The message class is `SyncPayloadFrom`: it contain from where the receiver will need to load the data.
 5. When receiving a `SyncPayloadFrom` message, it will then send requests for synchronization using the `SyncPayload`
    message.
 6. The node receiving a `SyncPayload` will finally send to the requester the missing data 
    (chunked) using the `DataToSync` message.
 
 Notes: as we are using bloom filters, it may be possible that some data will not be synchronized. A new job that
 check more in depth will be needed.