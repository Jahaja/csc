# A small cache library on top of redigo utilizing Redis's server-assisted client side caching functionality

This library has been created with the main use-case being a web server, and thus http requests.
The library includes two approaches to accomplish a server-assisted local cache for this use-case:

1. Caching per client using Tracking mode. Get a client for each request from a client pool. 
   Each client handles its own cache storage. Meaning the storage has the same lifetime as the client and keys are not shared between the clients. 
   If a client loses either of its connections (data and/or invalidation) to Redis, it's marked as failed and never reused since it is assumed to be out-of-sync.
   
2. Global cache using Broadcasting mode. Create a single broadcasting client that invalidates a global cache. 
   Each request's cache client doesn't track keys but just gets from/sets to the local storage during Set/Get calls. If the broadcasting invalidation connection fails the global cache must be flushed.