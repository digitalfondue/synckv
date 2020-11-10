SyncKV
======


[![Maven Central](https://img.shields.io/maven-central/v/ch.digitalfondue.synckv/synckv.svg)](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22synckv%22)
[![Build Status](https://img.shields.io/github/workflow/status/digitalfondue/synckv/Java%20CI%20with%20Maven)](https://github.com/digitalfondue/synckv/actions?query=workflow%3A%22Java+CI+with+Maven%22)


SyncKV is a key,value store based on h2-mvstore and jgroups.
His main characteristic is, as his name implies, to be able to 
synchronize/replicate his whole content transparently between multiple 
instances thanks to jgroups.

It has the following limitations:

 - only put (insert/update)
 - no delete operation
 - key are string, value are byte array (a key: string, value: string api is exposed too)
 - it's for relatively small database
 - untested :D
 
 License
 -------
 
 SyncKV is licensed under the Apache License Version 2.0.
 
 Download
 --------
 
 maven:
 
 ```xml
<dependency>
    <groupId>ch.digitalfondue.synckv</groupId>
    <artifactId>synckv</artifactId>
    <version>0.6.0</version>
</dependency>
 ```

gradle:

```
compile 'ch.digitalfondue.synckv:synckv:0.6.0'
```
