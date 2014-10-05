HyperQueue
==========
Introduction
==
This project implements the consumer-producer pattern with broker implemented as a server on top of Tomcat. The broker,
producer and consumer are all included in this project. The broker is placed in package com.hqdemo.server and both the
producer and consumer are placed in package com.hqdemo.local.

Test cases are included in com.hqdemo.local/Main.java, which creates a thread pool and executes both producer and consumer.
I have run the server on my PC, which can be accesed by http://hqdemo.nat123.net/HyperQueue/Broker. 

To demo this project, you can simply run or edit Main.java to see the result. In addition, you can configure and run the 
broker on your own server.

Server
==
The broker(server) is run on top of Tomcat 7.0. I mapped my IP　address to  http://hqdemo.nat123.net/HyperQueue/Broker using 
nat123 (www.nat123.com) for easier demo. If you cannot access the server, please either contact me or configure the Broker.java
to run it on your own server.
