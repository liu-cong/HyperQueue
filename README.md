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

Console messages
==
To help the user understand the procedure, console messages will be displayed to show the current status of producer, consumer and broker.
Consumer:
If the consumer connects to the broker for the first time, it gets
a new session id. Sample consle message: 

[Consumer status] new session ID is obtained. ID=1.

If the consumer fails to get a new session ID, console message will be:

[Consumer status] Failed to get session ID.

If the consumer successfully received a new message from the server:

[Consumer status] Topic=exampleTopic ID=1 Message=newMessage

If the consumer failed to receive a message because of either invalid request or network problem:

[Consumer status] Failed to get a message from the server.


Producer:
If the producer posted a new message on the server:

[Producer status] A new message is posted. topic=exampletopic Message=newMessage.

If the post failed:

[Producer status] Failed to post the message to the server.

Consumer:
When the broker sends a new sesssion ID to a consumer:

[Consumer Request] New session ID is assigned. ID=1.

When the broker sends a new message to a cosumer:

[Consumer Request] A new message is consumed. SessionID=1 Offset=0 Topic=exampleTopic Message=newMessage

If the consumer sends in an invalid request (e.g. wrong topic or empty message queue):

[Consumer Request] Invalid request, no message is consumed.

When a producer succesfully adds a new message on the broker:

[Producer Request] New message added. Topic=exampletopic Message=newMessage

If the broker receives a null topic or message from the producer:

[Producer Request] Null topic or message received by broker.

