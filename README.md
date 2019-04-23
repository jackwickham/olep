# Event-Oriented Database
This is an implementation of the online event processing (OLEP) model proposed by Kleppmann, Beresford and Svingen [[1]](https://queue.acm.org/detail.cfm?id=3321612).

## Background
The OLEP model is based on [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html), where all changes to the database are made by adding _events_ to a persistent _event log_. An event represents a domain-specific change, rather than specifying the values that should be changed—"Alice transferred $20 to Bob" rather than "set Alice's balance to $35 and Bob's balance to $42". This makes it easier to see exactly what has happened to the data, and allows you to do things like looking at what the database was like at a particular point in time, or changing how an event is interpreted then resetting the database to use the old events with the new interpretation.

OLEP adds support for transaction consistency checks and returning data to the application by adding another layer of indirection.

## This Project
This project implements the [TPC-C benchmark](http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf) for an event-oriented database (implementing the OLEP model).

The event logs use [Apache Kafka](https://kafka.apache.org/).

The aim of this project is to demonstrate that event-oriented databases are viable, and create a working implementation.

## System Overview
<figure>
<a href="https://raw.githubusercontent.com/jackwickham/olep/master/images/system-diagram.png"><img src="https://raw.githubusercontent.com/jackwickham/olep/master/images/system-diagram.png" alt="System Diagram"></a>
<figcaption style="text-align: center; font-size: 0.7em;">Figure 1: System Diagram</figcaption>
</figure>

Applications write events to the _transaction requests_ topic.
 
The verifiers read events from the transaction results topic and ensure that the consistency properties of the transaction will not be violated. The only way a transaction can fail in TPC-C is if there is an invalid item in a New-Order transaction, which can be determined without maintaining any state about previous transactions, so the verifiers are stateless except for the items store. They are implemented as a Kafka Streams stream processor.

When a transaction is accepted or rejected by the verifier, it writes a message to the _transaction results_ topic, which is read by the application to find out what happened to the transaction. In addition, if the transaction is accepted, it is also written to the _accepted transactions_ topic.

The workers read from the accepted transactions topic and compute the new values of the stateful fields. They are partitioned according to the warehouse that the transaction affects, and maintain a number of state stores which hold the current values for data in this partition. They are implemented as a Kafka Streams stream processor.

When the worker has produced the transaction results, it writes them to the _transaction results_ topic so that the application can access them. It also writes an event to the _modifications_ topic with the transaction input and new fields, so that it can be processed by the views.

The views in this project store the data in memory, and communicate with the application using Java RMI. They consume the modifications topic and update their representations of the data, which are optimised for read queries. Practical implementations are likely to use a different approach, but it is not required—because all of the changes are persistent in the modifications topic, the entire view can be repopulated if a failure occurs.
