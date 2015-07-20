[![Stories in Ready](https://badge.waffle.io/orangerhymelabs/helenusdb.svg?label=ready&title=Ready)](http://waffle.io/orangerhymelabs/helenusdb)

#HelenusDB: A REST-based, Document-Oriented Cassandra
---

**HelenusDB** marries a simple BSON-document storage model (like MongoDB) with the truly horizontal, global, multi-datacenter scalability of Cassandra. It enables developers
to store arbitrary payloads as JSON/BSON, as they would with MongoDB, in a Cassandra cluster. It supports indexing, filtering, sorting, querying and pagination
(via familiar 'limit' and 'offset' query semantics). Simple json document storage with effortless scaling, exposed as a REST API or embedded in your application - **that's HelenusDB**!

## About the Name
In Greek mythology, Helenus (/ˈhɛlənəs/; Ancient Greek: Ἕλενος) was the son of King Priam and Queen Hecuba of Troy, and the **twin brother of the prophetess Cassandra.** According to legend, Cassandra, having been given the power of prophecy by Apollo, taught it to her brother. He had the benefit, unlike Cassandra, that people believed him.

HelenusDB makes Cassandra easier for developers to use, yet has the truly linear, global and multi-datacenter scalability of Cassandra, unlike any other document-oriented storage technology. 

## Five Minute Quick Start
Let's begin by starting a server and beginning to store data. 

### Prerequisites 
A running Cassandra instance and maven (for building Java packages). 

### Starting the HelenusDB API

We need to build and run the API server. First, if using a command line, navigate to the **rest** project folder in the root project. Once there, type:
> mvn exec:java -Dexec.mainClass="com.orangerhymelabs.helenusdb.Main"

If successful, the HelenusDB will now be listening to requests on **127.0.0.1:8081**.

### Creating a Database
Once the server has been started we can begin to interact with it. The first step is a create a database in which to store data. This can be done by making a **POST** request to:
> http://localhost:8081/*{databaseName}*

The *{databaseName}* variable mentioned above can be whatever the user would like *as long as it has not been previously used*. Names must be **globally unique** and **in lower case**. Otherwise an error will result.

### Creating a Table
To create a logical grouping of similar data we'll create a table. We can do this by making another **POST** request to:
> http://localhost:8081/*{databaseName}*/*{tablename}*

Naming restrictions on the table name are similar to the database: unique *to the database* and in lower case. 

### Storing Data
- TBD

##Additional Notes

### To create a project deployable assembly (zip file):

* mvn clean package
* mvn assembly:single

### To run the project via the assembly (zip file):

* unzip 'assembly file created in above step'
* cd 'artifact sub-directory'
* java -jar 'artifact jar file' [environment name]

To run the integration tests:

*mvn clean install -P integration
