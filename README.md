[![Stories in Ready](https://badge.waffle.io/orangerhymelabs/orangedb.svg?label=ready&title=Ready)](http://waffle.io/orangerhymelabs/orangedb)

#OrangeDB: A REST-based, Document-Oriented Cassandra
---

**OrangeDB** marries MongoDB's simple data storage model with the horizontal scaling of Cassandra. It enables developers to store arbitrary payloads as
BSON, as they would with MongoDB, in a Cassandra cluster. It supports indexing, filtering, sorting, querying and pagination
(via familiar limit and offset semantics). Simple json document storage with effortless scaling exposed as a service - **that's OrangeDB**!

## Five Minute Quick Start
Let's begin by starting a server and beginning to store data. 

### Prerequisites 
A running Cassandra instance and maven (for building Java packages). 

### Setting Up the Cassandra Datastore
Before we can start the Docussandra API server that talks to Cassandra we have to create the keyspace where data will be stored. We do this by loading the schema found in the **src/main/resources/docussandra.cql** directory. On the command line this looks like:

> cqlsh -f src/main/resources/docussandra.cql

### Starting the OrangeDB API

Next we need to build and run the API server. First, if using a command line, navigate to the **api** project folder in the root project. Once there, type:
> mvn exec:java -Dexec.mainClass="com.strategicgains.docussandra.Main"

If successful, the Docussandra will now be listening to requests on **127.0.0.1:8081**.

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

