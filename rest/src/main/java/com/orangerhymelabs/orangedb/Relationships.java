/*
    Copyright 2014, Strategic Gains, Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package com.orangerhymelabs.orangedb;

import static com.orangerhymelabs.orangedb.cassandra.Constants.Routes.DATABASE;
import static com.orangerhymelabs.orangedb.cassandra.Constants.Routes.DATABASES;
import static com.orangerhymelabs.orangedb.cassandra.Constants.Routes.TABLE;
import static com.orangerhymelabs.orangedb.cassandra.Constants.Routes.TABLES;
import static com.strategicgains.hyperexpress.RelTypes.SELF;
import static com.strategicgains.hyperexpress.RelTypes.UP;

import java.util.Map;

import org.restexpress.RestExpress;

import com.orangerhymelabs.orangedb.cassandra.database.Database;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.strategicgains.hyperexpress.HyperExpress;

/**
 * @author toddf
 * @since Jun 12, 2014
 */
public class Relationships
{
	public static void define(RestExpress server)
	{
		Map<String, String> routes = server.getRouteUrlsByName();

		HyperExpress.relationships()
		.forCollectionOf(Database.class)
			.rel(SELF, routes.get(DATABASES))

		.forClass(Database.class)
			.rel(SELF, routes.get(DATABASE))
			.rel(UP, routes.get(DATABASES))
			.rel("collections", routes.get(TABLES))
				.title("The collections in this namespace")

		.forCollectionOf(Table.class)
			.rel(SELF, routes.get(TABLES))
			.rel(UP, routes.get(DATABASE))
				.title("The namespace containing this collection")

		.forClass(Table.class)
			.rel(SELF, routes.get(TABLE))
			.rel(UP, routes.get(TABLES))
				.title("The entire list of collections in this namespace");
//			.rel("documents", routes.get(DOCUMENTS))
//				.title("The documents in this collection")
//
//		.forCollectionOf(Document.class)
//			.rel(SELF, routes.get(DOCUMENTS))
//			.rel(UP, routes.get(TABLE))
//				.title("The collection containing this document")
//
//		.forClass(Document.class)
//			.rel(SELF, routes.get(DOCUMENT))
//			.rel(UP, routes.get(DOCUMENTS))
//				.title("The entire list of documents in this collection");
	}
}
