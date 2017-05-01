/*
    Copyright 2015, Strategic Gains, Inc.

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
package com.orangerhymelabs.helenus.cassandra.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.bson.BSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.util.JSON;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.SchemaRegistry;
import com.orangerhymelabs.helenus.cassandra.database.Database;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.view.View;
import com.orangerhymelabs.helenus.cassandra.view.ViewRepository;
import com.orangerhymelabs.helenus.cassandra.view.ViewService;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 16, 2015
 */
public class ViewStorageTest
{
	private static final BSONObject BSON = (BSONObject) JSON.parse("{'a':'some', 'b':1, 'c':'excitement'}");

	private static AbstractDocumentRepository uuidDocs;
	private static AbstractDocumentRepository dateDocs;
	private static DocumentService allDocs;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException, ExecutionException, KeyDefinitionException
	{
		CassandraManager.start();
		SchemaRegistry.instance().createAll(CassandraManager.session(), CassandraManager.keyspace());
		
		DatabaseRepository dbr = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseService dbs = new DatabaseService(dbr);
		TableService tables = new TableService(dbs, new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace()));
		ViewService views = new ViewService(new ViewRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace()), tables);

		DocumentRepositoryFactory factory = new DocumentRepositoryFactoryImpl(CassandraManager.session(), CassandraManager.keyspace());

		Database db = new Database();
		db.name("db1");
		db.description("DB for Test Documents");
		dbs.create(db).get();

		Table uuids = new Table();
		uuids.name("uuids");
		uuids.database("db1");
		uuids.description("a test UUID-keyed table");
		uuids.keys("id:uuid");
		Table uuidTable = tables.create(uuids).get();

		View dates = new View();
		dates.name("dates");
		dates.table(uuidTable);
		dates.keys("createdAt:timestamp");
		dates.description("a test date-keyed table");
		View dateView = views.create(dates).get();

		uuidDocs = factory.newInstance(uuidTable);
		dateDocs = factory.newInstance(dateView);
		allDocs = new DocumentService(tables, views, new DocumentRepositoryFactoryImpl(CassandraManager.session(), CassandraManager.keyspace()));
	}

	@AfterClass
	public static void afterClass()
	{
		SchemaRegistry.instance().dropAll(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldWriteView()
	throws Exception
	{
		// Create
		Date createdAt = new Date();
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		BSON.put("createdAt", createdAt);
		doc.object(BSON);
		Document createResult = allDocs.create("db1", "uuids", doc).get();

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = uuidDocs.read(doc.identifier()).get();
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Read by Date
		Document result2 = dateDocs.read(new Identifier(createdAt)).get();
		assertEquals(doc.object(), result2.object());
		assertNotEquals(result2.createdAt(), doc.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());
	}
}
