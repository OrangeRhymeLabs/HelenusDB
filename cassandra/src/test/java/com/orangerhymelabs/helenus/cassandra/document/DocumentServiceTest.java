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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.bson.BSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.util.JSON;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.SchemaRegistry;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.database.Database;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.document.Document;
import com.orangerhymelabs.helenus.cassandra.document.DocumentRepositoryFactoryImpl;
import com.orangerhymelabs.helenus.cassandra.document.DocumentService;
import com.orangerhymelabs.helenus.cassandra.index.IndexRepository;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;

/**
 * @author tfredrich
 * @since Jun 16, 2015
 */
public class DocumentServiceTest
{
	private static final String DATES_TABLE = "dates";
	private static final String UUIDS_TABLE = "uuids";
	private static final String DB_NAME = "db3";
	private static final int CALLBACK_TIMEOUT = 2000;
	private static final BSONObject BSON = (BSONObject) JSON.parse("{'a':'some', 'b':1, 'c':'excitement'}");

	private static DocumentService allDocs;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		SchemaRegistry.instance().createAll(CassandraManager.session(), CassandraManager.keyspace());
		
		DatabaseRepository dbs = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		TableService tables = new TableService(dbs,
			new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace()));

		Database db = new Database();
		db.name(DB_NAME);
		db.description("DB for DocumentServiceTest Documents");
		dbs.create(db);

		Table uuids = new Table();
		uuids.name(UUIDS_TABLE);
		uuids.database(DB_NAME);
		uuids.description("a test UUID-keyed table");
		tables.create(uuids);

		Table dates = new Table();
		dates.name(DATES_TABLE);
		dates.database(DB_NAME);
		dates.idType(DataTypes.TIMESTAMP);
		dates.description("a test date-keyed table");
		tables.create(dates);

		allDocs = new DocumentService(tables,
			new DocumentRepositoryFactoryImpl(CassandraManager.session(), CassandraManager.keyspace(),
				new IndexRepository(CassandraManager.session(), CassandraManager.keyspace())));
	}

	@AfterClass
	public static void afterClass()
	{
		SchemaRegistry.instance().dropAll(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldCRUDUUIDsSynchronously()
	throws Exception
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		Document createResult = allDocs.create(DB_NAME, UUIDS_TABLE, doc);

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = allDocs.read(DB_NAME, UUIDS_TABLE, doc.id());
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = allDocs.update(DB_NAME, UUIDS_TABLE, doc);
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = allDocs.read(DB_NAME, UUIDS_TABLE, doc.id());
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		allDocs.delete(DB_NAME, UUIDS_TABLE, doc.id());

		// Re-Read
		try
		{
			allDocs.read(DB_NAME, UUIDS_TABLE, doc.id());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Document not deleted: " + doc.identifier().toString());
	}

	@Test
	public void shouldCRUDAltIdSynchronously()
	throws Exception
	{
		// Create
		Date now = new Date();
		Document doc = new Document();
		doc.id(now);
		Document createResult = allDocs.create(DB_NAME, DATES_TABLE, doc);

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = allDocs.read(DB_NAME, DATES_TABLE, doc.id());
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = allDocs.update(DB_NAME, DATES_TABLE, doc);
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = allDocs.read(DB_NAME, DATES_TABLE, doc.id());
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		allDocs.delete(DB_NAME, DATES_TABLE, doc.id());

		// Re-Read
		try
		{
			allDocs.read(DB_NAME, DATES_TABLE, doc.id());
		}
		catch (ItemNotFoundException e)
		{
			return;
		}

		fail("Document not deleted: " + doc.identifier().toString());
	}

	@Test
	public void shouldCRUDUUIDAsynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.createAsync(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		allDocs.readAsync(DB_NAME, UUIDS_TABLE, doc.id(), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		allDocs.updateAsync(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		allDocs.readAsync(DB_NAME, UUIDS_TABLE, doc.id(), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		allDocs.deleteAsync(DB_NAME, UUIDS_TABLE, doc.id(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		allDocs.readAsync(DB_NAME, UUIDS_TABLE, doc.id(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldCRUDAltIdAsynchronously()
	throws InterruptedException
	{
		Date now = new Date();
		Document doc = new Document();
		doc.id(now);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.createAsync(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		allDocs.readAsync(DB_NAME, DATES_TABLE, doc.id(), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		allDocs.updateAsync(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		allDocs.readAsync(DB_NAME, DATES_TABLE, doc.id(), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		callback.clear();
		allDocs.deleteAsync(DB_NAME, DATES_TABLE, doc.id(), callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Re-Read
		callback.clear();
		allDocs.readAsync(DB_NAME, DATES_TABLE, doc.id(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		Document createResult = allDocs.create(DB_NAME, UUIDS_TABLE, doc);
		assertEquals(doc, createResult);

		allDocs.create(DB_NAME, UUIDS_TABLE, doc);
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.createAsync(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertTrue(callback.isEmpty());

		// Create Duplicate
		allDocs.createAsync(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnCreateInvalidIdSynchronously()
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		allDocs.create(DB_NAME, DATES_TABLE, doc);
	}

	@Test
	public void shouldThrowOnCreateInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		TestCallback<Document> callback = new TestCallback<Document>();

		allDocs.createAsync(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnUpdateInvalidIdSynchronously()
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		allDocs.update(DB_NAME, DATES_TABLE, doc);
	}

	@Test
	public void shouldThrowOnUpdateInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.id(id);
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.updateAsync(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentSynchronously()
	{
		allDocs.read(DB_NAME, UUIDS_TABLE, UUID.randomUUID());
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentAltIdSynchronously()
	{
		allDocs.read(DB_NAME, DATES_TABLE, new Date());
	}

	@Test
	public void shouldThrowOnReadNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		allDocs.readAsync(DB_NAME, UUIDS_TABLE, UUID.randomUUID(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldThrowOnReadNonExistentAltIdAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		allDocs.readAsync(DB_NAME, DATES_TABLE, new Date(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	{
		Document doc = new Document();
		doc.id(UUID.randomUUID());
		allDocs.update(DB_NAME, UUIDS_TABLE, doc);
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		Document doc = new Document();
		doc.id(UUID.randomUUID());
		allDocs.updateAsync(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnReadInvalidIdSynchronously()
	{
		Document doc = new Document();
		doc.id(UUID.randomUUID());
		allDocs.read(DB_NAME, DATES_TABLE, doc.id());
	}

	@Test
	public void shouldThrowOnReadInvalidIdAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		Document doc = new Document();
		doc.id(UUID.randomUUID());
		allDocs.readAsync(DB_NAME, DATES_TABLE, doc.id(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	private void waitFor(TestCallback<Document> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
