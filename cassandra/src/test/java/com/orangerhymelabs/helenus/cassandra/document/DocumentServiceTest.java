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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.mongodb.BasicDBObject;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.SchemaRegistry;
import com.orangerhymelabs.helenus.cassandra.TestCallback;
import com.orangerhymelabs.helenus.cassandra.database.Database;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.cassandra.view.ViewRepository;
import com.orangerhymelabs.helenus.cassandra.view.ViewService;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;

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
	private static final BSONObject BSON = (BSONObject) BasicDBObject.parse("{'a':'some', 'b':1, 'c':'excitement'}");

	private static DocumentService allDocs;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException, ExecutionException
	{
		CassandraManager.start();
		SchemaRegistry.instance().createAll(CassandraManager.session(), CassandraManager.keyspace());
		
		DatabaseRepository dbr = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
		DatabaseService dbs = new DatabaseService(dbr);
		TableService tables = new TableService(dbs, new TableRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace()));
		ViewService views = new ViewService(new ViewRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace()), tables);

		Database db = new Database();
		db.name(DB_NAME);
		db.description("DB for DocumentServiceTest Documents");
		dbs.create(db).get();

		Table uuids = new Table();
		uuids.name(UUIDS_TABLE);
		uuids.database(DB_NAME);
		uuids.description("a test UUID-keyed table");
		tables.create(uuids).get();

		Table dates = new Table();
		dates.name(DATES_TABLE);
		dates.database(DB_NAME);
		dates.keys("id:timestamp");
		dates.description("a test date-keyed table");
		tables.create(dates).get();

		allDocs = new DocumentService(tables, views, new DocumentRepositoryFactoryImpl(CassandraManager.session(), CassandraManager.keyspace()));
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
		doc.identifier(new Identifier(id));
		Document createResult = allDocs.create(DB_NAME, UUIDS_TABLE, doc).get();

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = allDocs.read(DB_NAME, UUIDS_TABLE, doc.identifier()).get();
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = allDocs.update(DB_NAME, UUIDS_TABLE, doc).get();
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = allDocs.read(DB_NAME, UUIDS_TABLE, doc.identifier()).get();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		allDocs.delete(DB_NAME, UUIDS_TABLE, doc.identifier()).get();

		// Re-Read
		try
		{
			allDocs.read(DB_NAME, UUIDS_TABLE, doc.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (e.getCause() instanceof ItemNotFoundException) return;
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
		doc.identifier(new Identifier(now));
		Document createResult = allDocs.create(DB_NAME, DATES_TABLE, doc).get();

		assertNotNull(createResult);
		assertEquals(doc, createResult);

		// Read
		Document result = allDocs.read(DB_NAME, DATES_TABLE, doc.identifier()).get();
		assertEquals(createResult, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		doc.object(BSON);
		Document updateResult = allDocs.update(DB_NAME, DATES_TABLE, doc).get();
		assertEquals(doc, updateResult);

		// Re-Read
		Document result2 = allDocs.read(DB_NAME, DATES_TABLE, doc.identifier()).get();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		allDocs.delete(DB_NAME, DATES_TABLE, doc.identifier()).get();

		// Re-Read
		try
		{
			allDocs.read(DB_NAME, DATES_TABLE, doc.identifier()).get();
		}
		catch (ExecutionException e)
		{
			if (e.getCause() instanceof ItemNotFoundException) return;
		}

		fail("Document not deleted: " + doc.identifier().toString());
	}

	@Test
	public void shouldCRUDUUIDAsynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.create(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		Futures.addCallback(allDocs.read(DB_NAME, UUIDS_TABLE, doc.identifier()), callback, MoreExecutors.directExecutor());
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		allDocs.update(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		allDocs.read(DB_NAME, UUIDS_TABLE, doc.identifier(), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		TestCallback<Boolean> deleteCallback = new TestCallback<>();
		allDocs.delete(DB_NAME, UUIDS_TABLE, doc.identifier(), deleteCallback);
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		allDocs.read(DB_NAME, UUIDS_TABLE, doc.identifier(), callback);
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
		doc.identifier(new Identifier(now));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.create(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Read
		callback.clear();
		allDocs.read(DB_NAME, DATES_TABLE, doc.identifier(), callback);
		waitFor(callback);

		assertEquals(doc, callback.entity());

		// Update
		callback.clear();
		doc.object(BSON);
		allDocs.update(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());

		// Re-Read
		callback.clear();
		allDocs.read(DB_NAME, DATES_TABLE, doc.identifier(), callback);
		waitFor(callback);

		Document result2 = callback.entity();
		assertEquals(doc, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		TestCallback<Boolean> deleteCallback = new TestCallback<>();
		allDocs.delete(DB_NAME, DATES_TABLE, doc.identifier(), deleteCallback);
		waitFor(deleteCallback);

		assertTrue(deleteCallback.entity());

		// Re-Read
		callback.clear();
		allDocs.read(DB_NAME, DATES_TABLE, doc.identifier(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=DuplicateItemException.class)
	public void shouldThrowOnDuplicateSynchronously()
	throws Throwable
	{
		// Create
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		Document createResult = allDocs.create(DB_NAME, UUIDS_TABLE, doc).get();
		assertEquals(doc, createResult);

		try
		{
			allDocs.create(DB_NAME, UUIDS_TABLE, doc).get();
		}
//		catch (ExecutionException e)
		catch (Exception e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnDuplicateAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.create(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		// Create Duplicate
		allDocs.create(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof DuplicateItemException);
	}

	@Test
	public void shouldUpsert()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.upsert(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		// Create Duplicate
		allDocs.upsert(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnCreateInvalidIdSynchronously()
	throws Throwable
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		try
		{
			allDocs.create(DB_NAME, DATES_TABLE, doc).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnCreateInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		allDocs.create(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnUpdateInvalidIdSynchronously()
	throws Throwable
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		try
		{
			allDocs.update(DB_NAME, DATES_TABLE, doc).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnUpdateInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.update(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnUpsertInvalidIdSynchronously()
	throws Throwable
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		try
		{
			allDocs.upsert(DB_NAME, DATES_TABLE, doc).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnUpsertInvalidIdAynchronously()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.upsert(DB_NAME, DATES_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentSynchronously()
	throws Throwable
	{
		try
		{
			allDocs.read(DB_NAME, UUIDS_TABLE, new Identifier(UUID.randomUUID())).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnReadNonExistentAltIdSynchronously()
	throws Throwable
	{
		try
		{
			allDocs.read(DB_NAME, DATES_TABLE, new Identifier(new Date())).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnReadNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		allDocs.read(DB_NAME, UUIDS_TABLE, new Identifier(UUID.randomUUID()), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test
	public void shouldReturnExistsBoolean()
	throws InterruptedException
	{
		UUID id = UUID.randomUUID();
		Document doc = new Document();
		doc.identifier(new Identifier(id));
		TestCallback<Document> callback = new TestCallback<Document>();

		// Create
		allDocs.create(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNull(callback.throwable());
		assertNotNull(callback.entity());

		TestCallback<Boolean> existsCallback = new TestCallback<Boolean>();
		allDocs.exists(DB_NAME, UUIDS_TABLE, new Identifier(id), existsCallback);
		waitFor(existsCallback);

		assertNull(existsCallback.throwable());
		assertTrue(existsCallback.entity());

		allDocs.exists(DB_NAME, UUIDS_TABLE, new Identifier(UUID.randomUUID()), existsCallback);
		waitFor(existsCallback);

		assertNull(existsCallback.throwable());
		assertFalse(existsCallback.entity());
	}

	@Test
	public void shouldThrowOnReadNonExistentAltIdAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		allDocs.read(DB_NAME, DATES_TABLE, new Identifier(new Date()), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=ItemNotFoundException.class)
	public void shouldThrowOnUpdateNonExistentSynchronously()
	throws Throwable
	{
		Document doc = new Document();
		doc.identifier(new Identifier(UUID.randomUUID()));
		try
		{
			allDocs.update(DB_NAME, UUIDS_TABLE, doc).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnUpdateNonExistentAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		Document doc = new Document();
		doc.identifier(new Identifier(UUID.randomUUID()));
		allDocs.update(DB_NAME, UUIDS_TABLE, doc, callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof ItemNotFoundException);
	}

	@Test(expected=InvalidIdentifierException.class)
	public void shouldThrowOnReadInvalidIdSynchronously()
	throws Throwable
	{
		Document doc = new Document();
		doc.identifier(new Identifier(UUID.randomUUID()));

		try
		{
			allDocs.read(DB_NAME, DATES_TABLE, doc.identifier()).get();
		}
		catch (ExecutionException e)
		{
			throw e.getCause();
		}
	}

	@Test
	public void shouldThrowOnReadInvalidIdAsynchronously()
	throws InterruptedException
	{
		TestCallback<Document> callback = new TestCallback<Document>();
		Document doc = new Document();
		doc.identifier(new Identifier(UUID.randomUUID()));
		allDocs.read(DB_NAME, DATES_TABLE, doc.identifier(), callback);
		waitFor(callback);

		assertNotNull(callback.throwable());
		assertTrue(callback.throwable() instanceof InvalidIdentifierException);
	}

	private void waitFor(TestCallback<?> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
