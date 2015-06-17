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
package com.orangerhymelabs.orangedb.cassandra.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.cassandra.ReadInCallback;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DatabaseRepositoryReadInTest
{
	private static final int CALLBACK_TIMEOUT = 2000;
	private static final Identifier[] IDS = {
		new Identifier("dba1"),
		new Identifier("dba3"),
		new Identifier("dba5"),
		new Identifier("dba7"),
		new Identifier("dba21") // doesn't exist.
	};

	private static KeyspaceSchema keyspace;
	private static DatabaseRepository databases;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
		databases = new DatabaseRepository(CassandraManager.cluster().connect(CassandraManager.keyspace()), CassandraManager.keyspace());
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldReadIn()
	throws InterruptedException
	{
		populateDatabase("dba", 10);
		shouldReturnListSynchronously();
		shouldReturnListAsynchronously();
	}

	private void populateDatabase(String prefix, int count)
    {
		Database db = new Database();

		for (int i = 1; i <= count; i++)
		{
			db.name(prefix + i);
			databases.create(db);
		}
    }

	private void shouldReturnListSynchronously()
    {
		assertDatabases(databases.readIn(IDS));
    }

	private void shouldReturnListAsynchronously()
	throws InterruptedException
    {
		ReadInCallback<Database> callback = new ReadInCallback<Database>(5); // 5 forces a timeout, but checks that 'dba21' doesn't have a value in the list.
		databases.readInAsync(callback, IDS);
		waitFor(callback);

		assertDatabases(callback.entities());
    }

	private void assertDatabases(List<Database> dbs)
    {
	    assertNotNull(dbs);
		assertFalse(dbs.isEmpty());
		assertEquals(4, dbs.size());
		Collections.sort(dbs, new Comparator<Database>()
		{
			@Override
            public int compare(Database o1, Database o2)
            {
	            return o1.name().compareTo(o2.name());
            }
		});

		assertEquals("dba1", dbs.get(0).name());
		assertEquals("dba3", dbs.get(1).name());
		assertEquals("dba5", dbs.get(2).name());
		assertEquals("dba7", dbs.get(3).name());
    }

	private void waitFor(ReadInCallback<Database> callback)
	throws InterruptedException
    {
		synchronized(callback)
		{
			callback.wait(CALLBACK_TIMEOUT);
		}
    }
}
