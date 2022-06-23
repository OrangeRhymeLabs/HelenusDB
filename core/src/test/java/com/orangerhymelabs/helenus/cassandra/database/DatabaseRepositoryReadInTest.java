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
package com.orangerhymelabs.helenus.cassandra.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.CassandraManager;
import com.orangerhymelabs.helenus.cassandra.KeyspaceSchema;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DatabaseRepositoryReadInTest
{
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
	throws Exception
	{
		populateDatabase("dba", 10);
		shouldReturnListAsynchronously();
	}

	private void populateDatabase(String prefix, int count)
	throws InterruptedException, ExecutionException
    {
		Database db = new Database();

		for (int i = 1; i <= count; i++)
		{
			db.name(prefix + i);
			databases.create(db).get();
		}
    }

	private void shouldReturnListAsynchronously()
	throws Exception
    {
		ListenableFuture<List<Database>> dbs = databases.readIn(IDS);
		List<Database> list = new ArrayList<>(dbs.get());
		assertDatabases(list);
    }

	private void assertDatabases(List<Database> dbs)
    {
	    assertNotNull(dbs);
		assertFalse(dbs.isEmpty());
		assertEquals(5, dbs.size());
		Collections.sort(dbs, new Comparator<Database>()
		{
			@Override
            public int compare(Database o1, Database o2)
            {
				if (o1 == null && o2 == null) return 0;
				if (o1 == null && o2 != null) return -1;
				if (o1 != null && o2 == null) return 1;
	            return o1.name().compareTo(o2.name());
            }
		});

		assertEquals("dba1", dbs.get(1).name());
		assertEquals("dba3", dbs.get(2).name());
		assertEquals("dba5", dbs.get(3).name());
		assertEquals("dba7", dbs.get(4).name());
    }
}
