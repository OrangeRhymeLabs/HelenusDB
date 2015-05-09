package com.orangerhymelabs.orangedb.cassandra.database;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;

public class DatabaseRepositoryTest
{
	private static KeyspaceSchema keyspace;
	private static DatabaseRepository databases;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.useLocalReplication();
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
	public void shouldCRUDDatabase()
	throws Exception
	{
		// Create
		Database entity = new Database();
		entity.name("db1");
		entity.description("a test database");
		databases.create(entity).get();

		// Read
		ResultSet rs = databases.read(entity.getId()).get();
		Database result = databases.marshalRow(rs.one());
		assertEquals(entity, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());

		// Update
		entity.description("an updated test database");
		databases.update(entity).get();

		// Re-Read
		ResultSet rs2 = databases.read(entity.getId()).get();
		Database result2 = databases.marshalRow(rs2.one());
		assertEquals(entity, result2);
		assertNotEquals(result2.createdAt(), result2.updatedAt());
		assertNotNull(result2.createdAt());
		assertNotNull(result2.updatedAt());

		// Delete
		databases.delete(entity.getId()).get();

		// Re-Read
		ResultSet rs3 = databases.read(entity.getId()).get();
		assertTrue(rs3.all().isEmpty());
	}
}
