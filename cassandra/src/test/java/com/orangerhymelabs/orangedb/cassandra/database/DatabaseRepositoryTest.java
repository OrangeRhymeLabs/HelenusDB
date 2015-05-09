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
	public void shouldCreateReadAndDeleteDatabase()
	throws Exception
	{
		Database entity = new Database();
		entity.name("db1");
		entity.description("a test database");
		ResultSetFuture future = databases.create(entity);
        ResultSet rs = future.get();
		Database result = databases.marshalRow(rs.one());
		assertEquals(entity, result);
		assertNotNull(result.createdAt());
		assertNotNull(result.updatedAt());
	}
}
