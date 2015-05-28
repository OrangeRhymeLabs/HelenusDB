package com.orangerhymelabs.orangedb.cassandra;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class SchemaRegistryTest
{
	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		SchemaRegistry.instance().initializeAll(CassandraManager.session(), CassandraManager.keyspace());
	}

	@AfterClass
	public static void afterClass()
	{
		SchemaRegistry.instance().dropAll(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void keyspaceShouldExist()
	{
		ResultSet rs = CassandraManager.session().execute(String.format("SELECT count(*) FROM system.schema_keyspaces where keyspace_name='%s'", CassandraManager.keyspace()));
		Row row = rs.one();
		assertTrue(row.getLong(0) > 0);
	}

	@Test
	public void databaseTableShouldExist()
	{
		String tableName = "sys_db";
		ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system.schema_columnfamilies where keyspace_name='%s' and columnfamily_name='%s'", CassandraManager.keyspace(), tableName));
		Row row = rs.one();
		assertTrue(row.getLong(0) > 0);
	}

	@Test
	public void tableTableShouldExist()
	{
		String tableName = "sys_tbl";
		ResultSet rs = CassandraManager.session().execute(String.format("select count(*) from system.schema_columnfamilies where keyspace_name='%s' and columnfamily_name='%s'", CassandraManager.keyspace(), tableName));
		Row row = rs.one();
		assertTrue(row.getLong(0) > 0);
	}
}
