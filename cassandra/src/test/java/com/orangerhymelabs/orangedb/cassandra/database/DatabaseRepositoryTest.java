package com.orangerhymelabs.orangedb.cassandra.database;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orangerhymelabs.orangedb.cassandra.CassandraManager;
import com.orangerhymelabs.orangedb.cassandra.KeyspaceSchema;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;

public class DatabaseRepositoryTest
{
	private static KeyspaceSchema keyspace;

	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
		keyspace = new KeyspaceSchema();
		keyspace.create(CassandraManager.session(), CassandraManager.keyspace());
		new DatabaseRepository.Schema().create(CassandraManager.session(), CassandraManager.keyspace());
//		databases = new DatabaseRepository(CassandraManager.session(), CassandraManager.keyspace());
//		databases.createSchema();
	}

	@AfterClass
	public static void afterClass()
	{
		keyspace.drop(CassandraManager.session(), CassandraManager.keyspace());		
	}

	@Test
	public void shouldCreateReadAndDeleteDatabase()
	{
		DatabaseRepository databases = new DatabaseRepository(CassandraManager.session(), CassandraManager.keyspace());
		Database entity = new Database();
		entity.name("db1");
		entity.description("a test database");
		databases.create(entity, new ResultCallback<Database>()
		{
			@Override
			public void onSuccess(Database result)
			{
				databases.read(result.getId(), new ResultCallback<Database>()
				{
					@Override
                    public void onSuccess(Database result)
                    {
						assertEquals(entity, result);
						databases.delete(result.getId(), new ResultCallback<Database>()
						{
							@Override
                            public void onSuccess(Database result)
                            {
								System.out.println("deleted: " + result.name());
                            }

							@Override
                            public void onFailure(Throwable t)
                            {
								fail(t.getMessage());
                            }
						});
                    }

					@Override
                    public void onFailure(Throwable t)
                    {
						fail(t.getMessage());
                    }
				});
			}
			
			@Override
			public void onFailure(Throwable t)
			{
				fail(t.getMessage());
			}
		});
	}
}
