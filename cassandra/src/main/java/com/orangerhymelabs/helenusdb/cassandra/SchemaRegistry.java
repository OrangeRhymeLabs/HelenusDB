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
package com.orangerhymelabs.helenusdb.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenusdb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenusdb.cassandra.index.IndexRepository;
import com.orangerhymelabs.helenusdb.cassandra.itable.IndexControlRepository;
import com.orangerhymelabs.helenusdb.cassandra.meta.MetadataRepository;
import com.orangerhymelabs.helenusdb.cassandra.table.TableRepository;

/**
 * A Singleton object to drop and/or (re)create the database schema in Cassandra.
 * 
 * @author tfredrich
 * @since May 7, 2015
 */
public class SchemaRegistry
{
	private static final SchemaRegistry INSTANCE = new SchemaRegistry();

	static
	{
		INSTANCE.register(new KeyspaceSchema());
		INSTANCE.register(new MetadataRepository.Schema());
		INSTANCE.register(new DatabaseRepository.Schema());
		INSTANCE.register(new TableRepository.Schema());
		INSTANCE.register(new IndexRepository.Schema());
		INSTANCE.register(new IndexControlRepository.Schema());
	}

	private List<Schemaable> schemas = new ArrayList<Schemaable>();

	private SchemaRegistry()
	{
		// prevents instantiation.
	}

	public static SchemaRegistry instance()
	{
		return INSTANCE;
	}

	/**
	 * Order matters!
	 * 
	 * @param schema
	 * @return
	 */
	public SchemaRegistry register(Schemaable schema)
	{
		if (schema != null)
		{
			schemas.add(schema);
		}

		return this;
	}

	public void initializeAll(Session session, String keyspace)
	{
		for (Schemaable schema : schemas)
		{
			schema.drop(session, keyspace);
			schema.create(session, keyspace);
		}
	}

	public void createAll(Session session, String keyspace)
	{
		for (Schemaable schema : schemas)
		{
			schema.create(session, keyspace);
		}
	}

	public void dropAll(Session session, String keyspace)
	{
		for (Schemaable schema : schemas)
		{
			schema.drop(session, keyspace);
		}
	}
}
