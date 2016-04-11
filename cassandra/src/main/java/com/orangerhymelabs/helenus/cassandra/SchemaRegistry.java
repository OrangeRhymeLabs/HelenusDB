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
package com.orangerhymelabs.helenus.cassandra;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.cassandra.bucket.IndexControlRepository;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.index.IndexRepository;
import com.orangerhymelabs.helenus.cassandra.meta.MetadataRepository;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;

/**
 * A Singleton object to drop and/or (re)create the database schema in Cassandra.
 * 
 * @author tfredrich
 * @since May 7, 2015
 */
public class SchemaRegistry
{
	private static final SchemaRegistry INSTANCE = new SchemaRegistry();

	// Registration order matters...
	static
	{
		INSTANCE.register(new KeyspaceSchema());
		INSTANCE.register(new MetadataRepository.Schema());
		INSTANCE.register(new DatabaseRepository.Schema());
		INSTANCE.register(new TableRepository.Schema());
		INSTANCE.register(new IndexRepository.Schema());
		INSTANCE.register(new IndexControlRepository.Schema());
	}

	private List<SchemaProvider> schemas = new ArrayList<SchemaProvider>();

	private SchemaRegistry()
	{
		// prevents instantiation.
	}

	public static SchemaRegistry instance()
	{
		return INSTANCE;
	}

	/**
	 * Registration order matters!
	 * 
	 * @param provider
	 * @return this schema registry
	 */
	public SchemaRegistry register(SchemaProvider provider)
	{
		if (provider != null)
		{
			schemas.add(provider);
		}

		return this;
	}

	/**
	 * Drops all schema providers, then creates all.
	 * 
	 * @param session
	 * @param keyspace
	 */
	public void initializeAll(Session session, String keyspace)
	{
		for (SchemaProvider schema : schemas)
		{
			schema.drop(session, keyspace);
			schema.create(session, keyspace);
		}
	}

	public void createAll(Session session, String keyspace)
	{
		for (SchemaProvider schema : schemas)
		{
			schema.create(session, keyspace);
		}
	}

	public void dropAll(Session session, String keyspace)
	{
		for (SchemaProvider schema : schemas)
		{
			schema.drop(session, keyspace);
		}
	}
}
