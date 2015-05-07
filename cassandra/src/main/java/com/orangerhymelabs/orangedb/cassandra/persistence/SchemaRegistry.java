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
package com.orangerhymelabs.orangedb.cassandra.persistence;

import java.util.ArrayList;
import java.util.List;

/**
 * A Singleton object to drop and/or (re)create the database schema in Cassandra.
 * 
 * @author toddf
 * @since May 7, 2015
 */
public class SchemaRegistry
{
	private static final SchemaRegistry INSTANCE = new SchemaRegistry();

	private List<Schemaable> schemas = new ArrayList<Schemaable>();

	private SchemaRegistry()
	{
		// prevents instantiation.
	}

	public static SchemaRegistry instance()
	{
		return INSTANCE;
	}

	public SchemaRegistry register(Schemaable schema)
	{
		if (schema != null)
		{
			schemas.add(schema);
		}

		return this;
	}

	public void initializeAll()
	{
		for (Schemaable schema : schemas)
		{
			schema.dropSchema();
			schema.createSchema();
		}
	}

	public void createAll()
	{
		for (Schemaable schema : schemas)
		{
			schema.createSchema();
		}
	}

	public void dropAll()
	{
		for (Schemaable schema : schemas)
		{
			schema.dropSchema();
		}
	}
}
