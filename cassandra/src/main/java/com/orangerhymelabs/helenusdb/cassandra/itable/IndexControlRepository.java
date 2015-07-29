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
package com.orangerhymelabs.helenusdb.cassandra.itable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenusdb.cassandra.Schemaable;

/**
 * @author tfredrich
 * @since Jul 27, 2015
 */
public class IndexControlRepository
{
	private static final String TABLE_NAME = "sys_ctrl";

	public static class Schema
	implements Schemaable
	{
		private static final String DROP_TABLE = "drop table if exists %s." + TABLE_NAME;
		private static final String CREATE_TABLE = "create table %s." + TABLE_NAME +
		"(" +
			"tbl_name text," +
			// TODO: figure out the type of the doc_id
			"doc_id uuid," +
			"property text," +  // the name of the indexed property component

		    //"value %s," + // the actual value of the property, which could be any type.
		    "inserted_at timeuuid," +
		    "primary key ((tbl_name), doc_id, property)" +
		")";

		@Override
		public boolean drop(Session session, String keyspace)
		{
			ResultSet rs = session.execute(String.format(Schema.DROP_TABLE, keyspace));
			return rs.wasApplied();
		}

		@Override
		public boolean create(Session session, String keyspace)
		{
			ResultSet rs = session.execute(String.format(Schema.CREATE_TABLE, keyspace));
			return rs.wasApplied();
		}
	}

	private class Columns
	{
		static final String TABLE_NAME = "tbl_name";
		static final String DOCUMENT_ID = "doc_id";
		static final String PROPERTY = "property";
		// static final String VALUE = "value";
		static final String INSERTED_AT = "inserted_at";
	}
}
