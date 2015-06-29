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
package com.orangerhymelabs.orangedb.cassandra.itable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.orangerhymelabs.orangedb.cassandra.document.Document;
import com.orangerhymelabs.orangedb.cassandra.index.Index;
import com.orangerhymelabs.orangedb.cassandra.table.Table;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class ItableStatementFactory
{
	public static class Schema
    {
		private static final String DROP_TABLE = "drop table if exists %s.%s";
		private static final String CREATE_TABLE = "create table %s.%s" +
			"(" +
				"bucket_id bigint," +
				"%s," + // arbitrary blank area for creating the actual fields that are indexed.
				"object_id %s," +
				"object blob," +
				"created_at timestamp," +
				"updated_at timestamp," +
				"primary key ((bucket_id), %s)" +
			")" +
			"WITH CLUSTERING ORDER BY (%s)";

		public boolean drop(Session session, String keyspace, String table)
		{
			ResultSet rs = session.execute(String.format(Schema.DROP_TABLE, keyspace, table));
			return rs.wasApplied();
		}

		public boolean create(Session session, String keyspace, String table, FieldType oidType, String columnDefs, String pkDefs, String ordering)
		{
			ResultSet rs = session.execute(String.format(Schema.CREATE_TABLE, keyspace, table, columnDefs, oidType.cassandraType(), pkDefs, ordering));
			return rs.wasApplied();
		}
    }

	private Table table;

	public ItableStatementFactory(Table table)
    {
		super();
		this.table = table;
    }

	public List<BoundStatement> createIndexCreateStatements(Document document)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

		List<BoundStatement> stmts = new ArrayList<BoundStatement>(table.indexes().size());

		for (Index index : table.indexes())
		{
			BoundStatement stmt = createIndexCreateStatment(document, index);

			if (stmt != null)
			{
				stmts.add(stmt);
			}
		}

	    return stmts;
    }

	public List<BoundStatement> createIndexUpdateStatements(Document document, Document previous)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

	    return null;
    }

	public List<BoundStatement> createIndexDeleteStatements(Document document)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

		List<BoundStatement> stmts = new ArrayList<BoundStatement>(table.indexes().size());

		for (Index index : table.indexes())
		{
			BoundStatement stmt = createIndexDeleteStatment(document, index);

			if (stmt != null)
			{
				stmts.add(stmt);
			}
		}

	    return stmts;
    }

	private BoundStatement createIndexCreateStatment(Document document, Index index)
	{
		return null;
	}

	private BoundStatement createIndexDeleteStatment(Document document, Index index)
	{
		return null;
	}
}
