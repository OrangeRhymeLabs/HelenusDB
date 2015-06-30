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
import com.orangerhymelabs.orangedb.cassandra.index.IndexField;
import com.orangerhymelabs.orangedb.cassandra.table.Table;

/**
 * Creates BoundStatements for maintaining index table (ITable) entries. There is one ItableStatementFactory per Table
 * per keyspace.
 * 
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

	private String keyspace;
	private Table table;

	public ItableStatementFactory(String keyspace, Table table)
    {
		super();
		this.table = table;
		this.keyspace = keyspace;
		initializeStatements();
    }

	private void initializeStatements()
    {
	    // TODO Auto-generated method stub
    }

	public List<BoundStatement> createIndexEntryCreateStatements(Document document)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

		List<BoundStatement> stmts = new ArrayList<BoundStatement>(table.indexes().size());

		for (Index index : table.indexes())
		{
			BoundStatement stmt = createIndexEntryCreateStatment(document, index);

			if (stmt != null)
			{
				stmts.add(stmt);
			}
		}

	    return stmts;
    }

	public List<BoundStatement> createIndexEntryUpdateStatements(Document document, Document previous)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

		List<BoundStatement> stmts = new ArrayList<BoundStatement>(table.indexes().size());

		for (Index index : table.indexes())
		{
			if (isIndexKeyChanged(document, previous, index))
			{
				BoundStatement creStmt = createIndexEntryCreateStatment(document, index);
	
				if (creStmt != null)
				{
					stmts.add(creStmt);
				}
	
				BoundStatement delStmt = createIndexEntryDeleteStatment(document, index);
	
				if (delStmt != null)
				{
					stmts.add(delStmt);
				}
			}
			else
			{
				BoundStatement updStmt = createIndexEntryUpdateStatment(document, index);

				if (updStmt != null)
				{
					stmts.add(updStmt);
				}
			}
		}

	    return stmts;
    }

	public List<BoundStatement> createIndexEntryDeleteStatements(Document document)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

		List<BoundStatement> stmts = new ArrayList<BoundStatement>(table.indexes().size());

		for (Index index : table.indexes())
		{
			BoundStatement stmt = createIndexEntryDeleteStatment(document, index);

			if (stmt != null)
			{
				stmts.add(stmt);
			}
		}

	    return stmts;
    }

	private BoundStatement createIndexEntryCreateStatment(Document document, Index index)
	{
		List<IndexField> fields = index.fieldSpecs();
		return null;
	}

	private BoundStatement createIndexEntryUpdateStatment(Document document, Index index)
	{
		return null;
	}

	private BoundStatement createIndexEntryDeleteStatment(Document document, Index index)
	{
		return null;
	}

	/**
	 * Determines if the key(s) for the index have changed in this version of the document.
	 * If so, returns true. Otherwise, false.
	 * 
	 * @param document the new version of the document.
	 * @param previous the previous version of the document.
	 * @param index the Index that informs the key(s) to check for deltas.
	 * @return True if the indexed key(s) have changed. Otherwise, false.
	 */
	private boolean isIndexKeyChanged(Document document, Document previous,Index index)
    {
	    return true;
    }
}
