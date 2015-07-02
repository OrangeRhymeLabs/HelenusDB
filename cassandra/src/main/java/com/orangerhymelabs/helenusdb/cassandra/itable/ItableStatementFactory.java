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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenusdb.cassandra.FieldType;
import com.orangerhymelabs.helenusdb.cassandra.document.Document;
import com.orangerhymelabs.helenusdb.cassandra.index.Index;
import com.orangerhymelabs.helenusdb.cassandra.index.IndexField;
import com.orangerhymelabs.helenusdb.cassandra.table.Table;

/**
 * Creates BoundStatements for maintaining index table (ITable) entries. There is one ItableStatementFactory per Table
 * per keyspace.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class ItableStatementFactory
{
	private static final String QUESTION_MARKS = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,";

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

	private class Columns
	{
		static final String BUCKET_ID = "bucket_id";
		static final String OBJECT_ID = "object_id";
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where " + Columns.BUCKET_ID + " = ? and %s";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.BUCKET_ID + ", %s, " + Columns.OBJECT_ID + ", " + Columns.OBJECT + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT +") values (?, ?, ?, ?, ?, %s) if not exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.OBJECT + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists";
	private static final String READ_ALL_CQL = "select * from %s.%s where " + Columns.BUCKET_ID + " = ?";

	private Session session;
	private String keyspace;
	private Table table;

	public ItableStatementFactory(Session session, String keyspace, Table table)
    {
		super();
		this.session = session;
		this.keyspace = keyspace;
		this.table = table;
    }

	public Table table()
	{
		return table;
	}

	public List<BoundStatement> createIndexEntryCreateStatements(Document document)
    {
		if (!table.hasIndexes()) return Collections.emptyList();

		List<BoundStatement> stmts = new ArrayList<BoundStatement>(table.indexes().size());

		for (Index index : table.indexes())
		{
			BoundStatement stmt = createIndexEntryCreateStatement(document, index);

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
				BoundStatement creStmt = createIndexEntryCreateStatement(document, index);
	
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

	public BoundStatement createIndexEntryCreateStatement(Document document, Index index)
	{
		Map<String, Object> bindings = new LinkedHashMap<String, Object>(index.size());
		boolean isBound = extractBindings(document.object(), index, bindings);

		if (isBound)
		{
			// TODO: cache prepared statements.
			String cql = String.format(CREATE_CQL, keyspace, index.toDbTable(), index.toPkDefs(), getQuestionMarks(index));
			PreparedStatement ps = session.prepare(cql);
			BoundStatement bs = new BoundStatement(ps);
			long bucketId = getBucketId(index, bindings.values());
			bindAll(index, bs, bucketId, document, bindings.values().toArray());
			return bs;
		}

		return null;
	}

	public BoundStatement createIndexEntryUpdateStatment(Document document, Index index)
	{
		return null;
	}

	public BoundStatement createIndexEntryDeleteStatment(Document document, Index index)
	{
		return null;
	}

	private void bindAll(Index index, BoundStatement bs, long bucketId, Document document, Object[] keys)
    {
		int i = 0;
		bs.setLong(i++, bucketId);
		i = bindKeys(bs, i, index.fieldSpecs(), keys);
		bindDocumentId(bs, document.id(), i++);

		if (document.hasObject())
		{
			bs.setBytes(i++, ByteBuffer.wrap(BSON.encode(document.object())));
		}
		else
		{
			i++;
		}

		bs.setDate(i++, document.createdAt());
		bs.setDate(i++, document.updatedAt());
    }

	private void bindDocumentId(BoundStatement bs, Object id, int i)
    {
	    table.idType().bindTo(bs, i, id);
    }

	private int bindKeys(BoundStatement bs, int i, List<IndexField> keySpecs, Object[] keys)
    {
	    int keyIndex = 0;

		for (Object key : keys)
		{
			keySpecs.get(keyIndex++).type().bindTo(bs, i++, key);
		}

	    return i;
    }

	private long getBucketId(Index index, Collection<Object> values)
    {
		// TODO: Determine bucketId.
	    return 0;
    }

	private String getQuestionMarks(Index index)
    {
	    return QUESTION_MARKS.substring(0, (index.size() * 2) - 1);
    }

	private boolean extractBindings(BSONObject bsonObject, Index index, Map<String, Object> bindings)
    {
	    boolean isBound = true;

	    for (IndexField indexKey : index.fieldSpecs())
		{
			Object value = bsonObject.get(indexKey.name());

			if (value != null)
			{
				bindings.put(indexKey.name(), value);
			}
			else
			{
				isBound = false;
			}
		}

	    return isBound;
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
