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
package com.orangerhymelabs.helenus.cassandra.document;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.bson.BSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.document.DocumentRepository.DocumentStatements;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenus.persistence.Query;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * Document repositories are unique per document/table and therefore must be cached by table.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DocumentRepository
extends AbstractCassandraRepository<Document, DocumentStatements>
{
	private static final Logger LOG = LoggerFactory.getLogger(DocumentRepository.class);

	private class Columns
	{
		static final String ID = "id";
		static final String OBJECT = "object";
		static final String LUCENE = "lucene";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	public static class Schema
	{
		private static final String DROP_TABLE = "drop table if exists %s.%s;";
		private static final String CREATE_TABLE = "create table if not exists %s.%s" +
		"(" +
			Columns.ID + " %s," +
		    Columns.OBJECT + " blob," +
		    // TODO: Add Location details to Document.
		    Columns.LUCENE + " text," + 			// Empty, but facilitates Lucene indexing / searches.
			Columns.CREATED_AT + " timestamp," +
		    Columns.UPDATED_AT + " timestamp," +
			"primary key (" + Columns.ID + ")" +
		")";

		public boolean drop(Session session, String keyspace, String table)
        {
			ResultSetFuture rs = session.executeAsync(String.format(DROP_TABLE, keyspace, table));
	        try
	        {
				return rs.get().wasApplied();
			}
	        catch (InterruptedException | ExecutionException e)
	        {
	        	LOG.error("Document schema drop failed", e);
			}

	        return false;
        }

        public boolean create(Session session, String keyspace, String table, DataTypes idType)
        {
			ResultSetFuture rs = session.executeAsync(String.format(CREATE_TABLE, keyspace, table, idType.cassandraType()));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Document schema create failed", e);
			}
			
			return false;
        }
	}

	public interface DocumentStatements
	extends StatementFactory
	{
		@Override
		@Query("insert into %s.%s (" + Columns.ID + ", " + Columns.OBJECT + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?) if not exists")
		PreparedStatement create();

		@Override
		@Query("delete from %s.%s where " + Columns.ID + " = ?")
		PreparedStatement delete();

		@Override
		@Query("update %s.%s set " + Columns.OBJECT + " = ?, " + Columns.UPDATED_AT + " = ? where " + Columns.ID + " = ? if exists")
		PreparedStatement update();

		@Override
		@Query("select * from %s.%s where " + Columns.ID + " = ?")
		PreparedStatement read();
	}

	private Table table;

	public DocumentRepository(Session session, String keyspace, Table table)
	{
		super(session, keyspace, table.toDbTable(), DocumentStatements.class);
		this.table = table;
	}

	public String tableName()
	{
		return table.toDbTable();
	}

	@Override
	protected ResultSetFuture submitCreate(Document document)
	{
		BatchStatement batch = new BatchStatement(Type.UNLOGGED);
		BoundStatement create = new BoundStatement(statementFactory().create());
		bindCreate(create, document);
		batch.add(create);

//		if (table.hasIndexes())
//		{
//			batch.addAll(iTableStmtFactory.createIndexEntryCreateStatements(document));
//		}

		return session().executeAsync(batch);
	}

	@Override
	protected ResultSetFuture submitUpdate(Document document)
	{
		BatchStatement batch = new BatchStatement(Type.UNLOGGED);
		BoundStatement update = new BoundStatement(statementFactory().update());
		bindUpdate(update, document);
		batch.add(update);

//		if (table.hasIndexes())
//		{
			// TODO: make this lookup non-blocking (asynchronous).
//			Document previous = read(document.getIdentifier()).get();
//			read(document.getIdentifier());
//			batch.addAll(iTableStmtFactory.createIndexEntryUpdateStatements(document, previous));
//		}

		return session().executeAsync(batch);
	}

	@Override
	protected ResultSetFuture submitDelete(Identifier id)
	{
		BatchStatement batch = new BatchStatement(Type.UNLOGGED);
		BoundStatement delete = new BoundStatement(statementFactory().delete());
		bindIdentity(delete, id);
		batch.add(delete);

//		if (table.hasIndexes())
//		{
			// TODO: make this lookup non-blocking (asynchronous).
//			Document previous = read(id);
//			batch.addAll(iTableStmtFactory.createIndexEntryDeleteStatements(previous));
//		}

		return session().executeAsync(batch);
	}

	@Override
	protected void bindCreate(BoundStatement bs, Document document)
	{
		Date now = new Date();
		document.createdAt(now);
		document.updatedAt(now);

		try
		{
			if (document.hasObject())
			{
				bs.bind(document.id(),
					ByteBuffer.wrap(BSON.encode(document.object())),
				    document.createdAt(),
				    document.updatedAt());
			}
			else
			{
				bs.bind(document.id(),
					null,
				    document.createdAt(),
				    document.updatedAt());
			}
		}
		catch (InvalidTypeException | CodecNotFoundException e)
		{
			throw new InvalidIdentifierException(e);
		}
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Document document)
	{
		document.updatedAt(new Date());

		try
		{
			if (document.hasObject())
			{
				bs.bind(ByteBuffer.wrap(BSON.encode(document.object())),
					document.updatedAt(),
				    document.id());
			}
			else
			{
				bs.bind(null,
					document.updatedAt(),
				    document.id());
			}
		}
		catch (InvalidTypeException | CodecNotFoundException e)
		{
			throw new InvalidIdentifierException(e);
		}
	}

	@Override
	protected Document marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		Document d = new Document();
		d.id(marshalId(row));
		ByteBuffer b = row.getBytes(Columns.OBJECT);

		if (b != null && b.hasArray())
		{
			byte[] result = new byte[b.remaining()];
			b.get(result);
			d.object(BSON.decode(result));
		}

		d.createdAt(row.getTimestamp(Columns.CREATED_AT));
		d.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return d;
	}

	private Object marshalId(Row row)
    {
		switch(table.idType())
		{
			case BIGINT: return row.getLong(Columns.ID);
			case DECIMAL: return row.getDecimal(Columns.ID);
			case DOUBLE: return row.getDouble(Columns.ID);
			case FLOAT: return row.getFloat(Columns.ID);
			case INTEGER: return row.getInt(Columns.ID);
			case TEXT: return row.getString(Columns.ID);
			case TIMESTAMP: return row.getTimestamp(Columns.ID);
			case TIMEUUID:
			case UUID:  return row.getUUID(Columns.ID);
			default: throw new UnsupportedOperationException("Conversion of ID type: " + table.idType().toString());
		}
    }
}
