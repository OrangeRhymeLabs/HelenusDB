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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.document.DocumentRepository.DocumentStatements;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.StorageException;
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
		    // TODO: historical documents
//		    "primary key ((" + Columns.ID + "), " + Columns.UPDATED_AT + ")" +
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

		@Query("select count(*) from %s.%s where " + Columns.ID + " = ? limit 1")
		PreparedStatement exists();

		@Override
		@Query("update %s.%s set " + Columns.OBJECT + " = ?, " + Columns.UPDATED_AT + " = ? where " + Columns.ID + " = ? if exists")
		PreparedStatement update();

		@Query("insert into %s.%s (" + Columns.ID + ", " + Columns.OBJECT + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?)")
		PreparedStatement upsert();

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

	public ListenableFuture<Boolean> exists(Identifier id)
	{
		ListenableFuture<ResultSet> future = submitExists(id);
		return Futures.transform(future, new Function<ResultSet, Boolean>()
		{
			@Override
			public Boolean apply(ResultSet result)
			{
				return result.one().getLong(0) > 0;
			}
		});
	}

	public ListenableFuture<Document> upsert(Document entity)
	{
		ListenableFuture<ResultSet> future = submitUpsert(entity);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(ResultSet result)
			throws Exception
			{
				if (result.wasApplied())
				{
					return Futures.immediateFuture(entity);
				}

				//TODO: This doesn't provide any informational value... what should it be?
				return Futures.immediateFailedFuture(new StorageException(String.format("Table %s failed to store document: %s", table.toDbTable(), entity.toString())));
			}
		});
	}

	@Override
	protected ResultSetFuture submitCreate(Document document)
	{
		BoundStatement create = new BoundStatement(statementFactory().create());
		bindCreate(create, document);
		return session().executeAsync(create);
	}

	@Override
	protected ResultSetFuture submitDelete(Identifier id)
	{
		BoundStatement delete = new BoundStatement(statementFactory().delete());
		bindIdentity(delete, id);
		return session().executeAsync(delete);
	}

	protected ListenableFuture<ResultSet> submitExists(Identifier id)
	{
		BoundStatement bs = new BoundStatement(statementFactory().exists());
		bindIdentity(bs, id);
		return session().executeAsync(bs);
	}

	@Override
	protected ResultSetFuture submitUpdate(Document document)
	{
		BoundStatement update = new BoundStatement(statementFactory().update());
		bindUpdate(update, document);
		return session().executeAsync(update);
	}

	protected ResultSetFuture submitUpsert(Document document)
	{
		BoundStatement upsert = new BoundStatement(statementFactory().upsert());
		bindCreate(upsert, document);
		return session().executeAsync(upsert);
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
