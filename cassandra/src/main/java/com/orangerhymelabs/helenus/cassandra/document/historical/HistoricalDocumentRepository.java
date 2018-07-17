/*
    Copyright 2016, Strategic Gains, Inc.

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
package com.orangerhymelabs.helenus.cassandra.document.historical;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

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
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.document.IdPropertyConverter;
import com.orangerhymelabs.helenus.cassandra.document.historical.HistoricalDocumentRepository.HistoricalDocumentStatements;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyComponent;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinition;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionParser;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * Historical documents are never 'created' or 'updated' in that it's always an upsert. They are also never deleted.
 * Only marked as deleted.
 * 
 * @author tfredrich
 * @since 7 Oct 2016
 */
public class HistoricalDocumentRepository
extends AbstractCassandraRepository<HistoricalDocument, HistoricalDocumentStatements>
{
	private static final Logger LOG = LoggerFactory.getLogger(HistoricalDocumentRepository.class);

	private class Columns
	{
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
		static final String DELETED_AT = "deleted_at";
		static final String IS_DELETED = "is_deleted";
	}

	public static class Schema
	{
		private static final String DROP_TABLE = "drop table if exists %s.%s;";
		private static final String CREATE_TABLE = "create table if not exists %s.%s" +
		"(" +
			"%s," +									// identifying properties
		    Columns.OBJECT + " blob," +
		    // TODO: Add Location details to Document.
		    Columns.IS_DELETED + " boolean" +
			Columns.CREATED_AT + " timestamp," +
		    Columns.UPDATED_AT + " timestamp," +
		    Columns.DELETED_AT + " timestamp," +
			"%s" +									// primary key
		")" +
		" %s";

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

        public boolean create(Session session, String keyspace, String table, KeyDefinition key)
        {
			ResultSetFuture rs = session.executeAsync(String.format(CREATE_TABLE, keyspace, table, key.asColumns(), key.asPrimaryKey(), key.asClusteringKey()));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("ViewDocument schema create failed", e);
			}
			
			return false;
        }
	}

	public static class HistoricalDocumentStatements
	implements StatementFactory
	{
		private static final String DELETE = "delete";
		private static final String EXISTS = "exists";
		private static final String READ = "read";
		private static final String READ_HISTORY = "readHistory";
		private static final String UPSERT = "upsert";

		private KeyDefinition keys;
		private Session session;
		private String keyspace;
		private String tableName;
		private Map<String, PreparedStatement> statements = new ConcurrentHashMap<>();

		public HistoricalDocumentStatements(Session session, String keyspace, String tableName, KeyDefinition keys)
		throws KeyDefinitionException
		{
			super();
			this.session = session;
			this.keyspace = keyspace;
			this.tableName = tableName;
			this.keys = keys;
		}

		@Override
		public PreparedStatement delete()
		{
			PreparedStatement ps = statements.get(DELETE);

			if (ps == null)
			{
				try
				{
					ps = session.prepareAsync(String.format("update %s.%s where %s",
						keyspace,
						tableName,
						keys.asIdentityClause())).get();
					statements.put(DELETE, ps);
				}
				catch (InterruptedException | ExecutionException e)
				{
					LOG.error("Error preparing delete() statement", e);
				}
			}

			return ps;
		}

		public PreparedStatement exists()
		{
			PreparedStatement ps = statements.get(EXISTS);

			if (ps == null)
			{
				try
				{
					ps = session.prepareAsync(String.format("select count(*) from %s.%s where %s limit 1",
						keyspace,
						tableName,
						keys.asIdentityClause())).get();
					statements.put(EXISTS, ps);
				}
				catch (InterruptedException | ExecutionException e)
				{
					LOG.error("Error preparing exists() statement", e);
				}
			}

			return ps;
		}

		public PreparedStatement upsert()
		{
			PreparedStatement ps = statements.get(UPSERT);

			if (ps == null)
			{
				try
				{
					ps = session.prepareAsync(String.format("insert into %s.%s (%s, %s, %s, %s) values (%s)",
						keyspace,
						tableName,
						keys.asSelectProperties(),
						Columns.OBJECT,
						Columns.CREATED_AT,
						Columns.UPDATED_AT,
						keys.asQuestionMarks(3))).get();
					statements.put(UPSERT, ps);
				}
				catch (InterruptedException | ExecutionException e)
				{
					LOG.error("Error preparing upsert() statement", e);
				}
			}

			return ps;
		}

		@Override
		public PreparedStatement read()
		{
			PreparedStatement ps = statements.get(READ);

			if (ps == null)
			{
				try
				{
					ps = session.prepareAsync(String.format("select * from %s.%s where %s limit 1",
						keyspace,
						tableName,
						keys.asIdentityClause())).get();
					statements.put(READ, ps);
				}
				catch (InterruptedException | ExecutionException e)
				{
					LOG.error("Error preparing read() statement", e);
				}
			}

			return ps;
		}

		@Override
		public PreparedStatement readAll()
		{
			PreparedStatement ps = statements.get(READ_HISTORY);

			if (ps == null)
			{
				try
				{
					ps = session.prepareAsync(String.format("select * from %s.%s where %s",
						keyspace,
						tableName,
						keys.asPartitionIdentityClause())).get();
					statements.put(READ_HISTORY, ps);
				}
				catch (InterruptedException | ExecutionException e)
				{
					LOG.error("Error preparing readHistory() statement", e);
				}
			}

			return ps;
		}

		@Override
		public PreparedStatement create()
		{
			throw new UnsupportedOperationException("Historical documents don't support create(). Use upsert().");
		}

		@Override
		public PreparedStatement update()
		{
			throw new UnsupportedOperationException("Historical documents don't support update(). Use upsert().");
		}
	}

	private Table table;
	private KeyDefinition keys;

	public HistoricalDocumentRepository(Session session, String keyspace, Table table)
	throws KeyDefinitionException
	{
		super(session, keyspace);
		this.table = table;
		this.keys = new KeyDefinitionParser().parse(table.keys());
		statementFactory(new HistoricalDocumentStatements(session, keyspace, tableName(), keys));

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
		}, MoreExecutors.directExecutor());
	}

	public ListenableFuture<HistoricalDocument> upsert(HistoricalDocument entity)
	{
		ListenableFuture<ResultSet> future = submitUpsert(entity);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, HistoricalDocument>()
		{
			@Override
			public ListenableFuture<HistoricalDocument> apply(ResultSet result)
			throws Exception
			{
				if (result.wasApplied())
				{
					return Futures.immediateFuture(entity);
				}

				//TODO: This doesn't provide any informational value... what should it be?
				return Futures.immediateFailedFuture(new StorageException(String.format("Table %s failed to store document: %s", table.toDbTable(), entity.toString())));
			}
		}, MoreExecutors.directExecutor());
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

	protected ResultSetFuture submitUpsert(HistoricalDocument document)
	{
		BoundStatement upsert = new BoundStatement(statementFactory().upsert());
		bindUpsert(upsert, document);
		return session().executeAsync(upsert);
	}

	protected void bindUpsert(BoundStatement bs, HistoricalDocument document)
	{
		Date now = new Date();
		document.createdAt(now);
		document.updatedAt(now);
		Identifier id = document.identifier();
		Object[] values = new Object[id.size() + 3];

		try
		{
			if (document.hasObject())
			{
				bind(values, 0, id.components().toArray());
				bind(values, id.size(),
						ByteBuffer.wrap(BSON.encode(document.object())),
					    document.createdAt(),
					    document.updatedAt());
			}
			else
			{
				bind(values, 0, id.components().toArray());
				bind(values, id.size(),
					null,
					document.createdAt(),
					document.updatedAt());
			}

			bs.bind(values);
		}
		catch (InvalidTypeException | CodecNotFoundException e)
		{
			throw new InvalidIdentifierException(e);
		}
	}

	private void bind(Object[] array, int offset, Object... values)
	{
		for (int i = offset; i < values.length + offset; i++)
		{
			array[i] = values[i - offset];
		}
	}

	@Override
	protected HistoricalDocument marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		HistoricalDocument d = new HistoricalDocument();
		d.identifier(marshalId(keys, row));
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

	private Identifier marshalId(KeyDefinition keyDefinition, Row row)
	{
		Identifier id = new Identifier();
		keyDefinition.components().forEach(new Consumer<KeyComponent>()
		{
			@Override
			public void accept(KeyComponent t)
			{
				id.add(IdPropertyConverter.marshal(t.property(), t.type(), row));
			}
		});
		return id;
	}

	@Override
	protected void bindCreate(BoundStatement bs, HistoricalDocument entity)
	{
		throw new UnsupportedOperationException("Historical documents don't support create(). Use upsert().");
	}

	@Override
	protected void bindUpdate(BoundStatement bs, HistoricalDocument entity)
	{
		throw new UnsupportedOperationException("Historical documents don't support update(). Use upsert().");
	}
}
