package com.orangerhymelabs.orangedb.cassandra.document;

import java.nio.ByteBuffer;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.orangerhymelabs.orangedb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.orangedb.cassandra.event.EventFactory;
import com.orangerhymelabs.orangedb.cassandra.event.StateChangeEventingObserver;
import com.orangerhymelabs.orangedb.persistence.Identifier;

public class DocumentRepository
extends AbstractCassandraRepository<Document>
{
	public static class Schema
	{
		private static final String DROP_TABLE = "drop table if exists %s.%s;";
		private static final String CREATE_TABLE = "create table %s.%s" +
		"(" +
			"id uuid," +
		    "object blob," +
			"created_at timestamp," +
		    "updated_at timestamp," +
			"primary key (id))";
		// + "primary key ((id), updated_at))"
		// + ") with clustering order by (updated_at DESC);";

        public boolean drop(Session session, String keyspace, String table)
        {
			ResultSet rs = session.execute(String.format(DROP_TABLE, keyspace, table));
	        return rs.wasApplied();
        }

        public boolean create(Session session, String keyspace, String table)
        {
			ResultSet rs = session.execute(String.format(CREATE_TABLE, keyspace, table));
	        return rs.wasApplied();
        }
	}

	private class Columns
	{

		static final String ID = "id";
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String READ_CQL = "select * from %s.%s where id = ?";
	private static final String DELETE_CQL = "delete from %s.%s where id = ?";
	private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where id = ? if exists";
	private static final String CREATE_CQL = "insert into %s.%s (id, object, created_at, updated_at) values (?, ?, ?, ?) if not exists";

	private String table;

	public DocumentRepository(Session session, String keyspace, String table)
	{
		super(session, keyspace);
		this.table = table;
		addObserver(new StateChangeEventingObserver(new DocumentEventFactory()));
	}

	@Override
	protected void bindCreate(BoundStatement bs, Document entity)
	{
		BSONObject bson = (BSONObject) JSON.parse(entity.object());
		bs.bind(entity.getUuid(), ByteBuffer.wrap(BSON.encode(bson)),
		    entity.createdAt(), entity.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Document entity)
	{
		BSONObject bson = (BSONObject) JSON.parse(entity.object());

		bs.bind(ByteBuffer.wrap(BSON.encode(bson)), entity.updatedAt(),
		    entity.getUuid());
	}

	@Override
	protected Document marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		Document d = new Document();
		d.setUuid(row.getUUID(Columns.ID));
		ByteBuffer b = row.getBytes(Columns.OBJECT);

		if (b != null && b.hasArray())
		{
			byte[] result = new byte[b.remaining()];
			b.get(result);
			BSONObject o = BSON.decode(result);
			d.object(JSON.serialize(o));
		}

		d.createdAt(row.getDate(Columns.CREATED_AT));
		d.updatedAt(row.getDate(Columns.UPDATED_AT));
		return d;
	}

	private class DocumentEventFactory
	implements EventFactory
	{

		@Override
		public Object newCreatedEvent(Object object)
		{
			return new DocumentCreatedEvent((Document) object);
		}

		@Override
		public Object newUpdatedEvent(Object object)
		{
			return new DocumentUpdatedEvent((Document) object);
		}

		@Override
		public Object newDeletedEvent(Object object)
		{
			return new DocumentDeletedEvent((Identifier) object);
		}
	}

	@Override
    protected String buildCreateStatement()
    {
	    return String.format(CREATE_CQL, keyspace(), table);
    }

	@Override
    protected String buildUpdateStatement()
    {
	    return String.format(UPDATE_CQL, keyspace(), table);
    }

	@Override
    protected String buildReadStatement()
    {
	    return String.format(READ_CQL, keyspace(), table);
    }

	@Override
    protected String buildReadAllStatement()
    {
	    return null;
    }

	@Override
    protected String buildDeleteStatement()
    {
	    return String.format(DELETE_CQL, keyspace(), table);
    }
}
