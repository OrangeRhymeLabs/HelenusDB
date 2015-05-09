package com.orangerhymelabs.orangedb.cassandra.document;

import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BSON;
import org.bson.BSONObject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mongodb.util.JSON;
import com.orangerhymelabs.orangedb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.orangedb.cassandra.event.EventFactory;
import com.orangerhymelabs.orangedb.cassandra.event.StateChangeEventingObserver;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;

public class DocumentRepository
extends AbstractCassandraRepository<Document>
{
	private class Schema
	{

		static final String CREATE_TABLE = "create table %s"
		    + " (id uuid, object blob, created_at timestamp, updated_at timestamp,"
		    + " primary key (id))";// + " primary key ((id), updated_at))"
		// + " with clustering order by (updated_at DESC);";
		static final String DROP_TABLE = "drop table if exists %s;";
	}

	private class Columns
	{

		static final String ID = "id";
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String EXISTENCE_CQL = "select count(*) from %s where %s = ?";
	private static final String READ_CQL = "select * from %s where %s = ?";
	private static final String DELETE_CQL = "delete from %s where %s = ?";
	private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where %s = ?";
	private static final String CREATE_CQL = "insert into %s (%s, object, created_at, updated_at) values (?, ?, ?, ?)";

	public DocumentRepository(Session session, String keyspace)
	{
		super(session, keyspace);
		addObserver(new DocumentObserver());
		addObserver(new StateChangeEventingObserver<Document>(
		    new DocumentEventFactory()));
	}

	@Override
	public boolean dropSchema()
	{
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean createSchema()
	{
		// TODO Auto-generated method stub
		return false;
	}

	public boolean exists(Identifier identifier)
	{
		if (identifier == null || identifier.isEmpty())
		{
			return false;
		}

		Table table = extractTable(identifier);
		Identifier id = extractId(identifier);
		PreparedStatement existStmt = session().prepare(
		        String.format(EXISTENCE_CQL, table.toDbTable(), Columns.ID));

		BoundStatement bs = new BoundStatement(existStmt);
		bindIdentity(bs, id);
		return (session().execute(bs).one().getLong(0) > 0);
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

	private Identifier extractId(Identifier identifier)
	{
		// This includes the date/version on the end...
		// List<Object> l = identifier.components().subList(2, 4);

		// TODO: determine what to do with version here.
		List<Object> l = identifier.components().subList(2, 3);
		return new Identifier(l.toArray());
	}

	private Table extractTable(Identifier identifier)
	{
		Table t = new Table();
		List<Object> l = identifier.components().subList(0, 2);// NOTE/TODO:
															   // frequent
															   // IndexOutOfBounds
															   // here
		t.database((String) l.get(0));
		t.name((String) l.get(1));
		return t;
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
	implements EventFactory<Document>
	{

		@Override
		public Object newCreatedEvent(Document object)
		{
			return new DocumentCreatedEvent(object);
		}

		@Override
		public Object newUpdatedEvent(Document object)
		{
			return new DocumentUpdatedEvent(object);
		}

		@Override
		public Object newDeletedEvent(Document object)
		{
			return new DocumentDeletedEvent(object);
		}
	}

	@Override
    protected String buildCreateStatement()
    {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    protected String buildUpdateStatement()
    {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    protected String buildReadStatement()
    {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    protected String buildReadAllStatement()
    {
	    // TODO Auto-generated method stub
	    return null;
    }

	@Override
    protected String buildDeleteStatement()
    {
	    // TODO Auto-generated method stub
	    return null;
    }
}
