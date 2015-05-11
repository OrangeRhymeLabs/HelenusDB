package com.orangerhymelabs.orangedb.cassandra.table;

import java.util.List;

import com.orangerhymelabs.orangedb.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;
import com.strategicgains.syntaxe.ValidationEngine;

public class TableService
{
	private TableRepository tables;
	private DatabaseRepository databases;
	
	public TableService(DatabaseRepository databaseRepository, TableRepository tableRepository)
	{
		super();
		this.databases = databaseRepository;
		this.tables = tableRepository;
	}

	public void create(Table entity, ResultCallback<Table> callback)
	{
		if (!databases.exists(entity.database().getId()))
		{
			throw new ItemNotFoundException("Database not found: " + entity.database());
		}

		ValidationEngine.validateAndThrow(entity);
		tables.createAsync(entity, callback);
	}

	public void read(String database, String table, ResultCallback<Table> callback)
	{
		tables.readAsync(new Identifier(database, table), callback);
	}

	public void readAll(String database, ResultCallback<List<Table>> callback)
	{
		if (!databases.exists(new Identifier(database))) throw new ItemNotFoundException("Database not found: " + database);

		tables.readAllAsync(callback, database);
	}

	public void update(Table entity, ResultCallback<Table> callback)
    {
		ValidationEngine.validateAndThrow(entity);
		tables.updateAsync(entity, callback);
    }

	public void delete(Identifier id, ResultCallback<Table> callback)
    {
		tables.deleteAsync(id, callback);
    }
}
