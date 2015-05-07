package com.orangerhymelabs.orangedb.cassandra.database;

import java.util.List;

import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;
import com.strategicgains.syntaxe.ValidationEngine;

public class DatabaseService
{
	private DatabaseRepository databases;
	
	public DatabaseService(DatabaseRepository databaseRepository)
	{
		super();
		this.databases = databaseRepository;
	}

	public void create(Database entity, ResultCallback<Database> callback)
	{
		ValidationEngine.validateAndThrow(entity);
		databases.create(entity, callback);
	}

	public void read(String name, ResultCallback<Database> callback)
	{
		databases.read(new Identifier(name), callback);
	}

	public void readAll(ResultCallback<List<Database>> callback)
	{
		databases.readAll(callback);
	}

	public void update(Database entity, ResultCallback<Database> callback)
    {
		ValidationEngine.validateAndThrow(entity);
		databases.update(entity, callback);
    }

	public void delete(String name, ResultCallback<Database> callback)
    {
		databases.delete(new Identifier(name), callback);
    }
}
