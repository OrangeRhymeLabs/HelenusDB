package com.orangerhymelabs.orangedb.cassandra.database;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

public class DatabaseService
{
	private DatabaseRepository databases;
	
	public DatabaseService(DatabaseRepository databaseRepository)
	{
		super();
		this.databases = databaseRepository;
	}

	public Database create(Database database)
	{
		ValidationEngine.validateAndThrow(database);
		return databases.create(database);
	}

	public void createAsync(Database database, FutureCallback<Database> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(database);
			databases.createAsync(database, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public Database read(String name)
	{
		return databases.read(new Identifier(name));		
	}

	public void readAsync(String name, FutureCallback<Database> callback)
	{
		databases.readAsync(new Identifier(name), callback);
	}

	public List<Database> readAll()
	{
		return databases.readAll();
	}

	public void readAllAsync(FutureCallback<List<Database>> callback)
	{
		databases.readAllAsync(callback);
	}

	public Database update(Database database)
	{
		ValidationEngine.validateAndThrow(database);
		return databases.update(database);
	}

	public void updateAsync(Database database, FutureCallback<Database> callback)
    {
		try
		{
			ValidationEngine.validateAndThrow(database);
			databases.updateAsync(database, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
    }

	public void delete(String name)
	{
		databases.delete(new Identifier(name));
	}

	public void deleteAsync(String name, FutureCallback<Database> callback)
    {
		databases.deleteAsync(new Identifier(name), callback);
    }
}
