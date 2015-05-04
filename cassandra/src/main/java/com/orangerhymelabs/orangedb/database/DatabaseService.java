package com.orangerhymelabs.orangedb.database;

import java.util.List;

import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;

public class DatabaseService
{
	private DatabaseRepository databases;
	
	public DatabaseService(DatabaseRepository databaseRepository)
	{
		super();
		this.databases = databaseRepository;
	}

	public Database create(Database entity)
	{
		ValidationEngine.validateAndThrow(entity);
		return databases.create(entity);
	}

	public Database read(String name)
	{
		Database n = databases.read(new Identifier(name));
		
		if (n == null) throw new ItemNotFoundException("Database not found: " + name);

		return n;
	}

	public List<Database> readAll()
	{
		return databases.readAll();
	}

	public void update(Database entity)
    {
		ValidationEngine.validateAndThrow(entity);
		databases.update(entity);
    }

	public void delete(String name)
    {
		databases.delete(new Identifier(name));
    }
}
