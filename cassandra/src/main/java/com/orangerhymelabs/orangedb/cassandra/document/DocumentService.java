package com.orangerhymelabs.orangedb.cassandra.document;

import com.orangerhymelabs.orangedb.cassandra.table.TableRepository;
import com.strategicgains.repoexpress.domain.Identifier;
import com.strategicgains.repoexpress.exception.ItemNotFoundException;
import com.strategicgains.syntaxe.ValidationEngine;

public class DocumentService
{
    private TableRepository tables;
    private DocumentRepository docs;

    public DocumentService(TableRepository databaseRepository, DocumentRepository documentRepository)
    {
        super();
        this.docs = documentRepository;
        this.tables = databaseRepository;
    }

    public Document create(String database, String table, String json)
    {
        verifyTable(database, table);

        Document doc = new Document();
        doc.table(database, table);
        doc.object(json);
        ValidationEngine.validateAndThrow(doc);
        return docs.create(doc);
    }

    public Document read(String database, String table, Identifier id)
    {
        verifyTable(database, table);
        return docs.read(id);
    }

//	public List<Document> readAll(String database, String table)
//	{
//		verifyTable(database, table);
//		return docs.readAll(database, table);
//	}
//
//	public long countAll(String database, String table)
//	{
//		return docs.countAll(database, table);
//	}
    
    public void update(Document entity)
    {
        ValidationEngine.validateAndThrow(entity);
        docs.update(entity);
    }

    public void delete(String database, String table, Identifier id)
    {
        verifyTable(database, table);
        docs.delete(id);
    }

    private void verifyTable(String database, String table)
    {
        Identifier tableId = new Identifier(database, table);

        if (!tables.exists(tableId))
        {
            throw new ItemNotFoundException("Table not found: " + tableId.toString());
        }
    }
}
