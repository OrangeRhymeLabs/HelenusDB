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
package com.orangerhymelabs.orangedb.cassandra.document;

import com.orangerhymelabs.orangedb.cassandra.table.TableRepository;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
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
//
//    public Document create(String database, String table, String json)
//    {
//        verifyTable(database, table);
//
//        Document doc = new Document();
//        doc.table(database, table);
//        doc.object(json);
//        ValidationEngine.validateAndThrow(doc);
//        return docs.create(doc);
//    }
//
//    public Document read(String database, String table, Identifier id)
//    {
//        verifyTable(database, table);
//        return docs.read(id);
//    }
//
////	public List<Document> readAll(String database, String table)
////	{
////		verifyTable(database, table);
////		return docs.readAll(database, table);
////	}
////
////	public long countAll(String database, String table)
////	{
////		return docs.countAll(database, table);
////	}
//    
//    public void update(Document entity)
//    {
//        ValidationEngine.validateAndThrow(entity);
//        docs.update(entity);
//    }
//
//    public void delete(String database, String table, Identifier id)
//    {
//        verifyTable(database, table);
//        docs.delete(id);
//    }
//
//    private void verifyTable(String database, String table)
//    {
//        Identifier tableId = new Identifier(database, table);
//
//        if (!tables.exists(tableId))
//        {
//            throw new ItemNotFoundException("Table not found: " + tableId.toString());
//        }
//    }
}
