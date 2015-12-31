package com.orangerhymelabs.helenusdb.rest;

/*
 * Copyright 2015 udeyoje.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.jayway.restassured.RestAssured;

import static com.jayway.restassured.RestAssured.expect;
import static com.jayway.restassured.RestAssured.given;

import com.jayway.restassured.response.Response;
import com.mongodb.util.JSON;
import com.orangerhymelabs.helenusdb.cassandra.database.Database;
import com.orangerhymelabs.helenusdb.cassandra.document.Document;
import com.orangerhymelabs.helenusdb.cassandra.table.Table;
import com.orangerhymelabs.helenusdb.rest.cassandra.testhelper.Fixtures;
import com.orangerhymelabs.helenusdb.rest.testhelper.RestExpressManager;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import org.bson.BSONObject;

import static org.hamcrest.Matchers.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author udeyoje
 */
@Ignore //cassandra-unit does not like us pulling prepared statements from the cache for this test -- TODO: Why?
public class DocumentControllerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;

    public DocumentControllerTest() throws Exception{
        
    }

    /**
     * Initialization that is performed once before any of the tests in this
     * class are executed.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        f = Fixtures.getInstance();
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest() {
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        RestAssured.basePath = "/" + testDb.name() + "/" + testTable.name();
    }

    /**
     * Cleanup that is performed once after all of the tests in this class are
     * executed.
     */
    @AfterClass
    public static void afterClass() {
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest() {
        f.clearTestTables();
    }

    /**
     * Tests that the GET /{databases}/{table}/{document} properly retrieves an existing
     * document.
     */
    @Test
    public void getDocumentTest() {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        expect().statusCode(200)
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object", containsString("greeting"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString());
    }

    /**
     * Tests that the POST /{databases}/{table}/ endpoint properly creates a
     * document.
     */
    @Test
    public void postDocumentTest() {
        Document testDocument = Fixtures.createTestDocument();
        String tableStr = testDocument.object();

        //act
        Response r = given().body(tableStr).expect().statusCode(201)
                .body("id", notNullValue())
                .body("object", notNullValue())
                .body("object", containsString("greeting"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post("/").andReturn();

        BSONObject bson = (BSONObject) JSON.parse(r.getBody().asString());
        String id = (String) bson.get("id");
        //check
        expect().statusCode(200)
                .body("id", equalTo(id))
                .body("object", notNullValue())
                .body("object", containsString("greeting"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .get(id);
        testDocument.setUuid(UUID.fromString(id));
        //cleanup the random uuid'ed doc
        f.deleteDocument(testDocument);
    }

    /**
     * Tests that the PUT /{databases}/{table}/{document} endpoint properly updates a
     * document.
     */
    @Test
    public void putDocumentTest() {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        String newObject = "{\"newjson\": \"object\"}";
        //act
        given().body(newObject).expect().statusCode(204)
                .when().put(testDocument.getUuid().toString());
        
        //check

        Response response = expect().statusCode(200)
                .body("id", equalTo(testDocument.getUuid().toString()))
                .body("object", notNullValue())
                .body("object", equalTo("{ \"newjson\" : \"object\"}"))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testDocument.getUuid().toString()).andReturn();
                LOGGER.debug("body for put response: " + response.getBody().prettyPrint());
    }

    /**
     * Tests that the DELETE /{databases}/{table}/{document} endpoint properly deletes a
     * document.
     */
    @Test
    public void deleteDocumentTest() {
        Document testDocument = Fixtures.createTestDocument();
        f.insertDocument(testDocument);
        //act
        given().expect().statusCode(204)
                .when().delete(testDocument.getUuid().toString());
        //check
        expect().statusCode(404).when()
                .get(testDocument.getUuid().toString());
    }
}
