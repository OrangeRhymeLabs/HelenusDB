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

import com.orangerhymelabs.helenusdb.cassandra.database.Database;
import com.orangerhymelabs.helenusdb.cassandra.table.Table;
import com.orangerhymelabs.helenusdb.rest.cassandra.testhelper.Fixtures;
import com.orangerhymelabs.helenusdb.rest.testhelper.RestExpressManager;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author udeyoje
 */
public class TableControllerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger(TableControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;

    public TableControllerTest() throws Exception
    {
        f = Fixtures.getInstance();
    }

    /**
     * Initialization that is performed once before any of the tests in this
     * class are executed.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception
    {
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        //RestAssured.basePath = "/courses/" + COURSE_ID + "/categories";

//        String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
//        String[] env = {testEnv};
        //Thread.sleep(10000);
        f = Fixtures.getInstance();
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest()
    {
        f.clearTestTables();
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        RestAssured.basePath = "/" + testDb.name();
    }

    /**
     * Cleanup that is performed once after all of the tests in this class are
     * executed.
     */
    @AfterClass
    public static void afterClass()
    {
    }

    /**
     * Cleanup that is performed after each test is executed.
     */
    @After
    public void afterTest()
    {
        f.clearTestTables();
    }

    /**
     * Tests that the GET /{databases}/{table} properly retrieves an existing
     * table.
     */
    @Test
    public void getTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        expect().statusCode(200)
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(testTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testTable.name());
    }

    /**
     * Tests that the POST /{databases}/{table} endpoint properly creates a
     * table.
     */
    @Test
    public void postTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        String tableStr = "{" + "\"description\" : \"" + testTable.description()
                + "\"," + "\"name\" : \"" + testTable.name() + "\"}";
        //act
        given().body(tableStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(testTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(testTable.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(testTable.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testTable.name());
    }

    /**
     * Tests that the PUT /{databases}/{table} endpoint properly updates a
     * table.
     */
    @Test
    public void putTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        String newDesciption = "this is a new description";
        String tableStr = "{" + "\"description\" : \"" + newDesciption
                + "\"," + "\"name\" : \"" + testTable.name() + "\"}";

        //act
        given().body(tableStr).expect().statusCode(204)
                .when().put(testTable.name());

        //check
        expect().statusCode(200)
                .body("name", equalTo(testTable.name()))
                .body("description", equalTo(newDesciption))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get(testTable.name());
    }

    /**
     * Tests that the DELETE /{databases}/{table} endpoint properly deletes a
     * table.
     */
    @Test
    public void deleteTableTest()
    {
        Table testTable = Fixtures.createTestTable();
        f.insertTable(testTable);
        //act
        given().expect().statusCode(204)
                .when().delete(testTable.name());
        //check
        expect().statusCode(404).when()
                .get(testTable.name());
    }
}
