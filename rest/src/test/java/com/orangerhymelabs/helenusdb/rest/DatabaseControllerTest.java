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
public class DatabaseControllerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseControllerTest.class);
    private static final String BASE_URI = "http://localhost";
    private static final int PORT = 19080;
    private static Fixtures f;

    public DatabaseControllerTest() throws Exception{
        
    }

    /**
     * Initialization that is performed once before any of the tests in this
     * class are executed.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        RestAssured.baseURI = BASE_URI;
        RestAssured.port = PORT;
        RestAssured.basePath = "/";

//        String testEnv = System.getProperty("TEST_ENV") != null ? System.getProperty("TEST_ENV") : "local";
//        String[] env = {testEnv};
        LOGGER.debug("Loading RestExpress Environment... ");
        //Thread.sleep(10000);
        f = Fixtures.getInstance();
        RestExpressManager.getManager().ensureRestExpressRunning();
    }

    @Before
    public void beforeTest() {
        f.clearTestTables();
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

//    /**
//     * Tests that the GET / properly retrieves all existing database
//     */
//    @Test
//    public void getDatabasesTest() {
//        Database testDb = Fixtures.createTestDatabase();
//        f.insertDatabase(testDb);
//        //not the best test
//        expect().statusCode(200)
//                .body("", contains(testDb.name()))
//                .body("", contains(testDb.description())).when()
//                .get("/");
//    }
    /**
     * Tests that the GET /{databases} properly retrieves an existing database.
     */
    @Test
    public void getDatabaseTest() {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.getId());
    }

    /**
     * Tests that the POST /{databases} endpoint properly creates a database.
     */
    @Test
    public void postDatabaseTest() {
        Database testDb = Fixtures.createTestDatabase();
        String dbStr = "{" + "\"description\" : \"" + testDb.description()
                + "\"," + "\"name\" : \"" + testDb.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(201)
                //.header("Location", startsWith(RestAssured.basePath + "/"))
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue())
                .when().post(testDb.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(testDb.description()))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.getId());
    }

    /**
     * Tests that the PUT /{databases} endpoint properly updates a database.
     */
    @Test
    public void putDatabaseTest() {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        String newDesciption = "this is a new description";
        String dbStr = "{" + "\"description\" : \"" + newDesciption
                + "\"," + "\"name\" : \"" + testDb.name() + "\"}";
        //act
        given().body(dbStr).expect().statusCode(204)
                .when().put(testDb.name());
        //check
        expect().statusCode(200)
                .body("name", equalTo(testDb.name()))
                .body("description", equalTo(newDesciption))
                .body("createdAt", notNullValue())
                .body("updatedAt", notNullValue()).when()
                .get("/" + testDb.getId());

    }

    /**
     * Tests that the DELETE /{databases} endpoint properly deletes a database.
     */
    @Test
    public void deleteDatabaseTest() {
        Database testDb = Fixtures.createTestDatabase();
        f.insertDatabase(testDb);
        //act
        given().expect().statusCode(204)
                .when().delete(testDb.name());
        //check
        expect().statusCode(404).when()
                .get(testDb.name());
    }
}
