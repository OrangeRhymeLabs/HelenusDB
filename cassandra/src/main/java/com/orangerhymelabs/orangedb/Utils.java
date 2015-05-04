package com.orangerhymelabs.orangedb;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

public class Utils {
	 private static Logger logger = LoggerFactory.getLogger(Utils.class);
	 
	/**
     * Creates the database based off of a passed in CQL file. WARNING: Be
     * careful, this could erase data if you are not cautious. Ignores comment
     * lines (lines that start with "//").
     *
     * @param cqlPath path to the CQl file you wish to use to init the database.
     * @param session Database session
     *
     * @throws IOException if it can't read from the CQL file for some reason.
     */
    public static void initDatabase(String cqlPath, Session session) throws IOException
    {
        logger.warn("Initing database from CQL file: " + cqlPath);
        InputStream cqlStream = Utils.class.getResourceAsStream(cqlPath);
        String cql = IOUtils.toString(cqlStream);
        String[] statements = cql.split("\\Q;\\E");
        for (String statement : statements)
        {
            statement = statement.trim();
            statement = statement.replaceAll("\\Q\n\\E", " ");
            if (!statement.equals("") && !statement.startsWith("//"))//don't count comments
            {
                logger.info("Executing CQL statement: " + statement);
                session.execute(statement);
            }
        }
    }
}
