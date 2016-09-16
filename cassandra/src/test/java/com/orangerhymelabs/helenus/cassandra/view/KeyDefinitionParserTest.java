package com.orangerhymelabs.helenus.cassandra.view;

import static org.junit.Assert.*;

import org.junit.Test;

import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinition;
import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinitionParser;

public class KeyDefinitionParserTest
{
	private KeyDefinitionParser parser = new KeyDefinitionParser();

	@Test
	public void shouldParseSimple()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("alpha:uuid");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid", kd.asColumns());
		assertEquals("primary key (alpha)", kd.asPrimaryKey());
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowWithSortOnPartitionKey()
	throws KeyDefinitionException
	{
		parser.parse("-alpha:uuid");
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowWithInvalidPartitionKey()
	throws KeyDefinitionException
	{
		parser.parse("9alpha:uuid");
	}

	@Test
	public void shouldParsePartitionKey()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("alpha:uuid, beta:text");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text", kd.asColumns());
		assertEquals("primary key (alpha,beta)", kd.asPrimaryKey());
		assertEquals("", kd.asClusteringKey());
	}

	@Test
	public void shouldParseComplex()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("((alpha:uuid, beta:text), -chi:timestamp, +delta:int)");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("with clustering order by (chi DESC,delta ASC)", kd.asClusteringKey());
	}

	@Test
	public void shouldParseComplexWithOptionalParens()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("(alpha:uuid, beta:text), -chi:timestamp, +delta:int");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("with clustering order by (chi DESC,delta ASC)", kd.asClusteringKey());
	}

	@Test
	public void shouldParseComplexWithoutOptionalSort()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("(alpha:uuid, beta:text), chi:timestamp, delta:int");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("", kd.asClusteringKey());
	}

	@Test
	public void shouldParseComplexWithoutOptionalComma()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("((alpha:uuid, beta:text) -chi:timestamp, +delta:int)");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("with clustering order by (chi DESC,delta ASC)", kd.asClusteringKey());
	}

	@Test
	public void shouldParseComplexWithoutOptionalCommaAndParens()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("(alpha:uuid, beta:text) -chi:timestamp, +delta:int");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("with clustering order by (chi DESC,delta ASC)", kd.asClusteringKey());
	}

	@Test
	public void shouldTollerateParenDepth()
	throws KeyDefinitionException
	{
		KeyDefinition kd = parser.parse("(((alpha:uuid, beta:text)), -chi:timestamp, +delta:int)");
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("with clustering order by (chi DESC,delta ASC)", kd.asClusteringKey());
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowOnUnbalancedParens()
	throws KeyDefinitionException
	{
		parser.parse("((alpha:uuid, beta:text), -chi:timestamp, +delta:int");
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowOnInvalidChar()
	throws KeyDefinitionException
	{
		parser.parse("((alpha:uuid, beta:text), %chi:timestamp, +delta:int)");
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowOnInvalidSort()
	throws KeyDefinitionException
	{
		parser.parse("((alpha:uuid, beta:text), 9chi:timestamp, +delta:int)");
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowOnInvalidPartitionKeyType()
	throws KeyDefinitionException
	{
		parser.parse("((alpha:uuid, beta:data), -chi:timestamp, +delta:int)");
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowOnInvalidClusterKeyType()
	throws KeyDefinitionException
	{
		parser.parse("((alpha:uuid, beta:text), -chi:data, +delta:int)");
	}
}
