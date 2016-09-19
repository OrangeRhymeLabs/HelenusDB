package com.orangerhymelabs.helenus.cassandra.view.key;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.bson.BSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mongodb.util.JSON;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.table.key.ClusteringKeyComponent;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyComponent;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinition;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.table.key.ClusteringKeyComponent.Ordering;
import com.orangerhymelabs.helenus.persistence.Identifier;

public class KeyDefinitionTest
{
	private static final BSONObject BSON = (BSONObject) JSON.parse("{'alpha':'some', 'beta':1, 'chi':'excitement', 'delta':3.14159}");

	@Test
	public void shouldHandleSimple()
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.UUID));
		assertTrue(kd.isValid());
		assertEquals("alpha uuid", kd.asColumns());
		assertEquals("primary key (alpha)", kd.asPrimaryKey());
	}

	@Test
	public void shouldHandlePartitionKey()
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.UUID))
			.addPartitionKey(new KeyComponent("beta", DataTypes.TEXT));
		assertTrue(kd.isValid());
		assertEquals("alpha uuid,beta text", kd.asColumns());
		assertEquals("primary key (alpha,beta)", kd.asPrimaryKey());
		assertEquals("", kd.asClusteringKey());
		assertTrue(kd.isValid());
	}

	@Test
	public void shouldHandleComplex()
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.UUID))
			.addPartitionKey(new KeyComponent("beta", DataTypes.TEXT))
			.addClusteringKey(new ClusteringKeyComponent("chi", DataTypes.TIMESTAMP, Ordering.DESC))
			.addClusteringKey(new ClusteringKeyComponent("delta", DataTypes.INTEGER, Ordering.ASC));
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertEquals("with clustering order by (chi DESC,delta ASC)", kd.asClusteringKey());
		assertTrue(kd.isValid());
	}

	@Test
	public void shouldHandleComplexWithDefaultSort()
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.UUID))
			.addPartitionKey(new KeyComponent("beta", DataTypes.TEXT))
			.addClusteringKey(new ClusteringKeyComponent("chi", DataTypes.TIMESTAMP, Ordering.ASC))
			.addClusteringKey(new ClusteringKeyComponent("delta", DataTypes.INTEGER, Ordering.ASC));
		assertEquals("alpha uuid,beta text,chi timestamp,delta int", kd.asColumns());
		assertEquals("primary key ((alpha,beta),chi,delta)", kd.asPrimaryKey());
		assertTrue(kd.isValid());
	}

	@Test
	public void shouldReturnIdentifier()
	throws KeyDefinitionException
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.TEXT))
			.addPartitionKey(new KeyComponent("beta", DataTypes.INTEGER))
			.addClusteringKey(new ClusteringKeyComponent("chi", DataTypes.TEXT, Ordering.ASC))
			.addClusteringKey(new ClusteringKeyComponent("delta", DataTypes.DECIMAL, Ordering.ASC));
		Identifier id = kd.identifier(BSON);
		List<Object> components = id.components();
		assertEquals(4, components.size());
		assertEquals("some", components.get(0));
		assertEquals(1, components.get(1));
		assertEquals("excitement", components.get(2));
		assertEquals(3.14159, components.get(3));
	}

	@Test
	public void shouldReturnSmallIdentifier()
	throws KeyDefinitionException
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.TEXT))
			.addClusteringKey(new ClusteringKeyComponent("delta", DataTypes.DECIMAL, Ordering.ASC));
		Identifier id = kd.identifier(BSON);
		List<Object> components = id.components();
		assertEquals(2, components.size());
		assertEquals("some", components.get(0));
		assertEquals(3.14159, components.get(1));
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowWithMissingClusteringProperty()
	throws KeyDefinitionException
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("alpha", DataTypes.TEXT))
			.addPartitionKey(new KeyComponent("beta", DataTypes.INTEGER))
			.addClusteringKey(new ClusteringKeyComponent("chi", DataTypes.TEXT, Ordering.ASC))
			.addClusteringKey(new ClusteringKeyComponent("not_there", DataTypes.DECIMAL, Ordering.ASC));
		kd.identifier(BSON);
	}

	@Test(expected=KeyDefinitionException.class)
	public void shouldThrowWithMissingPartitionProperty()
	throws KeyDefinitionException
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("not_there", DataTypes.TEXT))
			.addPartitionKey(new KeyComponent("beta", DataTypes.INTEGER))
			.addClusteringKey(new ClusteringKeyComponent("chi", DataTypes.TEXT, Ordering.ASC))
			.addClusteringKey(new ClusteringKeyComponent("delta", DataTypes.DECIMAL, Ordering.ASC));
		kd.identifier(BSON);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void shouldContainMissingPropertiesInMessage()
	throws KeyDefinitionException
	{
		KeyDefinition kd = new KeyDefinition();
		kd.addPartitionKey(new KeyComponent("not_there", DataTypes.TEXT))
			.addPartitionKey(new KeyComponent("beta", DataTypes.INTEGER))
			.addPartitionKey(new KeyComponent("not_there_either", DataTypes.INTEGER))
			.addClusteringKey(new ClusteringKeyComponent("chi", DataTypes.TEXT, Ordering.ASC))
			.addClusteringKey(new ClusteringKeyComponent("delta", DataTypes.DECIMAL, Ordering.ASC));

		thrown.expect(KeyDefinitionException.class);
	    thrown.expectMessage("Missing properties: not_there, not_there_either");
	    kd.identifier(BSON);
	}
}
