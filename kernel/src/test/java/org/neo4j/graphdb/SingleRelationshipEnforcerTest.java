package org.neo4j.graphdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.ImpermanentDatabaseRule;

/**
 * This test does not include any
 */
public class SingleRelationshipEnforcerTest
{
    enum Type implements RelationshipType
    {
        KNOWS
    }

    @Rule
    public ImpermanentDatabaseRule database = new ImpermanentDatabaseRule();

    @Test
    public void shouldCreateRelationshipBetweenTwoNodes() throws Exception
    {
        GraphDatabaseService service = database.getGraphDatabaseService();

        Node node1;
        Node node2;
        Transaction transaction = service.beginTx();
        try
        {
            node1 = service.createNode();
            node2 = service.createNode();

            new SingleRelationshipEnforcer( transaction ).relate( node1, node2, Type.KNOWS );

            transaction.success();
        }
        finally
        {
            transaction.finish();
        }

        assertExactlyOneRelationship( node1, node2 );
    }

    @Test
    public void shouldDoNothingWhenRelationshipAlreadyExists() throws Exception
    {
        GraphDatabaseService service = database.getGraphDatabaseService();

        Node node1;
        Node node2;
        {
            Transaction transaction = service.beginTx();
            try
            {
                node1 = service.createNode();
                node2 = service.createNode();

                node1.createRelationshipTo( node2, Type.KNOWS );

                transaction.success();
            }
            finally
            {
                transaction.finish();
            }
        }
        assertExactlyOneRelationship( node1, node2 );
        {
            Transaction transaction = service.beginTx();
            try
            {
                new SingleRelationshipEnforcer( transaction ).relate( node1, node2, Type.KNOWS );

                transaction.success();
            }
            finally
            {
                transaction.finish();
            }
        }

        assertExactlyOneRelationship( node1, node2 );
    }

    @Test
    public void shouldAllowOtherChangesToTheStartNodeInTheSameTransaction() throws Exception
    {
        GraphDatabaseService service = database.getGraphDatabaseService();

        Node node1;
        Node node2;
        Transaction transaction = service.beginTx();
        try
        {
            node1 = service.createNode();
            node2 = service.createNode();

            new SingleRelationshipEnforcer( transaction ).relate( node1, node2, Type.KNOWS );
            node1.setProperty( "key1", "value1" );

            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
        assertExactlyOneRelationship( node1, node2 );
    }

    private void assertExactlyOneRelationship( Node node1, Node node2 )
    {
        Iterator<Relationship> relationshipIterator = node1.getRelationships( Direction.OUTGOING ).iterator();
        Relationship relationship = relationshipIterator.next();
        assertEquals( node2, relationship.getEndNode() );
        assertFalse( relationshipIterator.hasNext() );
    }

}
