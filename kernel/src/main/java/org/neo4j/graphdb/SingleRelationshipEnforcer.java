package org.neo4j.graphdb;

public class SingleRelationshipEnforcer
{
    private final Transaction transaction;

    public SingleRelationshipEnforcer( Transaction transaction )
    {
        this.transaction = transaction;
    }

    public Relationship relate( Node startNode, Node endNode, RelationshipType relationshipType )
    {
        Relationship relationship = findRelationship( startNode, endNode, relationshipType );
        if ( relationship != null )
        {
            return relationship;
        }
        Lock lock = transaction.acquireWriteLock( startNode );
        try
        {
            // check again
            relationship = findRelationship( startNode, endNode, relationshipType );
            if (relationship == null)
            {
                return startNode.createRelationshipTo( endNode, relationshipType );
            }
            return relationship;
        }
        finally
        {
            lock.release();
        }
    }

    private Relationship findRelationship( Node startNode, Node endNode, RelationshipType relationshipType )
    {
        Iterable<Relationship> relationshipIterator = startNode.getRelationships( Direction.OUTGOING, relationshipType );
        for ( Relationship relationship : relationshipIterator )
        {
            if (relationship.getEndNode().equals( endNode ))
            {
                return relationship;
            }
        }
        return null;
    }
}
