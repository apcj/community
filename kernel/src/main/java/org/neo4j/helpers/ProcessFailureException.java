package org.neo4j.helpers;

public class ProcessFailureException extends Exception
{
    public ProcessFailureException( Throwable cause )
    {
        super( "Monitored process failed", cause );
    }
}
