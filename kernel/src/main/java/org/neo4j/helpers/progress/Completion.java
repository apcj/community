package org.neo4j.helpers.progress;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.ProcessFailureException;

public final class Completion
{
    private volatile Collection<Runnable> callbacks = new ArrayList<Runnable>();
    private Throwable processFailureCause;

    void complete()
    {
        Collection<Runnable> callbacks = this.callbacks;
        if ( callbacks != null )
        {
            Runnable[] targets;
            synchronized ( callbacks )
            {
                targets = callbacks.toArray( new Runnable[callbacks.size()] );
                this.callbacks = null;
            }
            for ( Runnable target : targets )
            {
                try
                {
                    target.run();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
        }
    }

    void signalFailure( Throwable e )
    {
        this.processFailureCause = e;
        complete();
    }

    void notify( Runnable callback )
    {
        if ( callback == null )
        {
            throw new IllegalArgumentException( "callback may not be null" );
        }
        Collection<Runnable> callbacks = this.callbacks;
        if ( callbacks != null )
        {
            synchronized ( callbacks )
            {
                if ( this.callbacks == callbacks )
                { // double checked locking
                    callbacks.add( callback );
                    callback = null; // we have not reached completion
                }
            }
        }
        if ( callback != null )
        { // we have already reached completion
            callback.run();
        }
    }

    public void await( long timeout, TimeUnit unit )
            throws InterruptedException, TimeoutException, ProcessFailureException
    {
        CountDownLatch latch = null;
        Collection<Runnable> callbacks = this.callbacks;
        if ( callbacks != null )
        {
            synchronized ( callbacks )
            {
                if ( this.callbacks == callbacks )
                { // double checked locking
                    callbacks.add( new CountDown( latch = new CountDownLatch( 1 ) ) );
                }
            }
        }
        if ( latch != null )
        { // await completion
            if ( !latch.await( timeout, unit ) )
            {
                throw new TimeoutException(
                        String.format( "Process did not complete within %d %s.", timeout, unit.name() ) );
            }
        }
        if (processFailureCause != null)
        {
            throw new ProcessFailureException( processFailureCause );
        }
    }

    private static final class CountDown implements Runnable
    {
        private final CountDownLatch latch;

        CountDown( CountDownLatch latch )
        {
            this.latch = latch;
        }

        @Override
        public void run()
        {
            latch.countDown();
        }
    }
}
