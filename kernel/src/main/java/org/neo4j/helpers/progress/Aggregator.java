package org.neo4j.helpers.progress;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class Aggregator
{
    private final Map<ProgressListener.MultiPartProgressListener, ProgressListener.MultiPartProgressListener.State> states =
            new ConcurrentHashMap<ProgressListener.MultiPartProgressListener, ProgressListener.MultiPartProgressListener.State>();
    private Indicator indicator;
    private volatile long progress; // accessed through updater
    private volatile int last; // accessed through updater
    private static final AtomicLongFieldUpdater<Aggregator> PROGRESS = newUpdater( Aggregator.class, "progress" );
    private static final AtomicIntegerFieldUpdater<Aggregator> LAST =
            AtomicIntegerFieldUpdater.newUpdater( Aggregator.class, "last" );
    private long totalCount = 0;
    private final Completion completion = new Completion();

    public Aggregator( Indicator indicator )
    {
        this.indicator = indicator;
    }

    synchronized void add( ProgressListener.MultiPartProgressListener progress )
    {
        states.put( progress, ProgressListener.MultiPartProgressListener.State.INIT );
        this.totalCount += progress.totalCount;
    }

    synchronized Completion initialize()
    {
        indicator.startProcess( totalCount );
        if ( states.isEmpty() )
        {
            indicator.progress( 0, indicator.reportResolution() );
            indicator.completeProcess();
        }
        return completion;
    }

    void update( long delta )
    {
        long progress = PROGRESS.addAndGet( this, delta );
        int current = (int) ((progress * indicator.reportResolution()) / totalCount);
        for ( int last = this.last; current > last; last = this.last )
        {
            if ( LAST.compareAndSet( this, last, current ) )
            {
                synchronized ( this )
                {
                    indicator.progress( last, current );
                }
            }
        }
    }

    void start( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.put( part, ProgressListener.MultiPartProgressListener.State.LIVE ) == ProgressListener
                .MultiPartProgressListener.State.INIT )
        {
            indicator.startPart( part.part, part.totalCount );
        }
    }

    void complete( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.remove( part ) != null )
        {
            indicator.completePart( part.part );
            if ( states.isEmpty() )
            {
                indicator.completeProcess();
                completion.complete();
            }
        }
    }

    public void signalFailure( Throwable e )
    {
        completion.signalFailure( e );
    }
}
