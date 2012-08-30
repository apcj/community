package org.neo4j.helpers.progress;

import java.io.PrintWriter;

public abstract class Indicator
{
    static final Indicator NONE = new Indicator( 1 )
    {
        @Override
        protected void progress( int from, int to )
        {
            // do nothing
        }
    };
    private final int reportResolution;

    public Indicator( int reportResolution )
    {
        this.reportResolution = reportResolution;
    }

    protected abstract void progress( int from, int to );

    int reportResolution()
    {
        return reportResolution;
    }

    public void startProcess( long totalCount )
    {
        // default: do nothing
    }

    public void startPart( String part, long totalCount )
    {
        // default: do nothing
    }

    public void completePart( String part )
    {
        // default: do nothing
    }

    public void completeProcess()
    {
        // default: do nothing
    }

    public void failure( Throwable cause )
    {
        // default: do nothing
    }

    public static abstract class Decorator extends Indicator
    {
        private final Indicator indicator;

        public Decorator( ProgressMonitorFactory factory, String process )
        {
            this( factory.newIndicator( process ) );
        }

        public Decorator( Indicator indicator )
        {
            super( indicator.reportResolution() );
            this.indicator = indicator;
        }

        @Override
        public void startProcess( long totalCount )
        {
            indicator.startProcess( totalCount );
        }

        @Override
        public void startPart( String part, long totalCount )
        {
            indicator.startPart( part, totalCount );
        }

        @Override
        public void completePart( String part )
        {
            indicator.completePart( part );
        }

        @Override
        public void completeProcess()
        {
            indicator.completeProcess();
        }

        @Override
        protected void progress( int from, int to )
        {
            indicator.progress( from, to );
        }

        @Override
        public void failure( Throwable cause )
        {
            indicator.failure( cause );
        }
    }

    static class Textual extends Indicator
    {
        private final String process;
        private final PrintWriter out;

        Textual( String process, PrintWriter out )
        {
            super( 200 );
            this.process = process;
            this.out = out;
        }

        @Override
        public void startProcess( long totalCount )
        {
            out.println( process );
        }

        @Override
        protected void progress( int from, int to )
        {
            for ( int i = from; i < to; )
            {
                printProgress( ++i );
            }
            out.flush();
        }

        @Override
        public void failure( Throwable cause )
        {
            cause.printStackTrace( out );
        }

        private void printProgress( int progress )
        {
            out.print( '.' );
            if ( progress % 20 == 0 )
            {
                out.printf( " %3d%%%n", progress / 2 );
            }
        }
    }
}
