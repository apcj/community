package org.neo4j.helpers.progress;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

public abstract class ProgressMonitorFactory
{
    public static final ProgressMonitorFactory NONE = new ProgressMonitorFactory()
    {
        @Override
        protected Indicator newIndicator( String process )
        {
            return Indicator.NONE;
        }
    };

    public static ProgressMonitorFactory textual( final OutputStream out )
    {
        return textual( new PrintWriter( out ) );
    }

    public static ProgressMonitorFactory textual( final Writer out )
    {
        return new ProgressMonitorFactory()
        {
            @Override
            protected Indicator newIndicator( String process )
            {
                if ( out instanceof PrintWriter )
                {
                    return new Indicator.Textual( process, (PrintWriter) out );
                }
                else
                {
                    return new Indicator.Textual( process, new PrintWriter( out ) );
                }
            }
        };
    }

    public final MultiPartBuilder multipleParts( String process )
    {
        return new MultiPartBuilder( this, process );
    }

    public final ProgressListener singlePart( String process, long totalCount )
    {
        return new ProgressListener.SinglePartProgressListener( newIndicator( process ), totalCount );
    }

    protected abstract Indicator newIndicator( String process );

    public static final class MultiPartBuilder
    {
        private Aggregator aggregator;
        private Set<String> parts = new HashSet<String>();
        private Completion completion = null;

        private MultiPartBuilder( ProgressMonitorFactory factory, String process )
        {
            this.aggregator = new Aggregator(factory.newIndicator( process ));
        }

        public ProgressListener progressForPart( String part, long totalCount )
        {
            if ( aggregator == null )
            {
                throw new IllegalStateException( "Builder has been completed." );
            }
            if ( !parts.add( part ) )
            {
                throw new IllegalArgumentException( String.format( "Part '%s' has already been defined.", part ) );
            }
            ProgressListener.MultiPartProgressListener progress = new ProgressListener.MultiPartProgressListener( aggregator, part, totalCount );
            aggregator.add( progress );
            return progress;
        }

        public Completion build()
        {
            if ( aggregator != null )
            {
                completion = aggregator.initialize();
            }
            aggregator = null;
            parts = null;
            return completion;
        }
    }
}
