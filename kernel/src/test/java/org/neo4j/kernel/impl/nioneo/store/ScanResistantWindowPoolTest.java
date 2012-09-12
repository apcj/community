package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.channels.FileChannel;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;

public class ScanResistantWindowPoolTest
{
    @Test
    public void shouldMapConsecutiveWindowsWithAppropriateBoundaries() throws Exception
    {
        // given
        int bytesPerRecord = 9;
        int targetBytesPerPage = 4096;
        FileChannel fileChannel = mock( FileChannel.class );
        ScanResistantWindowPool pool = new ScanResistantWindowPool( "storeFileName", bytesPerRecord, targetBytesPerPage, fileChannel );

        // when
        PersistenceWindow window0 = pool.acquire( 0, OperationType.READ );

        // then
        assertEquals( 0, window0.position() );
        assertThat( window0.size(), greaterThan( 0 ) );
        assertEquals( bytesPerRecord, window0.getRecordSize() );
        verify( fileChannel ).map( eq( FileChannel.MapMode.READ_ONLY ), eq( 0L ), anyLong() );

        // when
        PersistenceWindow window1 = pool.acquire( window0.size(), OperationType.READ );

        // then
        assertEquals( window0.size(), window1.size() );
        assertEquals( window0.getRecordSize(), window1.getRecordSize() );
        assertEquals( window0.size(), window1.position() );

        verify( fileChannel ).map( eq( FileChannel.MapMode.READ_ONLY ), eq( (long) window0.size() * bytesPerRecord ), anyLong() );
    }

    @Test
    public void shouldRejectRecordSizeGreaterThanTargetBytesPerPage() throws Exception
    {
        // given
        int targetBytesPerPage = 4096;
        int bytesPerRecord = targetBytesPerPage + 1;

        // when
        try
        {
            new ScanResistantWindowPool( "storeFileName", bytesPerRecord, targetBytesPerPage,
                    mock( FileChannel.class ) );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldRejectRecordSizeOfZero() throws Exception
    {
        // when
        try
        {
            new ScanResistantWindowPool( "storeFileName", 0, 4096, mock( FileChannel.class ) );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalArgumentException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldRejectWriteOperations() throws Exception
    {
        // given
        int recordSize = 9;
        int targetBytesPerPage = 4096;
        ScanResistantWindowPool pool = new ScanResistantWindowPool( "storeFileName", recordSize, targetBytesPerPage,
                mock( FileChannel.class ) );

        // when
        try
        {
            pool.acquire( 0, OperationType.WRITE );
            fail( "should have thrown exception" );
        }
        // then
        catch ( UnsupportedOperationException e )
        {
            // expected
        }
    }
    private Matcher<Integer> greaterThan( final int value )
    {
        return new TypeSafeMatcher<Integer>()
        {
            @Override
            public boolean matchesSafely( Integer item )
            {
                return item > value;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "value > " ).appendValue( value );
            }
        };
    }

}
