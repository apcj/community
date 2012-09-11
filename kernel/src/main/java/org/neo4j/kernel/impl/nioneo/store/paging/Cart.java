/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.store.paging;

public class Cart implements TemporalUtilityCounter
{
    private final Storage storage;
    private final int capacity;

    private int p = 0;
    private int q = 0;
    private int shortTermUtilityPageCount = 0;
    private int longTermUtilityPageCount = 0;

    private CachedPageList recencyCache = new CachedPageList();
    private CachedPageList recencyHistory = new CachedPageList();
    private CachedPageList frequencyCache = new CachedPageList();
    private CachedPageList frequencyHistory = new CachedPageList();
    Page[] allPages;

    public Cart( Storage storage, int capacity, int maxAddress )
    {
        this.storage = storage;
        this.capacity = capacity;
        allPages = new Page[maxAddress];
        for ( int address = 0; address < maxAddress; address++ )
        {
            allPages[address] = new Page( address );
        }
    }

    public void acquire( int address )
    {
        Page page = allPages[address];
        if ( page.currentList == recencyCache || page.currentList == frequencyCache )
        {
            page.setReferenced();
            storage.hit( address );
            return; // hit
        }

        if ( recencyCache.size() + frequencyCache.size() == capacity )
        {
            // cache full
            replace();

            // history replace
            if ( page.currentList != recencyHistory && page.currentList != frequencyHistory
                    && recencyHistory.size() + frequencyHistory.size() == capacity + 1 )
            {
                if ( recencyHistory.size() > max( 0, q ) || frequencyHistory.size() == 0 )
                {
                    recencyHistory.removeHead().setUtility( this, TemporalUtility.UNKNOWN );
                }
                else
                {
                    frequencyHistory.removeHead().setUtility( this, TemporalUtility.UNKNOWN );
                }
            }
        }

        if ( page.currentList == recencyHistory )
        {
            p = min( p + max( 1, shortTermUtilityPageCount / recencyHistory.size() ), capacity );
            page.clearReference().moveToTailOf( recencyCache ).setUtility( this, TemporalUtility.LONG_TERM );
        }
        else if ( page.currentList == frequencyHistory )
        {
            p = max( p - max( 1, longTermUtilityPageCount / frequencyHistory.size() ), 0 );
            page.clearReference().moveToTailOf( recencyCache ).setUtility( this, TemporalUtility.LONG_TERM );
            if ( frequencyCache.size() + frequencyHistory.size() + recencyCache.size() - shortTermUtilityPageCount >= capacity )
            {
                q = min( q + 1, 2 * capacity - recencyCache.size() );
            }
        }
        else
        {
            page.moveToTailOf( recencyCache ).setUtility( this, TemporalUtility.SHORT_TERM );
        }

        storage.load( address );
    }

    private void replace()
    {
        while ( frequencyCache.size() > 0 && frequencyCache.head.referenced )
        {
            frequencyCache.head.clearReference().moveToTailOf( recencyCache );

            if ( frequencyCache.size() + frequencyHistory.size() + recencyHistory.size() - shortTermUtilityPageCount >= capacity )
            {
                q = min( q + 1, 2 * capacity - recencyCache.size() );
            }
        }

        while ( recencyCache.size() > 0 && (recencyCache.head.utility == TemporalUtility.LONG_TERM || recencyCache.head.referenced) )
        {
            if ( recencyCache.head.referenced )
            {
                Page page = recencyCache.head.clearReference().moveToTailOf( recencyCache );

                if ( recencyCache.size() > min( p + 1, recencyHistory.size() ) && page.utility == TemporalUtility.SHORT_TERM )
                {
                    page.setUtility( this, TemporalUtility.LONG_TERM );
                }
            }
            else
            {
                recencyCache.head.clearReference().moveToTailOf( frequencyCache );

                q = max( q - 1, capacity - recencyCache.size() );
            }
        }

        if ( recencyCache.size() >= max( 1, p ) )
        {
            storage.evict( recencyCache.head.address );
            recencyCache.head.moveToTailOf( recencyHistory ).setUtility( this, TemporalUtility.LONG_TERM );
        }
        else
        {
            storage.evict( frequencyCache.head.address );
            frequencyCache.head.moveToTailOf( frequencyHistory ).setUtility( this, TemporalUtility.SHORT_TERM );
        }
    }

    private static int min( int i1, int i2 )
    {
        return i1 < i2 ? i1 : i2;
    }

    private static int max( int i1, int i2 )
    {
        return i1 > i2 ? i1 : i2;
    }

    @Override
    public void increment( TemporalUtility utility )
    {
        switch ( utility )
        {
            case SHORT_TERM:
                shortTermUtilityPageCount++;
                break;
            case LONG_TERM:
                longTermUtilityPageCount++;
                break;
        }
    }

    @Override
    public void decrement( TemporalUtility utility )
    {
        switch ( utility )
        {
            case SHORT_TERM:
                shortTermUtilityPageCount--;
                break;
            case LONG_TERM:
                longTermUtilityPageCount--;
                break;
        }
    }

    public interface Storage
    {
        void hit( int address );

        void load( int address );

        void evict( int address );
    }
}
