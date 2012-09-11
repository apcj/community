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

public class Cart
{
    private final Storage storage;
    private final int capacity;

    private int p = 0;
    private int q = 0;
    private int nS = 0;
    private int nL = 0;
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
            page.referenced = true;
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
                    recencyHistory.removeHead();
                }
                else
                {
                    frequencyHistory.removeHead();
                }
            }
        }

        if ( page.currentList == recencyHistory )
        {
            p = min( p + max( 1, nS / recencyHistory.size() ), capacity );
            page.moveToTailOf( recencyCache );
            page.referenced = false;
            page.filter = FilterBit.L;
            nL++;
        }
        else if ( page.currentList == frequencyHistory )
        {
            p = max( p - max( 1, nL / frequencyHistory.size() ), 0 );
            page.moveToTailOf( recencyCache );
            page.referenced = false;
            nL++;
            if ( frequencyCache.size() + frequencyHistory.size() + recencyCache.size() - nS >= capacity )
            {
                q = min( q + 1, 2 * capacity - recencyCache.size() );
            }
        }
        else
        {
            page.moveToTailOf( recencyCache );
            nS++;
        }

        storage.load( address );
    }

    private void replace()
    {
        while ( frequencyCache.size() > 0 && frequencyCache.head.referenced )
        {
            Page page = frequencyCache.head;
            page.referenced = false;
            page.moveToTailOf( recencyCache );

            if ( frequencyCache.size() + frequencyHistory.size() + recencyHistory.size() - nS >= capacity )
            {
                q = min( q + 1, 2 * capacity - recencyCache.size() );
            }
        }

        while ( recencyCache.size() > 0 && (recencyCache.head.filter == FilterBit.L || recencyCache.head.referenced) )
        {
            if ( recencyCache.head.referenced )
            {
                Page page = recencyCache.head;
                page.referenced = false;
                page.moveToTailOf( recencyCache );

                if ( recencyCache.size() > min( p + 1, recencyHistory.size() ) && page.filter == FilterBit.S )
                {
                    page.filter = FilterBit.L;
                    nS--;
                    nL++;
                }
            }
            else
            {
                Page page = recencyCache.head;
                page.referenced = false;
                page.moveToTailOf( frequencyCache );

                q = max( q - 1, capacity - recencyCache.size() );
            }
        }

        if ( recencyCache.size() >= max( 1, p ) )
        {
            storage.evict( recencyCache.head.address );
            recencyCache.head.moveToTailOf( recencyHistory );
        }
        else
        {
            storage.evict( frequencyCache.head.address );
            frequencyCache.head.moveToTailOf( frequencyHistory );
        }
        nS--;
    }

    private static int min( int i1, int i2 )
    {
        return i1 < i2 ? i1 : i2;
    }

    private static int max( int i1, int i2 )
    {
        return i1 > i2 ? i1 : i2;
    }

    public interface Storage
    {
        void hit( int address );

        void load( int address );

        void evict( int address );
    }
}
