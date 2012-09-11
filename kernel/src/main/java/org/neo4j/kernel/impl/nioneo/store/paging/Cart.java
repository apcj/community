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

import java.util.LinkedList;

public class Cart
{
    private final Storage storage;
    private final int capacity;

    private int p = 0;
    private int q = 0;
    private int nS = 0;
    private int nL = 0;
    private SearchableList t1 = new SearchableList();
    private SearchableList b1 = new SearchableList();
    private SearchableList t2 = new SearchableList();
    private SearchableList b2 = new SearchableList();

    public Cart( Storage storage, int capacity )
    {
        this.storage = storage;
        this.capacity = capacity;
    }

    public void acquire( int address )
    {
        if ( t1.contains( address ) )
        {
            t1.setReferenced( address );
            return; // hit
        }
        if ( t2.contains( address ) )
        {
            t2.setReferenced( address );
            return; // hit
        }
        if ( t1.size() + t2.size() == capacity )
        {
            // cache full
            replace();

            // history replace
            if ( !b1.contains( address ) && !b2.contains( address )
                    && b1.size() + b2.size() == capacity + 1 )
            {
                if ( b1.size() > max( 0, q ) || b2.size() == 0 )
                {
                    b1.removeLast();
                }
                else
                {
                    b2.removeLast();
                }
            }
        }

        if ( b1.contains( address ) )
        {
            p = min( p + max( 1, nS / b1.size() ), capacity );
            Page page = b1.remove( address );
            t1.append( page );
            page.referenced = false;
            page.filter = FilterBit.L;
            nL++;
        }
        else if ( b2.contains( address ) )
        {
            p = max( p - max( 1, nL / b2.size() ), 0 );
            Page page = b2.remove( address );
            t1.append( page );
            page.referenced = false;
            nL++;
            if ( t2.size() + b2.size() + t1.size() - nS >= capacity )
            {
                q = min( q + 1, 2 * capacity - t1.size() );
            }
        }
        else
        {
            t1.append( address );
            nS++;
        }

        storage.load( address );
    }

    private void replace()
    {
        while ( t2.size() > 0 && t2.head().referenced )
        {
            Page page = t2.removeHead();
            page.referenced = false;
            t1.append( page );
            if ( t2.size() + b2.size() + b1.size() - nS >= capacity )
            {
                q = min( q + 1, 2 * capacity - t1.size() );
            }
        }

        while ( t1.size() > 0 && (t1.head().filter == FilterBit.L || t1.head().referenced) )
        {
            if ( t1.head().referenced )
            {
                Page page = t1.removeHead();
                page.referenced = false;
                t1.append( page );
                if ( t1.size() > min( p + 1, b1.size() ) && page.filter == FilterBit.S )
                {
                    page.filter = FilterBit.L;
                    nS--;
                    nL++;
                }
            }
            else
            {
                Page page = t1.removeHead();
                page.referenced = false;
                t2.append( page );
                q = max( q - 1, capacity - t1.size() );
            }
        }

        if ( t1.size() >= max( 1, p ) )
        {
            Page page = t1.removeHead();
            storage.evict( page.address );
            b1.insertHead( page );
            nS--;
        }
        else
        {
            Page page = t2.removeHead();
            storage.evict( page.address );
            b2.insertHead( page );
            nS--;
        }
    }

    private static class SearchableList
    {
        LinkedList<Page> list = new LinkedList<Page>();

        public boolean contains( int address )
        {
            return list.contains( new Page( address ) );
        }

        public Page head()
        {
            return list.getFirst();
        }

        public void setReferenced( int address )
        {
            list.get( list.indexOf( new Page( address ) ) ).referenced = true;
        }

        public int size()
        {
            return list.size();
        }

        public void append( int address )
        {
            list.addLast( new Page( address ) );
        }

        public void append( Page page )
        {
            list.addLast( page );
        }

        public Page removeHead()
        {
            return list.removeFirst();
        }

        public void removeLast()
        {
            list.removeLast();
        }

        public Page remove( int address )
        {
            return list.remove( list.indexOf( new Page( address ) ) );
        }

        public void insertHead( Page page )
        {
            list.add( 0, page );
        }
    }

    private enum FilterBit
    {
        S, L
    }

    private static class Page
    {
        private int address;
        private boolean referenced = false;
        private FilterBit filter = FilterBit.S;

        private Page( int address )
        {
            this.address = address;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals( Object o )
        {
            return ((Page) o).address == address;
        }

        @Override
        public int hashCode()
        {
            return address;
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

    public interface Storage
    {
        void load( int address );

        void evict( int address );
    }
}
