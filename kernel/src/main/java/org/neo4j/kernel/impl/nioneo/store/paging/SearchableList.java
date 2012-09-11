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

class SearchableList
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
