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
    LinkedList<Integer> list = new LinkedList<Integer>();
    private final Cart.CacheList listName;
    private final Cart cart;

    public SearchableList( Cart.CacheList listName, Cart cart )
    {
        this.listName = listName;
        this.cart = cart;
    }

    public int head()
    {
        return list.getFirst();
    }

    public int size()
    {
        return list.size();
    }

    public void append( int address )
    {
        list.addLast( address );
        cart.allPages[address].inList = listName;
    }

    public int removeHead()
    {
        Integer address = list.removeFirst();
        cart.allPages[address].inList = Cart.CacheList.none;
        return address;
    }

    public void removeLast()
    {
        Integer address = list.removeLast();
        cart.allPages[address].inList = Cart.CacheList.none;
    }

    public void remove( int address )
    {
        list.removeFirstOccurrence( address );
        cart.allPages[address].inList = Cart.CacheList.none;
    }

    public void insertHead( int address )
    {
        list.add( 0, address );
        cart.allPages[address].inList = listName;
    }
}
