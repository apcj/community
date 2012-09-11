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

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.hasItem;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class CartTest
{
    @Test
    public void linearScanWithSingleHitPerWindowLeadsToFifoEviction() throws Exception
    {
        // given
        StorageSpy storage = new StorageSpy();
        int capacity = 10;
        Cart cart = new Cart( storage, capacity, capacity * 2 );

        // when
        for ( int i = 0; i < capacity * 2; i++ )
        {
            cart.acquire( i );
        }

        // then
        assertArrayEquals( new String[]{
                "l0", "l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8", "l9",
                "e0", "l10", "e1", "l11", "e2", "l12", "e3", "l13", "e4",
                "l14", "e5", "l15", "e6", "l16", "e7", "l17", "e8", "l18", "e9", "l19"
        }, storage.events.toArray() );
    }

    @Test
    public void frequentlyAccessedPagesDoNotGetEvicted() throws Exception
    {
        // given
        StorageSpy storage = new StorageSpy();
        int capacity = 10;
        Cart cart = new Cart( storage, capacity, capacity * 2 );

        // when
        for ( int i = 0; i < capacity * 2; i++ )
        {
            // even number pages accessed more frequently
            cart.acquire( (i / 2) * 2 );

            // background access of even and odd numbered pages
            cart.acquire( i );
        }

        // then
        assertThat( storage.events, not( hasItem( "e0" ) ) );
        assertThat( storage.events, hasItem( "e1" ) );
        assertThat( storage.events, not( hasItem( "e2" ) ) );
        assertThat( storage.events, hasItem( "e3" ) );
        assertThat( storage.events, not( hasItem( "e4" ) ) );
        assertThat( storage.events, hasItem( "e5" ) );
        assertThat( storage.events, not( hasItem( "e6" ) ) );
        assertThat( storage.events, hasItem( "e7" ) );
        assertThat( storage.events, not( hasItem( "e8" ) ) );
        assertThat( storage.events, hasItem( "e9" ) );
    }

    private static class StorageSpy implements Cart.Storage
    {
        List<String> events = new ArrayList<String>();

        @Override
        public void load( int address )
        {
            events.add( "l" + address );
        }

        @Override
        public void evict( int address )
        {
            events.add( "e" + address );
        }
    }
}
