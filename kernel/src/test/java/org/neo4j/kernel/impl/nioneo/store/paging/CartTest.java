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
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.hasItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
                "L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9",
                "E0", "L10", "E1", "L11", "E2", "L12", "E3", "L13", "E4",
                "L14", "E5", "L15", "E6", "L16", "E7", "L17", "E8", "L18", "E9", "L19"
        }, storage.events.toArray() );
    }

    @Test
    public void reverseLinearScanWithSingleHitPerWindowLeadsToFifoEviction() throws Exception
    {
        // given
        StorageSpy storage = new StorageSpy();
        int capacity = 10;
        Cart cart = new Cart( storage, capacity, capacity * 2 );

        // when
        for ( int i = capacity * 2 - 1; i >= 0; i-- )
        {
            cart.acquire( i );
        }

        // then
        assertArrayEquals( new String[]{
                "L19", "L18", "L17", "L16", "L15", "L14", "L13", "L12", "L11", "L10",
                "E19", "L9", "E18", "L8", "E17", "L7", "E16", "L6", "E15", "L5",
                "E14", "L4", "E13", "L3", "E12", "L2", "E11", "L1", "E10", "L0",
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
        assertThat( storage.events, not( hasItem( "E0" ) ) );
        assertThat( storage.events, hasItem( "E1" ) );
        assertThat( storage.events, not( hasItem( "E2" ) ) );
        assertThat( storage.events, hasItem( "E3" ) );
        assertThat( storage.events, not( hasItem( "E4" ) ) );
        assertThat( storage.events, hasItem( "E5" ) );
        assertThat( storage.events, not( hasItem( "E6" ) ) );
        assertThat( storage.events, hasItem( "E7" ) );
        assertThat( storage.events, not( hasItem( "E8" ) ) );
        assertThat( storage.events, hasItem( "E9" ) );
    }

    @Test
    public void linearPlusRandom() throws Exception
    {
        // given
        StorageSpy storage = new StorageSpy();
        int capacity = 10;
        int randoms = 4;
        int pageSize = 10;
        Cart cart = new Cart( storage, capacity, capacity * 2 );

        // when
        Random random = new Random();
        for ( int i = 0; i < capacity * 2; i++ )
        {
            // background access of even and odd numbered pages
            for ( int j = 0; j < pageSize; j++ )
            {
                storage.linear = true;
                cart.acquire( i );
                storage.linear = false;
                for ( int k = 0; k < randoms; k++ )
                {
                    cart.acquire( random.nextInt( capacity * 2 ) );
                }
            }
        }

        // then
        assertTrue( storage.linMiss * pageSize <= (storage.linMiss + storage.linHit) );
        assertTrue( storage.rndMiss * 2 <= (storage.rndMiss + storage.rndHit) * 1.05 );
    }

    private static class StorageSpy implements Cart.Storage
    {
        List<String> events = new ArrayList<String>();

        int hitCount, loadCount;
        boolean linear;
        int linHit, linMiss;
        int rndHit, rndMiss;

        @Override
        public void hit( int address )
        {
            hitCount++;
            if ( linear )
            {
                linHit++;
            }
            else
            {
                rndHit++;
            }
        }

        @Override
        public void load( int address )
        {
            events.add( "L" + address );
            loadCount++;
            if ( linear )
            {
                linMiss++;
            }
            else
            {
                rndMiss++;
            }
        }

        @Override
        public void evict( int address )
        {
            events.add( "E" + address );
        }
    }
}
