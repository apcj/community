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
package org.neo4j.kernel.impl.nioneo.store.windowpool;

import org.neo4j.kernel.impl.util.StringLogger;

public class LoggingStatisticsListener implements MappingStatisticsListener
{
    private final StringLogger logger;

    public LoggingStatisticsListener( StringLogger logger )
    {
        this.logger = logger;
    }

    @Override
    public void onStatistics( String storeFileName, int acquiredPages, int mappedPages, long
            samplePeriod )
    {
        logger.logMessage( String.format( "In %s: %d pages acquired, %d pages mapped (%.2f%%) in %d ms",
                storeFileName, acquiredPages, mappedPages,
                (100.0 * mappedPages) / acquiredPages, samplePeriod ) );
    }
}
