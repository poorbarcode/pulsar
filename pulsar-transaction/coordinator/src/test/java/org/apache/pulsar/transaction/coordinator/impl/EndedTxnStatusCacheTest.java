/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.coordinator.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.EndedTxnStatus;
import org.testng.annotations.Test;

public class EndedTxnStatusCacheTest {

    @Test
    public void testCreateEndedTxnStatusCacheWithUnboundedDimensions() {
        TxnID txnID = new TxnID(1L, 1L);
        long now = System.currentTimeMillis();

        EndedTxnStatusCache cache = EndedTxnStatusCache.create(3600000L, 100000L, null);
        assertTrue(cache.record(txnID, EndedTxnStatus.COMMITTED, null, now));
        assertEquals(cache.get(txnID), EndedTxnStatus.COMMITTED);

        cache = EndedTxnStatusCache.create(-1L, 100000L, null);
        assertTrue(cache.record(txnID, EndedTxnStatus.ABORTED, null, now));
        assertEquals(cache.get(txnID), EndedTxnStatus.ABORTED);

        cache = EndedTxnStatusCache.create(3600000L, -1L, null);
        assertTrue(cache.record(txnID, EndedTxnStatus.TIMEOUT, null, now));
        assertEquals(cache.get(txnID), EndedTxnStatus.TIMEOUT);

        cache = EndedTxnStatusCache.create(0L, 100000L, null);
        assertFalse(cache.record(txnID, EndedTxnStatus.COMMITTED, null, now));
        assertNull(cache.get(txnID));

        cache = EndedTxnStatusCache.create(3600000L, 0L, null);
        assertFalse(cache.record(txnID, EndedTxnStatus.COMMITTED, null, now));
        assertNull(cache.get(txnID));

        cache = EndedTxnStatusCache.create(1L, -1L, null);
        assertFalse(cache.record(txnID, EndedTxnStatus.ABORTED, null, now - 10000L));
        assertNull(cache.get(txnID));

        assertThrows(IllegalArgumentException.class, () -> EndedTxnStatusCache.create(-1L, -1L, null));
    }
}
