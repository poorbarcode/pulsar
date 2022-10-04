/**
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
package org.apache.bookkeeper.mledger;

import java.util.Comparator;

public class PositionComparators {

    public static final Comparator<Position> COMPARE_WITH_LEDGER_AND_ENTRY_ID = new Comparator<Position> (){

        @Override
        public int compare(Position pos1, Position pos2) {
            int ledgerCompare = Long.compare(pos1.getLedgerId(), pos2.getLedgerId());
            if (ledgerCompare != 0) {
                return ledgerCompare;
            }
            return Long.compare(pos1.getEntryId(), pos2.getEntryId());
        }
    };
}
