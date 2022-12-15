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
package org.apache.pulsar;

import java.util.concurrent.atomic.AtomicInteger;

public class LockCoordinator {

    public static final AtomicInteger PROCESS_COODINATOR = new AtomicInteger();

    public static boolean waitForValue(int expectValue, int toValue){
        while (true){
            if (PROCESS_COODINATOR.compareAndSet(expectValue, toValue)){
                return true;
            }
            if (PROCESS_COODINATOR.get() >= toValue){
                return false;
            }
            System.out.println("===> " + Thread.currentThread().getName() + ", wait for " + expectValue + " --> " + toValue + ", current:" + PROCESS_COODINATOR.get());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
