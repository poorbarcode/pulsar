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
package org.apache.pulsar.common.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Thread deadlock checkable future.
 */
@Slf4j
public class DeadlockCheckableCompletableFuture<T> extends CompletableFuture<T> {

    private final Supplier<Boolean> inUnDeadLockThread;

    public DeadlockCheckableCompletableFuture (Supplier<Boolean> inUnDeadLockThreadChecker) {
        this.inUnDeadLockThread = inUnDeadLockThreadChecker;
    }

    /**
     * {@inheritDoc}
     */
    public T get() throws InterruptedException, ExecutionException {
        deadlockCheck();
        return super.get();
    }

    /**
     * {@inheritDoc}
     */
    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        deadlockCheck();
        return super.get(timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    public T join() {
        deadlockCheck();
        return super.join();
    }

    private void deadlockCheck() {
        if (!isDone() && inUnDeadLockThread.get()) {
            RuntimeException error = new RuntimeException("Deadlock check failed, this thread encountered a dead lock");
            log.error("", error);
            throw error;
        }
    }
}
