/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.debezium.internal;

import org.apache.flink.util.ExceptionUtils;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import io.debezium.engine.ChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Handover is a utility to hand over data (a buffer of records) and exception from a
 * <i>producer</i> thread to a <i>consumer</i> thread. It effectively behaves like a "size one
 * blocking queue", with some extras around exception reporting, closing, and waking up thread
 * without {@link Thread#interrupt() interrupting} threads.
 *
 * <p>This class is used in the Flink Debezium Engine Consumer to hand over data and exceptions
 * between the thread that runs the DebeziumEngine class and the main thread.
 *
 * <p>The Handover can also be "closed", signalling from one thread to the other that it the thread
 * has terminated.
 */
//TODO 这个类由两个线程访问, pollNext由debeziumFetcher调用,produce由debeziumConsumer调用,因为涉及多线程的调用,单纯的讲代码可能不容易理解,
// 可以去复习一下java多线程知识内容,或者自己debug一下看看调用流程就比较容易理解了
@ThreadSafe //todo 表示类是线程安全的,这类涉及engine和source线程两个线程操作,内部的实现保证了线程安全
@Internal
public class Handover implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Handover.class);
    private final Object lock = new Object();

    @GuardedBy("lock") // todo 注解表示该变量受lock的保护, 不是重点勿关注
    private List<ChangeEvent<SourceRecord, SourceRecord>> next;

    @GuardedBy("lock")
    @Nullable
    private Throwable error;

    private boolean wakeupProducer;

    /**
     * Polls the next element from the Handover, possibly blocking until the next element is
     * available. This method behaves similar to polling from a blocking queue.
     *
     * <p>If an exception was handed in by the producer ({@link #reportError(Throwable)}), then that
     * exception is thrown rather than an element being returned.
     *
     * @return The next element (buffer of records, never null).
     * @throws ClosedException Thrown if the Handover was {@link #close() closed}.
     * @throws Exception Rethrows exceptions from the {@link #reportError(Throwable)} method.
     */
    //TODO debeziumFetcher 调用,当没有数据的时候进入wait状态,wait状态的时候cpu是不会调用wait状态的线程,另一个线程就可以占用cpu的全部时间片
    public List<ChangeEvent<SourceRecord, SourceRecord>> pollNext() throws Exception {
        synchronized (lock) {
            // todo 没有数据没有异常则持续循环进入wait状态,为了防止虚假唤醒的情况
            while (next == null && error == null) {
                lock.wait();
            }
            List<ChangeEvent<SourceRecord, SourceRecord>> n = next;
            //TODO 上面的循环可以退出的时候,说明一定是有数据或者有异常,不存在其他的情况
            if (n != null) {
                //TODO 将next置为null 下面会根据此条件作为判断条件
                next = null;
                //TODO 唤醒其他等待线程,当然只可能是engine线程
                lock.notifyAll();
                return n;
            } else {
                //TODO  将异常抛出
                ExceptionUtils.rethrowException(error, error.getMessage());

                // this statement cannot be reached since the above method always throws an
                // exception this is only here to silence the compiler and any warnings
                //TODO 上面方法一定会抛出异常,改代码只是为了去掉编译警告...
                return Collections.emptyList();
            }
        }
    }

    /**
     * Hands over an element from the producer. If the Handover already has an element that was not
     * yet picked up by the consumer thread, this call blocks until the consumer picks up that
     * previous element.
     *
     * <p>This behavior is similar to a "size one" blocking queue.
     *
     * @param element The next element to hand over.
     * @throws InterruptedException Thrown, if the thread is interrupted while blocking for the
     *     Handover to be empty.
     */
    public void produce(final List<ChangeEvent<SourceRecord, SourceRecord>> element)
            throws InterruptedException {

        checkNotNull(element);

        synchronized (lock) {
            //TODO next不空一直进入wait状态
            while (next != null && !wakeupProducer) {
                lock.wait();
            }

            wakeupProducer = false;
            //todo 有异常抛出异常,没异常将接受新数据,并唤醒fetcher线程
            // an error marks this as closed for the producer
            if (error != null) {
                ExceptionUtils.rethrow(error, error.getMessage());
            } else {
                // if there is no error, then this is open and can accept this element
                next = element;
                lock.notifyAll();
            }
        }
    }

    /**
     * Reports an exception. The consumer will throw the given exception immediately, if it is
     * currently blocked in the {@link #pollNext()} method, or the next time it calls that method.
     *
     * <p>After this method has been called, no call to either {@link #produce( List)} or {@link
     * #pollNext()} will ever return regularly any more, but will always return exceptionally.
     *
     * <p>If another exception was already reported, this method does nothing.
     *
     * <p>For the producer, the Handover will appear as if it was {@link #close() closed}.
     *
     * @param t The exception to report.
     */
    public void reportError(Throwable t) {
        checkNotNull(t);

        synchronized (lock) {
            LOG.error("Reporting error:", t);
            // do not override the initial exception
            if (error == null) {
                error = t;
            }
            next = null;
            lock.notifyAll();
        }
    }

    /**
     * Return whether there is an error.
     *
     * @return whether there is an error
     */
    public boolean hasError() {
        return error != null;
    }

    /**
     * Return the error, if its set.
     *
     * @return the error, if its set
     */
    @Nullable
    public Throwable getError() {
        synchronized (lock) {
            return this.error;
        }
    }

    /**
     * Closes the handover. Both the {@link #produce(List)} method and the {@link #pollNext()} will
     * throw a {@link ClosedException} on any currently blocking and future invocations.
     *
     * <p>If an exception was previously reported via the {@link #reportError(Throwable)} method,
     * that exception will not be overridden. The consumer thread will throw that exception upon
     * calling {@link #pollNext()}, rather than the {@code ClosedException}.
     */
    @Override
    public void close() {
        synchronized (lock) {
            next = null;
            wakeupProducer = false;

            if (error == null) {
                error = new ClosedException();
            } else if (!(error instanceof ClosedException)) {
                error = new ClosedException("Close handover with error.", error);
            }
            lock.notifyAll();
        }
    }

    // ------------------------------------------------------------------------

    /**
     * An exception thrown by the Handover in the {@link #pollNext()} or {@link #produce(List)}
     * method, after the Handover was closed via {@link #close()}.
     */
    public static final class ClosedException extends Exception {

        private static final long serialVersionUID = 1L;

        private static final String GENTLY_CLOSED_MESSAGE = "Close handover gently.";

        public ClosedException() {
            super(GENTLY_CLOSED_MESSAGE);
        }

        public ClosedException(String message, Throwable cause) {
            super(message, cause);
        }

        public static boolean isGentlyClosedException(Throwable cause) {
            return cause instanceof ClosedException
                    && GENTLY_CLOSED_MESSAGE.equals(((ClosedException) cause).getMessage());
        }
    }

    @VisibleForTesting
    public Object getLock() {
        return lock;
    }
}
