/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.latency;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DumbWatcher;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.watch.IWatchManager;
import org.apache.zookeeper.test.ClientBase;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Fork(3)
public class LatencyBench {

    static public final byte[] randomBytes = new byte[4096];
    static {
        new Random().nextBytes(randomBytes);
    }

    static final String znode = "/bench";

    static public class MyClientBase {
        String hostPort = "127.0.0.1:22334";
        ServerCnxnFactory factory;
        ZooKeeperServer zks;
        File dataDir = new File("zkbenchdata");

        public void setUp() {
            try {
                recursiveDelete(dataDir);
                dataDir.mkdirs();
                factory = ServerCnxnFactory.createFactory(22334, 100);
                zks = new ZooKeeperServer(dataDir, dataDir, 3000);
                zks.setCreateSessionTrackerServerId(1);
                factory.startup(zks);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                System.exit(2);
            }
        }

        void recursiveDelete(File f) {
            if (f.isDirectory()) {
                for (File c: f.listFiles()) {
                    recursiveDelete(c);
                }
            }
            f.delete();
        }
        public void tearDown() {
            try {
                factory.shutdown();
                zks.getZKDatabase().close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(2);
            }
        }
        public String getHostPort() { return hostPort; }
    }

    @State(Scope.Benchmark)
    public static class IterationState {

        @Param({"edu.sjsu.sys.txnlog.HiPriFileOutputStream", "java.io.FileOutputStream"})
        public String fileOutputStreamClass;

        @Param({"1", "100", "1000", "10000"})
        public int outstandingLimit;

        public int outstanding = 0;
        public Object outstandingSynchronizer= new Object();

        MyClientBase mcb;

        public ZooKeeper client;

        public boolean zkRunning;

        @Setup(Level.Trial)
        public void setup() {
            System.err.println("Setting up ZooKeeper");
            Class<? extends FileOutputStream> clazz = null;
            try {
                clazz = Class.forName(fileOutputStreamClass).asSubclass(FileOutputStream.class);
            } catch (ClassNotFoundException e) {
                System.err.println("Could not resolve " + fileOutputStreamClass);
                System.exit(2);
            } catch (ClassCastException e) {
                System.err.println("Could not cast " + fileOutputStreamClass + " to subclass of FileOutputStream: " + e.getMessage());
                System.exit(2);
            }
            try {
                FileTxnLog.fileOutputStreamConstructor = clazz.getConstructor(File.class);
            } catch (NoSuchMethodException e) {
                System.err.println(fileOutputStreamClass + " does not have a public constructor that takes a File object");
                System.exit(2);
            }

            mcb = new MyClientBase();
            mcb.setUp();
            try {
                client = new ZooKeeper(mcb.getHostPort(), 30000,
                        e -> System.err.println(e));
                client.create(znode, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.err.println("Created " + znode + " session " + Long.toHexString(client.getSessionId()));
                zkRunning = true;
            } catch (KeeperException | InterruptedException | IOException e) {
                e.printStackTrace();
                System.exit(2);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            try {
                zkRunning = false;
                System.err.println("Tearing down ZooKeeper");
                mcb.tearDown();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(2);
            }
        }
    }

    static final int opsPerInvoke = 1000;
    /**
     * Latency of setData
     *
     */
    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @OperationsPerInvocation(opsPerInvoke)
    public void benchSetData(IterationState state) throws Exception {
        for (int i = 0; i < opsPerInvoke; i++) {
            synchronized (state.outstandingSynchronizer) {
                while (state.outstanding >= state.outstandingLimit) {
                    state.outstandingSynchronizer.wait();
                }
                state.outstanding += 1;
            }

            state.client.setData(znode, randomBytes, -1, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc != 0 && state.zkRunning) {
                        System.err.println("problem with setData rc=" + rc);
                        System.exit(rc);
                    }
                    synchronized (state.outstandingSynchronizer) {
                        state.outstanding -= 1;
                        state.outstandingSynchronizer.notifyAll();
                    }
                }
            }, null);
        }
    }
}
