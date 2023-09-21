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

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LatencyStandaloneBench {
    static public final byte[] randomBytes = new byte[4096];
    static {
        new Random().nextBytes(randomBytes);
    }

    static final String znode = "/bench";
    static boolean backgroundWrites;

        String hostPort = "127.0.0.1:22334";
        ServerCnxnFactory factory;
        ZooKeeperServer zks;
        File dataDir = new File("zkbenchdata");
        volatile boolean running;
        Thread backgroundWriter;

        public void setUp() {
            try {
                recursiveDelete(dataDir);
                dataDir.mkdirs();
                factory = ServerCnxnFactory.createFactory(22334, 100);
                zks = new ZooKeeperServer(dataDir, dataDir, 3000);
                zks.setCreateSessionTrackerServerId(1);
                factory.startup(zks);
                running = true;
                if (backgroundWrites) {
                    backgroundWriter = new Thread(() -> {
                        try {
                            RandomAccessFile raf = new RandomAccessFile("background.dat", "w");
                            while (running) {
                                raf.getFD().sync();
                                raf.seek(0);
                                // 536MiB writes
                                for (int i = 0; running && i < 134*1024; i++) {
                                    raf.write(randomBytes);
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
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
                running = false;
                factory.shutdown();
                zks.getZKDatabase().close();
                if (backgroundWrites) {
                    backgroundWriter.join();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(2);
            }
        }

        public String[] fileOutputStreamClasses = {"edu.sjsu.sys.txnlog.HiPriFileOutputStream", "java.io" +
                ".FileOutputStream"};

        public int[] outstandingLimits = {1, 100, 1000, 10000};

        public int outstanding = 0;
        public Object outstandingSynchronizer= new Object();

        public ZooKeeper client;

        public boolean zkRunning;

        public void trialSetup(String fileOutputStreamClass) {
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

            try {
                setUp();
                client = new ZooKeeper(hostPort, 30000, e -> System.err.println(e));
                client.create(znode, "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.err.println("Created " + znode + " session " + Long.toHexString(client.getSessionId()));
                zkRunning = true;
            } catch (KeeperException | InterruptedException | IOException e) {
                e.printStackTrace();
                System.exit(2);
            }
        }

        public void trialTearDown() {
            try {
                zkRunning = false;
                client.close(1000);
                System.err.println("Tearing down ZooKeeper");
                tearDown();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(2);
            }
        }

        final static int percentilesShown[] = {1, 5, 10, 50, 90, 95, 99, 100};
    public Percentile bench(int round, String fileOutputStreamClass, int count, int outstandingLimit) throws Exception {
        trialSetup(fileOutputStreamClass);
        List<Double> duration = Collections.synchronizedList(new ArrayList<Double>());
        Semaphore outstandingSem = new Semaphore(outstandingLimit);
        for (int i = 0; i < count; i++) {
            outstandingSem.acquire();
            client.setData(znode, randomBytes, -1, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (rc != 0 && zkRunning) {
                        System.err.println("problem with setData rc=" + rc);
                        System.exit(rc);
                    }
                    duration.add((double)(System.nanoTime() - (Long)ctx));
                    outstandingSem.release();
                }
            }, System.nanoTime());
        }
        trialTearDown();
        Percentile percentile = new Percentile();
        duration.sort(Double::compareTo);
        percentile.setData(duration.stream().mapToDouble(Double::doubleValue).toArray());
        return percentile;
    }

    static class Record {
        String fileOutputStreamClass;
        int round;
        int outstanding;
        Double[] percentiles;
        Record(String fileOutputStreamClass, int round, int outstanding, Double[] percentiles) {
            this.fileOutputStreamClass = fileOutputStreamClass;
            this.round = round;
            this.outstanding = outstanding;
            this.percentiles = percentiles;
        }
    }
    public static void main(String args[]) throws Exception {
        if (args.length != 3) {
            System.out.println("USAGE: LatencyStandaloneBench rounds iterations background_writes");
            System.exit(1);
        }
        int rounds = Integer.parseInt(args[0]);
        int iterations = Integer.parseInt(args[1]);
        if (args[2].equals("true")) {
            backgroundWrites = true;
            System.out.println("doing background_writes");
        } else if (args[2].equals("false")) {
            backgroundWrites = false;
            System.out.println("NOT doing background_writes");
        } else {
            System.out.println("background_writes must be either true or false. not " + args[2]);
            System.exit(1);
        }
        LatencyStandaloneBench benchmark = new LatencyStandaloneBench();
        ArrayList<Record> records = new ArrayList<>();
        for (int round = 0; round < rounds; round++) {
            for (String fileOutputStreamClass: benchmark.fileOutputStreamClasses) {
                for (int outstandingLimit: benchmark.outstandingLimits) {
                    Percentile p = benchmark.bench(round, fileOutputStreamClass, iterations, outstandingLimit);
                    ArrayList<Double> percentiles = new ArrayList<>();
                    for (int percentile: percentilesShown) {
                        percentiles.add(p.evaluate(percentile));
                    }
                    records.add(new Record(fileOutputStreamClass, round, outstandingLimit,
                            percentiles.toArray(new Double[0])));
                }
            }
        }
        for (Record r: records) {
            System.out.printf("%d %s %d ", r.round, r.fileOutputStreamClass, r.outstanding);
            for (double p: r.percentiles) {
                System.out.printf("%.2f ", p/1000000);
            }
            System.out.println();
        }
    }
}
