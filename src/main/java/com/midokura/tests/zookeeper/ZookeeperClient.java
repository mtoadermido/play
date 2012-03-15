/*
 * Copyright 2012 Midokura Europe SARL
 */
package com.midokura.tests.zookeeper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import com.google.code.guava.beans.Introspection;
import com.google.code.guava.beans.Properties;
import com.google.code.guava.beans.Property;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // TODO: Explain yourself
 *
 * @author Mihai Claudiu Toader <mtoader@midokura.com>
 *         Date: 3/13/12
 */
public class ZookeeperClient {
    private final static Logger log =
        LoggerFactory.getLogger(ZookeeperClient.class);

    public static final ACL defaultAcl = new ACL(ZooDefs.Perms.ALL,
                                                 ZooDefs.Ids.ANYONE_ID_UNSAFE);

    ZooKeeper zooKeeper;
    boolean connected;
    Thread connectionThread;


    private final String connection;
    private final int timeout;

    public ZookeeperClient(String connection, int timeout) {
        this.connection = connection;
        this.timeout = timeout;
    }

    public void makePersistentPath(String path)
        throws InterruptedException, KeeperException {

        zooKeeper.create(path, new byte[]{},
                         Lists.newArrayList(defaultAcl),
                         CreateMode.PERSISTENT);
    }

    public void connect() throws IOException {
        connected = false;
        connectionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    openConnection();
                } catch (IOException e) {
                    log.debug("Connection error: ", e);
                }
            }
        });

        connectionThread.start();

        while (!connected && connectionThread.isAlive()) {
            synchronized (this) {
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }
    }

    private void openConnection() throws IOException {
        zooKeeper = new ZooKeeper(connection, timeout, new Watcher());
    }

    public void clearTree(String path)
        throws InterruptedException, KeeperException {

        List<Op> multiOperation = new ArrayList<Op>();

        foldUpTreePath(multiOperation, path, new Function<String, Op>() {
            @Override
            public Op apply(@Nullable String input) {
                return Op.delete(input, -1);
            }
        });

        zooKeeper.multi(multiOperation);
    }

    private <V> List<V> foldUpTreePath(List<V> target, String path,
                                       Function<String, V> transformer)
        throws InterruptedException, KeeperException {

        if (zooKeeper.exists(path, null) != null) {
            List<String> children = zooKeeper.getChildren(path, null);
            for (String child : children) {
                foldUpTreePath(target, path + "/" + child, transformer);
            }

            target.add(transformer.apply(path));
        }

        return target;
    }

    public void makeEphemeralNode(String path)
        throws InterruptedException, KeeperException {
        zooKeeper.create(path, new byte[0], Lists.newArrayList(defaultAcl),
                         CreateMode.EPHEMERAL);
    }

    public void deleteNodes(String ... paths)
        throws InterruptedException, KeeperException {
        List<Op> deletes = new ArrayList<Op>();

        for (String path : paths) {
            deletes.add(Op.delete(path, -1));
        }

        zooKeeper.multi(deletes);
    }

    public void makePersistentPaths(String ... nodes)
        throws InterruptedException, KeeperException {
        List<Op> multiCreate = new ArrayList<Op>();
        for (String node : nodes) {
            multiCreate.add(Op.create(node, null, Lists.newArrayList(defaultAcl), CreateMode.PERSISTENT));
        }

        zooKeeper.multi(multiCreate);
    }

    public void existsWithDefaultWatcher(String zookeeperRootNode)
        throws InterruptedException, KeeperException {
        zooKeeper.exists(zookeeperRootNode, new org.apache.zookeeper.Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.debug("Watcher called: {}", event);
            }
        });
    }

    class Watcher implements org.apache.zookeeper.Watcher {

        @Override
        public void process(WatchedEvent event) {
            switch (event.getType()) {
                case None:
                    processConnectionChangedEvent(event);
                    synchronized (ZookeeperClient.this) {
                        ZookeeperClient.this.notifyAll();
                    }
                    break;
                default:
                    processDataRelatedEvent(event);
                    break;
            }
        }

        private void processDataRelatedEvent(WatchedEvent event) {
            log.debug("Data event: {}", event);
        }

        private void processConnectionChangedEvent(WatchedEvent event) {
            log.debug("Connection event: {}", event);
            switch (event.getState()) {
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    connected = false;
                    break;
                case SyncConnected:
                    connected = true;
                    break;
            }
        }
    }
}
