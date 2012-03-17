/*
 * Copyright 2012 Midokura Europe SARL
 */
package com.midokura.tests.zookeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import static java.lang.String.format;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // TODO: Explain yourself
 *
 * @author Mihai Claudiu Toader <mtoader@midokura.com>
 *         Date: 3/13/12
 */
public class BlockingBug {

    private final static Logger log =
        LoggerFactory.getLogger(BlockingBug.class);

    public static void main(String[] args)
        throws Exception, KeeperException {

        ZookeeperLauncher zookeeper = ZookeeperLauncher.launch();
        // start zookeeper

        // start first client (the one that acts as the mgmt api)
        ZookeeperClient zkClient =
            new ZookeeperClient(ZooKeeper.HOST_ADDRESS,
                                ZooKeeper.ZOOKEEPER_CLIENT_TIMEOUT);
        zkClient.connect();

        log.debug("Client connected .. making the nodes");

        zkClient.clearTree(ZooKeeper.ZOOKEEPER_ROOT_NODE);
        zkClient.makePersistentPaths(
            ZooKeeper.ZOOKEEPER_ROOT_NODE,
            format("%s/%s",
                   ZooKeeper.ZOOKEEPER_ROOT_NODE,
                   ZooKeeper.ZOOKEEPER_EPHEMERALS));

        zkClient.makePersistentPath(ZooKeeper.ZOOKEEPER_ROOT_NODE + "/test");
        zkClient.deleteNodes(ZooKeeper.ZOOKEEPER_ROOT_NODE + "/test");

        log.debug("Connecting the ephemeral client");

        ExternalZooKeeperClient ephemeralClient = null;
        // start the second client here
        try {
            ephemeralClient = ExternalZooKeeperClient.launch(
                ZooKeeper.HOST_ADDRESS,
//                ZooKeeper.ZOOKEEPER_CLIENT_TIMEOUT);
                250);
        } catch (IOException e) {
            log.error("Client failed to launch properly", e);
        }

        // give a chance to the client to create the ephemeral node
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        log.debug("Clearing the node while ignoring the ephemeral node");

        // try to delete partial data
        // expect to fail

        boolean forceDeletionError = true;

        if ( forceDeletionError ) {
            try {
                zkClient.deleteNodes(
                    format("%s/%s", ZooKeeper.ZOOKEEPER_ROOT_NODE,
                           ZooKeeper.ZOOKEEPER_EPHEMERALS),
                    ZooKeeper.ZOOKEEPER_ROOT_NODE);
                log.debug("It shouldn't have passed !! ");
            } catch (InterruptedException e) {
                //
            } catch (KeeperException e) {
                log.debug("Exception thrown as expected");
            }
        }

        // kill the client
        log.debug("Destroying client");
        if (ephemeralClient != null) {
            ephemeralClient.destroyHard();
//            ephemeralClient.destroySoft();
        }

        log.debug("Try to let the ephemeral timeout expire");
        Thread.sleep(20*1000);

        // try to delete and expect to fail
        log.debug("Trying to clean the root tree");
        try {
            zkClient.clearTree(ZooKeeper.ZOOKEEPER_ROOT_NODE);
            log.debug("No error :(");
        } catch (InterruptedException e) {
            //
        } catch (KeeperException e) {
            log.debug("We hit it !. The clear tree failed. ");
        }

        Thread.sleep(1000);
        zookeeper.destroy();
    }
}
