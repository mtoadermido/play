/*
 * Copyright 2012 Midokura Europe SARL
 */
package com.midokura.tests.zookeeper;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import static java.lang.String.format;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * // TODO: Explain yourself
 *
 * @author Mihai Claudiu Toader <mtoader@midokura.com>
 *         Date: 3/14/12
 */
public class ExternalZooKeeperClient {

    private final static Logger log =
        LoggerFactory.getLogger(ExternalZooKeeperClient.class);

    private final String hostAddress;
    private final int zkClientTimeout;
    private Process clientProcess;

    public ExternalZooKeeperClient(String hostAddress, int zkClientTimeout) {
        this.hostAddress = hostAddress;
        this.zkClientTimeout = zkClientTimeout;
    }

    public static ExternalZooKeeperClient launch(String hostAddress,
                                                 int zkClientTimeout)
        throws IOException {
        ExternalZooKeeperClient client =
            new ExternalZooKeeperClient(hostAddress, zkClientTimeout);

        client.launchHimself();
        return client;
    }

    private void launchHimself() throws IOException {
        String commandLine =
            String.format("java -cp %s %s %s %d",
                          buildClassPath(),
                          ExternalZooKeeperClient.class.getCanonicalName(),
                          hostAddress,
                          zkClientTimeout);

        log.debug("Client command line: {}", commandLine);

        clientProcess =
            new ProcessBuilder()
                .command(Lists.newArrayList(
                    Splitter.onPattern("[ \t]+").split(commandLine)))
                .redirectErrorStream(true)
                .start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    InputSupplier<InputStreamReader> threadInputSupplier = new InputSupplier<InputStreamReader>() {
                        @Override
                        public InputStreamReader getInput()
                            throws IOException {
                            return new InputStreamReader(
                                clientProcess.getInputStream());
                        }
                    };

                    LineProcessor<Object> logDumper = new LineProcessor<Object>() {
                        @Override
                        public boolean processLine(String line)
                            throws IOException {
                            log.debug("<zookeeper_client>: {}", line);
                            return true;
                        }

                        @Override
                        public Object getResult() {
                            return null;
                        }
                    };


                    CharStreams.readLines(threadInputSupplier, logDumper);
                } catch (IOException e) {
                    log.error("Exception while reading zookeeper stdout.", e);
                }
            }
        }).start();
    }

    private String buildClassPath() throws IOException {
        return
            new File("target/classes").getAbsolutePath() + ":" +
                Resources.toString(
                    Resources.getResource(ExternalZooKeeperClient.class,
                                          "/classpath.properties"),
                    Charsets.UTF_8);
    }

    public static void main(String[] args)
        throws IOException, InterruptedException, KeeperException {

        ZookeeperClient client =
            new ZookeeperClient(args[0],
                                Integer.parseInt(args[1]));

        client.connect();

        log.debug("Ephemeral Client connected to server");

        client.makeEphemeralNode(
            format("%s/%s/%s",
                   ZooKeeper.ZOOKEEPER_ROOT_NODE,
                   ZooKeeper.ZOOKEEPER_EPHEMERALS,
                   ZooKeeper.ZOOKEEPER_CLIENT_EPHEMERAL));

        log.debug("Ephemeral Client node created");

//        client.existsWithDefaultWatcher(ZooKeeper.ZOOKEEPER_ROOT_NODE);
        Thread.sleep(TimeUnit.SECONDS.toMillis(50));
    }

    public void destroyHard() {
        try {
            Object pid = getField(clientProcess, "pid");

            if (pid != null && pid instanceof Integer) {
                Integer pidAsInteger = (Integer) pid;

                Process killProcess = new ProcessBuilder()
                    .command("kill", "-9", "" + pidAsInteger)
                    .start();

                killProcess.waitFor();
                clientProcess.waitFor();
            }
        } catch (Exception e) {
            log.error("Error while killing the ephemeral client", e);
        }
    }

    private Object getField(Object object, String fieldName)
        throws NoSuchFieldException, IllegalAccessException {

        Field field = object.getClass().getDeclaredField(fieldName);

        field.setAccessible(true);
        return field.get(object);

    }

    public void destroySoft() {
        clientProcess.destroy();
        try {
            clientProcess.waitFor();
        } catch (InterruptedException e) {
            //
        }
    }
}
