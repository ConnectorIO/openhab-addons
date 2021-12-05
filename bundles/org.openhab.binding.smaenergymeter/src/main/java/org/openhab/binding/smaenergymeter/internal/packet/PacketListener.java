/**
 * Copyright (c) 2010-2021 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.smaenergymeter.internal.packet;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import java.util.concurrent.atomic.AtomicBoolean;
import org.openhab.binding.smaenergymeter.internal.handler.EnergyMeter;
import org.openhab.core.util.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PacketListener} class is responsible for communication with the SMA devices.
 * It handles udp/multicast traffic and broadcast received data to subsequent payload handlers.
 *
 * @author Łukasz Dywicki - Initial contribution
 */
public class PacketListener {

    private final DefaultPacketListenerRegistry registry;
    private final Set<PayloadHandler> handlers = new CopyOnWriteArraySet<>();
    private final AtomicBoolean stop = new AtomicBoolean();

    private Thread receiverThread;
    private String multicastGroup;
    private int port;

    public static final String DEFAULT_MCAST_GRP = "239.12.255.254";
    public static final int DEFAULT_MCAST_PORT = 9522;

    private MulticastSocket socket;

    public PacketListener(DefaultPacketListenerRegistry registry, String multicastGroup, int port) {
        this.registry = registry;
        this.multicastGroup = multicastGroup;
        this.port = port;
    }

    public void addPayloadHandler(PayloadHandler handler) {
        handlers.add(handler);
    }

    public void removePayloadHandler(PayloadHandler handler) {
        handlers.remove(handler);

        if (handlers.isEmpty()) {
            registry.close(multicastGroup, port);
        }
    }

    public boolean isOpen() {
        return socket != null && socket.isConnected();
    }

    public void open() throws IOException {
        if (isOpen()) {
            // no need to bind socket second time
            return;
        }
        socket = new MulticastSocket(port);
        socket.setSoTimeout(5000);
        InetAddress address = InetAddress.getByName(multicastGroup);
        socket.joinGroup(address);

        this.receiverThread = new Thread(new ReceivingTask(socket, multicastGroup + ":" + port, handlers, stop), "smaenergymeter-receiver-" + multicastGroup + ":" + port);
        this.receiverThread.setDaemon(true);
        this.receiverThread.start();
    }

    void close() throws IOException {
        stop.set(true);
        InetAddress address = InetAddress.getByName(multicastGroup);
        socket.leaveGroup(address);
        socket.close();
    }

    static class ReceivingTask implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ReceivingTask.class);
        private final DatagramSocket socket;
        private final String group;
        private final Set<PayloadHandler> handlers;
        private final AtomicBoolean stop;

        ReceivingTask(DatagramSocket socket, String group, Set<PayloadHandler> handlers, AtomicBoolean stop) {
            this.socket = socket;
            this.group = group;
            this.handlers = handlers;
            this.stop = stop;
        }

        public void run() {
            try {
                byte[] bytes = new byte[608];
                while (!stop.get()) {
                    DatagramPacket msgPacket = new DatagramPacket(bytes, bytes.length);
                    socket.receive(msgPacket);

                    try {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Received frame {}", HexUtils.bytesToHex(bytes));
                        }

                        EnergyMeter meter = new EnergyMeter();
                        meter.parse(bytes);

                        for (PayloadHandler handler : handlers) {
                            handler.handle(meter);
                        }
                    } catch (IOException e) {
                        logger.debug("Unexpected payload received for group {}", group, e);
                    }
                }
            } catch (IOException e) {
                logger.warn("Failed to receive data for multicast group {}", group, e);
            }
        }
    }

}
