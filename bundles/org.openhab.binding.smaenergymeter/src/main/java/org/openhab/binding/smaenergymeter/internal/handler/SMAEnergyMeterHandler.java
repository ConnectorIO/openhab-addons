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
package org.openhab.binding.smaenergymeter.internal.handler;

import static org.openhab.binding.smaenergymeter.internal.SMAEnergyMeterBindingConstants.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.smaenergymeter.internal.configuration.EnergyMeterConfig;
import org.openhab.binding.smaenergymeter.internal.packet.PacketListener;
import org.openhab.binding.smaenergymeter.internal.packet.PacketListenerRegistry;
import org.openhab.binding.smaenergymeter.internal.packet.PayloadHandler;
import org.openhab.core.config.core.Configuration;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.thing.Thing;
import org.openhab.core.thing.ThingStatus;
import org.openhab.core.thing.ThingStatusDetail;
import org.openhab.core.thing.binding.BaseThingHandler;
import org.openhab.core.types.Command;
import org.openhab.core.types.RefreshType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SMAEnergyMeterHandler} is responsible for handling commands, which are
 * sent to one of the channels.
 *
 * @author Osman Basha - Initial contribution
 */
public class SMAEnergyMeterHandler extends BaseThingHandler implements PayloadHandler, Runnable {

    private final Logger logger = LoggerFactory.getLogger(SMAEnergyMeterHandler.class);
    private final PacketListenerRegistry listenerRegistry;
    private final AtomicBoolean refresh = new AtomicBoolean(false);
    private @Nullable PacketListener listener;
    private @Nullable Long serialNumber;
    private long updateInterval;
    private long lastUpdate;
    private ScheduledFuture<?> timeoutFuture;

    public SMAEnergyMeterHandler(Thing thing, PacketListenerRegistry listenerRegistry) {
        super(thing);
        this.listenerRegistry = listenerRegistry;
    }

    @Override
    public void handleCommand(ChannelUID channelUID, Command command) {
        if (command == RefreshType.REFRESH) {
            logger.debug("Refreshing {}", channelUID);
            refresh.set(true);
        } else {
            logger.warn("This binding is a read-only binding and cannot handle commands");
        }
    }

    @Override
    public void initialize() {
        logger.debug("Initializing SMAEnergyMeter handler '{}'", getThing().getUID());

        EnergyMeterConfig config = getConfigAs(EnergyMeterConfig.class);

        int port = (config.getPort() == null) ? PacketListener.DEFAULT_MCAST_PORT : config.getPort();

        try {
            listener = listenerRegistry.getListener(config.getMcastGroup(), port);
            serialNumber = config.getSerialNumber();
            if (serialNumber == null) {
                if (!thing.getProperties().containsKey(Thing.PROPERTY_SERIAL_NUMBER)
                        || (serialNumber = retrieveSerialNumber(thing.getProperties())) == null) {
                    updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_PENDING,
                            "Meter serial number missing");
                    return;
                }

                // copy serial number from thing properties into config
                Map<String, Object> newConfig = editConfiguration().getProperties();
                newConfig.put("serialNumber", serialNumber);
                Thing thing = editThing().withConfiguration(new Configuration(newConfig)).build();
                updateThing(thing);
                updateStatus(ThingStatus.OFFLINE);
                return;
            } else {
                if (!thing.getProperties().containsKey(Thing.PROPERTY_SERIAL_NUMBER)) {
                    Map<String, String> props = editProperties();
                    props.put(Thing.PROPERTY_VENDOR, "SMA");
                    props.put(Thing.PROPERTY_SERIAL_NUMBER, "" + serialNumber);
                    updateProperties(props);
                }
            }

            logger.debug("Activated handler for SMA Energy Meter with S/N '{}'", serialNumber);

            this.updateInterval = TimeUnit.SECONDS
                    .toMillis(config.getPollingPeriod() == null ? 30 : config.getPollingPeriod());
            timeoutFuture = scheduler.scheduleWithFixedDelay(this, updateInterval * 2, updateInterval,
                    TimeUnit.MILLISECONDS);
            listener.addPayloadHandler(this);
            listener.open();
            logger.debug("Listening to meter updates and publishing its measurements every {}ms for '{}'",
                    updateInterval, getThing().getUID());
            // we do not set online status here, it will be set only when data is received
        } catch (IOException e) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.CONFIGURATION_ERROR, e.getMessage());
        }
    }

    @Override
    public void dispose() {
        logger.debug("Disposing SMAEnergyMeter handler '{}'", getThing().getUID());

        if (listener != null) {
            listener.removePayloadHandler(this);
        }
        if (timeoutFuture != null) {
            timeoutFuture.cancel(true);
        }
    }

    @Override
    public void handle(EnergyMeter energyMeter) {
        if (serialNumber == null || !serialNumber.equals(energyMeter.getSerialNumber())) {
            return;
        }

        long currentUpdate = System.currentTimeMillis();
        if (updateInterval > currentUpdate - lastUpdate) {
            logger.trace("Update is to early {}, waiting to {}", currentUpdate - lastUpdate, updateInterval);
            if (!refresh.get()) {
                return;
            }
        }
        lastUpdate = currentUpdate;

        logger.debug("Update SMAEnergyMeter {} data '{}'", serialNumber, getThing().getUID());
        updateStatus(ThingStatus.ONLINE);

        updateState(CHANNEL_POWER_IN, energyMeter.getPowerIn());
        updateState(CHANNEL_POWER_OUT, energyMeter.getPowerOut());
        updateState(CHANNEL_ENERGY_IN, energyMeter.getEnergyIn());
        updateState(CHANNEL_ENERGY_OUT, energyMeter.getEnergyOut());

        updateState(CHANNEL_POWER_IN_L1, energyMeter.getPowerInL1());
        updateState(CHANNEL_POWER_OUT_L1, energyMeter.getPowerOutL1());
        updateState(CHANNEL_ENERGY_IN_L1, energyMeter.getEnergyInL1());
        updateState(CHANNEL_ENERGY_OUT_L1, energyMeter.getEnergyOutL1());

        updateState(CHANNEL_POWER_IN_L2, energyMeter.getPowerInL2());
        updateState(CHANNEL_POWER_OUT_L2, energyMeter.getPowerOutL2());
        updateState(CHANNEL_ENERGY_IN_L2, energyMeter.getEnergyInL2());
        updateState(CHANNEL_ENERGY_OUT_L2, energyMeter.getEnergyOutL2());

        updateState(CHANNEL_POWER_IN_L3, energyMeter.getPowerInL3());
        updateState(CHANNEL_POWER_OUT_L3, energyMeter.getPowerOutL3());
        updateState(CHANNEL_ENERGY_IN_L3, energyMeter.getEnergyInL3());
        updateState(CHANNEL_ENERGY_OUT_L3, energyMeter.getEnergyOutL3());

        refresh.set(false);
    }

    @Override
    public void run() {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - lastUpdate > updateInterval * 2) {
            long missedWindow = TimeUnit.MILLISECONDS.toSeconds((updateInterval * 2));
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.OFFLINE.GONE,
                    "No update received within " + missedWindow + " seconds");
        }
    }

    @Nullable
    private static Long retrieveSerialNumber(Map<String, String> properties) {
        String property = properties.get(Thing.PROPERTY_SERIAL_NUMBER);
        if (property == null) {
            return null;
        }
        try {
            // old format in decimal form
            return Long.parseLong(property);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
