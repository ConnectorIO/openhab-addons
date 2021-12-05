/**
 * Copyright (c) 2010-2024 Contributors to the openHAB project
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
import java.util.Map;

import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.smaenergymeter.internal.configuration.EnergyMeterConfig;
import org.openhab.binding.smaenergymeter.internal.packet.PacketListener;
import org.openhab.binding.smaenergymeter.internal.packet.PacketListenerRegistry;
import org.openhab.binding.smaenergymeter.internal.packet.PayloadHandler;
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
public class SMAEnergyMeterHandler extends BaseThingHandler implements PayloadHandler {

    private final Logger logger = LoggerFactory.getLogger(SMAEnergyMeterHandler.class);
    private final PacketListenerRegistry listenerRegistry;
    private @Nullable PacketListener listener;
    private @Nullable String serialNumber;

    public SMAEnergyMeterHandler(Thing thing, PacketListenerRegistry listenerRegistry) {
        super(thing);
        this.listenerRegistry = listenerRegistry;
    }

    @Override
    public void handleCommand(ChannelUID channelUID, Command command) {
        if (command == RefreshType.REFRESH) {
            logger.debug("Refreshing {}", channelUID);
            if (listener != null) {
                listener.request();
            }
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
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_PENDING,
                        "Meter serial number missing");
                return;
            }

            logger.debug("Activated handler for SMA Energy Meter with S/N '{}'", serialNumber);

            int pollingPeriod = (config.getPollingPeriod() == null) ? 30 : config.getPollingPeriod();
            listener.addPayloadHandler(this);
            listener.open(pollingPeriod);
            logger.debug("Polling job scheduled to run every {} sec. for '{}'", pollingPeriod, getThing().getUID());
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
    }

    @Override
    public void handle(EnergyMeter energyMeter) {
        if (serialNumber == null || !serialNumber.equals(energyMeter.getSerialNumber())) {
            return;
        }
        logger.debug("Update SMAEnergyMeter {} data '{}'", serialNumber, getThing().getUID());

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

        updateStatus(ThingStatus.ONLINE);
    }

    @Nullable
    private static String retrieveSerialNumber(Map<String, String> properties) {
        String property = properties.get(Thing.PROPERTY_SERIAL_NUMBER);
        if (property == null) {
            return null;
        }
        try {
            // old format in decimal form
            int identifier = Integer.parseInt(property);
            return Integer.toHexString(identifier);
        } catch (NumberFormatException e) {
            // hex form for newly discovered meters
            return property;
        }
    }
}
