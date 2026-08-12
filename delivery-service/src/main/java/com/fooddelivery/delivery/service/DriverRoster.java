package com.fooddelivery.delivery.service;

import com.fooddelivery.delivery.dto.Driver;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Fixed in-memory list of ~6 dummy drivers. In a real system this would be a lookup against a
 * driver-availability service; here it's just enough to make the "DRIVER_ASSIGNED" event feel
 * realistic for the demo.
 */
@Component
public class DriverRoster {

    private static final List<Driver> DRIVERS = List.of(
            new Driver("DRV-1", "Sam Rivera"),
            new Driver("DRV-2", "Jordan Lee"),
            new Driver("DRV-3", "Priya Nair"),
            new Driver("DRV-4", "Marcus Webb"),
            new Driver("DRV-5", "Ana Torres"),
            new Driver("DRV-6", "Kenji Sato")
    );

    /** Picks a uniformly random driver from the fixed roster. */
    public Driver pickRandomDriver() {
        int index = ThreadLocalRandom.current().nextInt(DRIVERS.size());
        return DRIVERS.get(index);
    }
}
