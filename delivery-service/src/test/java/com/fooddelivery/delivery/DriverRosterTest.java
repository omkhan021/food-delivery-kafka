package com.fooddelivery.delivery;

import com.fooddelivery.delivery.dto.Driver;
import com.fooddelivery.delivery.service.DriverRoster;
import org.junit.jupiter.api.RepeatedTest;

import static org.assertj.core.api.Assertions.assertThat;

class DriverRosterTest {

    private final DriverRoster roster = new DriverRoster();

    @RepeatedTest(20)
    void pickRandomDriverAlwaysReturnsAValidRosterEntry() {
        Driver driver = roster.pickRandomDriver();

        assertThat(driver).isNotNull();
        assertThat(driver.id()).matches("DRV-[1-6]");
        assertThat(driver.name()).isNotBlank();
    }
}
