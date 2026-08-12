package com.fooddelivery.kitchen.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds the {@code kitchen.*} properties from application.yml (overridable via env vars,
 * e.g. KITCHEN_PREPSTARTDELAYMS is NOT the relaxed-binding form Spring uses - use
 * KITCHEN_PREP_START_DELAY_MS style env vars, or just edit application.yml).
 *
 * <p>These control the timing of the demo's simulated kitchen prep pipeline. They are
 * intentionally small (seconds, not real minutes) so a full order saga completes quickly
 * during a live demo, while {@link #maxPrepMinutes} still reports a realistic-looking
 * "estimated minutes" value to the customer-facing event payload.
 */
@ConfigurationProperties(prefix = "kitchen")
public class KitchenProperties {

    /** Delay (ms) between ORDER_RECEIVED and the PREPARING transition. */
    private long prepStartDelayMs = 2000;

    /** Delay (ms) between the PREPARING transition and the PREPARED transition. */
    private long prepDurationMs = 5000;

    /** Lower bound (inclusive) for the randomly generated estimatedMinutes field. */
    private int minPrepMinutes = 10;

    /** Upper bound (inclusive) for the randomly generated estimatedMinutes field. */
    private int maxPrepMinutes = 25;

    public long getPrepStartDelayMs() {
        return prepStartDelayMs;
    }

    public void setPrepStartDelayMs(long prepStartDelayMs) {
        this.prepStartDelayMs = prepStartDelayMs;
    }

    public long getPrepDurationMs() {
        return prepDurationMs;
    }

    public void setPrepDurationMs(long prepDurationMs) {
        this.prepDurationMs = prepDurationMs;
    }

    public int getMinPrepMinutes() {
        return minPrepMinutes;
    }

    public void setMinPrepMinutes(int minPrepMinutes) {
        this.minPrepMinutes = minPrepMinutes;
    }

    public int getMaxPrepMinutes() {
        return maxPrepMinutes;
    }

    public void setMaxPrepMinutes(int maxPrepMinutes) {
        this.maxPrepMinutes = maxPrepMinutes;
    }
}
