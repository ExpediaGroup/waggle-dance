package com.hotels.bdp.waggledance.metrics;

public enum MetastoreAvailabilityMetric {

  IS_AVAILABLE(0),
  IS_UNAVAILABLE(1),
  IS_UNKNOWN(2);

  private final int availabilityValue;

  MetastoreAvailabilityMetric(int availabilityValue) {
    this.availabilityValue = availabilityValue;
  }

  public int getValue() {
    return availabilityValue;
  }
}
