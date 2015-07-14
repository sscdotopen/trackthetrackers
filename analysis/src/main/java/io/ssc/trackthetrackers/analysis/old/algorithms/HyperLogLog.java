package io.ssc.trackthetrackers.analysis.old.algorithms;

/** a HyperLogLog sketch. */
public class HyperLogLog {

  private static final int NUMBER_OF_BUCKETS = 16;
  private static final double ALPHA_TIMES_MSQUARED = 0.673 * NUMBER_OF_BUCKETS * NUMBER_OF_BUCKETS;
  private static final double ESTIMATION_THRESHOLD = 2.5 * NUMBER_OF_BUCKETS;

  //TODO should be configurable
  private static final int SEED = 0xbeef;

  private byte[] buckets;
  
  public HyperLogLog() {
    this.buckets = new byte[NUMBER_OF_BUCKETS];
  }

  public void observe(long item) {
    setRegister(MurmurHash3.hash(item, SEED));
  }

  private void setRegister(int hash) {
    // last 4 bits as bucket index
    int mask = NUMBER_OF_BUCKETS - 1;
    int bucketIndex = hash & mask;

    // throw away last 4 bits
    hash >>= 4;
    // make sure the 4 new zeroes don't impact estimate
    hash |= 0xf0000000;
    // hash has now 28 significant bits left
    buckets[bucketIndex] = (byte) (Integer.numberOfTrailingZeros(hash) + 1L);
  }

  public long count() {

    double sum = 0.0;
    for (byte bucket : buckets) {
      sum += Math.pow(2.0, -bucket);
    }
    long estimate = (long) (ALPHA_TIMES_MSQUARED * (1.0 / sum));

    if (estimate < ESTIMATION_THRESHOLD) {
      /* look for empty buckets */
      int numEmptyBuckets = 0;
      for (byte bucket : buckets) {
        if (bucket == 0) {
          numEmptyBuckets++;
        }
      }

      if (numEmptyBuckets != 0) {
        estimate = (long) (NUMBER_OF_BUCKETS * Math.log((double) NUMBER_OF_BUCKETS / (double) numEmptyBuckets));
      }
    }

    return estimate;
  }
  
  public void merge(HyperLogLog other) {
    /* take the maximum of each bucket pair */
    for (int index = 0; index < buckets.length; index++) {
      if (buckets[index] < other.buckets[index]) {
        buckets[index] = other.buckets[index];
      }
    }
  }

  @Override
  public String toString() {
    return String.valueOf(count());
  }
}
