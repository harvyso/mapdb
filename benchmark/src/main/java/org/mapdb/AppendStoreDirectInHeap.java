package org.mapdb;

public class AppendStoreDirectInHeap extends AppendBenchmark {

  @Override
  protected Store initialize() {
    return new StoreDirect(
        null,
        CC.DEFAULT_MEMORY_VOLUME_FACTORY,
        false,
        0L,
        true,
        CC.STORE_DIRECT_CONC_SHIFT,
        CC.PAGE_SIZE,
        0L,
        false,
        false,
        false,
        true,
        false
    );
  }
}
