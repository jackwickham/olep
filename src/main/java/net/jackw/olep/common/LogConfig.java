package net.jackw.olep.common;

import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

public class LogConfig {
    public static final Marker TRANSACTION_PROCESSING_MARKER = MarkerManager.getMarker("TRANSACTION_PROCESSING");
    public static final Marker TRANSACTION_ID_MARKER = MarkerManager.getMarker("TRANSACTION_ID").addParents(TRANSACTION_PROCESSING_MARKER);
    public static final Marker TRANSACTION_DONE_MARKER = MarkerManager.getMarker("TRANSACTION_DONE").addParents(TRANSACTION_PROCESSING_MARKER);

    private LogConfig() { }
}
