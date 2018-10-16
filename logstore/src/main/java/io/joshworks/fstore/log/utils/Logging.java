package io.joshworks.fstore.log.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Logging {
    private Logging() {

    }

    public static Logger namedLogger(String logName, String subsystemName) {
        return LoggerFactory.getLogger(subsystemName + " [" + logName + "]");
    }

}
