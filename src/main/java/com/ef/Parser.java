package com.ef;

import com.ef.constants.Duration;
import com.ef.dbinstance.MySQLEmbedded;
import com.ef.model.ActiveIP;
import com.ef.service.ValidationService;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Parser {
    private static MySQLEmbedded mySQLEmbedded = MySQLEmbedded.startEbmeddedMySQLServer();
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss");

    public static void main(String[] args) {
        ValidationService validationService = new ValidationService();
        String logFilePath = Thread.currentThread().getContextClassLoader().getResource("access.log").getPath();
        mySQLEmbedded.uploadMainLogToDB(logFilePath, "MAIN_LOG");
        validationService.compareRowsInFileAndDB(logFilePath, "MAIN_LOG");
        List<ActiveIP> activeIPs = mySQLEmbedded.findActiveIPForPeriod(LocalDateTime.parse("2017-01-01.00:00:00", formatter), Duration.DAILY, 500);
        mySQLEmbedded.uploadActiveIpToDB(activeIPs, "IP_ACTIVITY");
    }
}
