package com.ef;

import com.ef.dbinstance.MySQLEmbedded;
import com.ef.model.ActiveIP;
import com.ef.service.ValidationService;
import org.apache.commons.cli.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Scanner;

public class Parser {
    private static MySQLEmbedded mySQLEmbedded = MySQLEmbedded.startEbmeddedMySQLServer();
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss");

    public static void main(String[] args) throws ParseException {
        ValidationService validationService = new ValidationService();
        Options options = buildCmdArgs();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        validationService.validateCommandLineArgs(cmd);
        mySQLEmbedded.uploadMainLogToDB(cmd.getOptionValue("a"), "MAIN_LOG");
        validationService.compareRowsInFileAndDB(cmd.getOptionValue("a"), "MAIN_LOG");
        List<ActiveIP> activeIPs = mySQLEmbedded.findActiveIPForPeriod(LocalDateTime.parse(cmd.getOptionValue("s"), formatter),
                cmd.getOptionValue("d"), Integer.valueOf(cmd.getOptionValue("t")));
        mySQLEmbedded.uploadActiveIpToDB(activeIPs, "IP_ACTIVITY");
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please check result in DB before exit from this application. For exit please press enter...");
        scanner.nextLine();
    }

    private static Options buildCmdArgs() {
        Options options = new Options();
        options.addOption("a", "accesslog", true, "Path to uploading log file to DB");
        options.addOption("s", "startDate", true, "Find request from start date");
        options.addOption("d", "duration", true, "Specify duration for filter requests. Parameter could have 'hourly' or 'daily' values");
        options.addOption("t", "threshold", true, "Specify count requests");

        return options;
    }
}