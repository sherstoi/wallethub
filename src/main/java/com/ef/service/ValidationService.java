package com.ef.service;

import com.ef.constants.Constant;
import com.ef.dbinstance.MySQLEmbedded;
import com.ef.exception.DifferentRowCountInFileAndDBException;
import com.ef.exception.IncorrectColumnCountInFileException;
import com.ef.utils.FSUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;

/**
 * Use this class for validate log file before uploading to DB.
 *
 */
public class ValidationService {

    public static boolean isFileContainCorrectColumnNumber(String filePath, int columnCount)
            throws IOException, IncorrectColumnCountInFileException {
        return countColumnsInFile(filePath) == columnCount;
    }

    public void compareRowsInFileAndDB(String filePath, String tableName) {
        try {
            MySQLEmbedded mySQLEmbedded = MySQLEmbedded.startEbmeddedMySQLServer();
            int fileRowCount = FSUtil.countRowInFile(filePath);
            int dbRowCount = mySQLEmbedded.countRowsInTable(tableName);
            if (fileRowCount != dbRowCount) {
                throw new DifferentRowCountInFileAndDBException("Rows count is different in file and DB!");
            }
        } catch (IOException io) {
            io.printStackTrace();
        }
    }

    private static int countColumnsInFile(String filePath) throws IOException {
        int res = 0;
        Set<Integer> countColumnsSet = new HashSet<>();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            br.lines().forEach(
                    (line) -> countColumnsSet.add(line.split(Constant.PIPE_ESCAPED).length));
        }
        if (CollectionUtils.isNotEmpty(countColumnsSet)) {
            if (countColumnsSet.size() > 0) {
                throw new IncorrectColumnCountInFileException(Constant.INCORRECT_COLUMN_SIZE);
            }
            res = countColumnsSet.iterator().next();
        }

        return res;
    }

    public void validateCommandLineArgs(CommandLine cmd) {
        if (StringUtils.isBlank(cmd.getOptionValue("a"))) {
            throw new RuntimeException("Please specify --accesslog parameter!");
        }

        if (StringUtils.isBlank(cmd.getOptionValue("s"))) {
            throw new RuntimeException("Please specify --startDate parameter!");
        } else {
            try {
                LocalDateTime.parse(cmd.getOptionValue("s"), DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss"));
            } catch (DateTimeParseException ex) {
                throw new RuntimeException("Please double check date format (yyyy-MM-dd.HH:mm:ss)", ex.getCause());
            }
        }

        if (StringUtils.isBlank(cmd.getOptionValue("d"))) {
            throw new RuntimeException("Please specify --duration argument");
        } else {
            String duration = cmd.getOptionValue("d");
            if (!duration.equals("daily") && !duration.equals("hourly")) {
                throw new RuntimeException("Duration argument could have only 'daily' or 'hourly' values.");
            }
        }

        if (StringUtils.isBlank(cmd.getOptionValue("t"))) {
            throw new RuntimeException("Please specify --threshold parameter!");
        } else {
            if (!StringUtils.isNumeric(cmd.getOptionValue("t"))) {
                throw new RuntimeException("Threshold should be numeric!");
            }
        }
    }
}