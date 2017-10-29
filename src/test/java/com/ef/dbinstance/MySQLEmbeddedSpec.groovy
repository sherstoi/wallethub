package com.ef.dbinstance

import com.ef.model.ActiveIP
import org.apache.commons.collections4.CollectionUtils
import spock.lang.Specification

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class MySQLEmbeddedSpec extends Specification {
    private MySQLEmbedded mySQLEmbedded = MySQLEmbedded.startEbmeddedMySQLServer()

    def "upload text file to embedded db" () {
        when:
            String uploadFilePath = Thread.currentThread().contextClassLoader.getResource("access_test.log").getPath()
            mySQLEmbedded.uploadMainLogToDB(uploadFilePath, "MAIN_LOG")

        then:
            mySQLEmbedded.countRowsInTable("MAIN_LOG") > 0
    }

    def "filter ip which made 1 or more request during day" () {
        List<ActiveIP> activeIPList
        when:
            activeIPList = mySQLEmbedded.findActiveIPForPeriod(LocalDateTime.parse("2017-01-01.00:00:00",
                    DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss")), "daily", 1)
        then:
            CollectionUtils.isNotEmpty(activeIPList)
            ActiveIP activeIP = activeIPList.get(0)
            activeIP.durationType == "daily"
    }
}
