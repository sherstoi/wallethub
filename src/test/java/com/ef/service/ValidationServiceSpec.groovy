package com.ef.service

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import spock.lang.Specification

class ValidationServiceSpec extends Specification {
    ValidationService validationService

    def setup() {
        validationService = new ValidationService()
    }

    def "pass empty command line arguments are not allowed"() {
        when:
            Options options = new Options()
            options.addOption("a", "accesslog", true, "Path to uploading log file to DB");
            CommandLineParser commandLineParser = new DefaultParser()
            CommandLine commandLine = commandLineParser.parse(options, new String[1])
            validationService.validateCommandLineArgs(commandLine)

        then:
            thrown(RuntimeException)
    }
}
