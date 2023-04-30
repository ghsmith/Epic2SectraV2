package epic2sectra;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.*;
import org.apache.commons.cli.*;
import org.apache.commons.csv.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;

/**
 *
 * @author Geoff
 */
public class ConvertCsvOrXlsx {

    public static void main(String[] args) throws IOException {

        List<String> services = new ArrayList<>();
        File singletonFile = null;

        File propertiesFile = null;
        File logFile = null;
        File epicReportDir = null;
        File epicMissedReportDir = null;
        File sectraInboxDir = null;
        File sectraProcessedDir = null;
        Integer reportFileNameLookbackDays = null;
        Integer processedFileNameLookbackDays = null;
        Boolean noUnstained = null;
        String stainRegex = null;
        String excelPassword = null;
        String excelPasswordBypass = null;
        
        PrintStream out = System.out;

        // *********************************************************************
        // 1. Set configuration from command-line options and/or properties file.
        // *********************************************************************
        {

            Options options = new Options();

            Option optionServicesCsv = new Option("s", "services", true, "comma separated list of services to include (for filtering)");
            optionServicesCsv.setRequired(false);
            optionServicesCsv.setType(String.class);
            options.addOption(optionServicesCsv);

            Option optionNoUnstained = new Option("u", "no-unstained", false, "filter out unstained slides (stains that start with \"US...\" or \"Unstained...\")");
            optionNoUnstained.setRequired(false);
            options.addOption(optionNoUnstained);
            
            Option optionStainRegex = new Option("x", "stain-regex", true, String.format("if a stain matches this regular expression, then the slide is filtered out", stainRegex));
            optionStainRegex.setRequired(false);
            optionStainRegex.setType(String.class);
            options.addOption(optionStainRegex);
            
            Option optionPropertiesFileName = new Option("z", "properties-file", true, "read options from a properties file");
            optionPropertiesFileName.setRequired(false);
            optionPropertiesFileName.setType(String.class);
            options.addOption(optionPropertiesFileName);

            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            CommandLine cmd = null; // not a good practice

            if(args.length == 0) {
                System.out.println();
                System.out.println("This program converts an Epic \"Lab Container\" template report to a Sectra manifest.");
                System.out.println();
                formatter.printHelp("java -jar epic2sectra.jar [options] {csv/xlsx-Epic-report-file-name} {xlsx-file-password}", options);
                System.exit(0);
            }
            
            try {
            
                cmd = parser.parse(options, args);
                
                if(cmd.hasOption(optionPropertiesFileName)) {

                    // This is used by the Windows scheduled task. All options
                    // are read from the properties file.

                    String propertiesFileName = cmd.getOptionValue(optionPropertiesFileName);
                    propertiesFile = new File(propertiesFileName);

                    try(InputStream inputStream = new FileInputStream(propertiesFile)) {
                        Properties props = new Properties();
                        props.load(inputStream);
                        if(props.get("services") != null && (props.getProperty("services")).length() > 0) {
                            for(String service : props.getProperty("services").split(",")) { services.add(service.trim().toUpperCase()); }
                        }
                        if(props.getProperty("log-file") != null && (props.getProperty("log-file")).length() > 0) { logFile = new File(props.getProperty("log-file")); }
                        if(props.getProperty("epic-report-dir") != null && (props.getProperty("epic-report-dir")).length() > 0) { epicReportDir = new File(props.getProperty("epic-report-dir")); }
                        if(props.getProperty("epic-missed-report-dir") != null && (props.getProperty("epic-missed-report-dir")).length() > 0) { epicMissedReportDir = new File(props.getProperty("epic-missed-report-dir")); }
                        if(props.getProperty("sectra-inbox-dir") != null && (props.getProperty("sectra-inbox-dir")).length() > 0) { sectraInboxDir = new File(props.getProperty("sectra-inbox-dir")); }
                        if(props.getProperty("sectra-processed-dir") != null && (props.getProperty("sectra-processed-dir")).length() > 0) { sectraProcessedDir = new File(props.getProperty("sectra-processed-dir")); }
                        if(props.getProperty("report-file-name-lookback-days") != null && (props.getProperty("report-file-name-lookback-days")).length() > 0) { reportFileNameLookbackDays = Integer.valueOf(props.getProperty("report-file-name-lookback-days")); }
                        if(props.getProperty("processed-file-name-lookback-days") != null && (props.getProperty("processed-file-name-lookback-days")).length() > 0) { processedFileNameLookbackDays = Integer.valueOf(props.getProperty("processed-file-name-lookback-days")); }
                        if(props.getProperty("no-unstained") != null) {
                            noUnstained = props.getProperty("no-unstained").length() > 0; // any value in no-unstained turns it on
                        }
                        if(props.get("stain-regex") != null && (props.getProperty("stain-regex")).length() > 0) { stainRegex = props.getProperty("stain-regex"); }
                        if(props.get("excel-password") != null && (props.getProperty("excel-password")).length() > 0) { excelPassword = props.getProperty("excel-password"); }
                        if(props.get("excel-password-bypass") != null && (props.getProperty("excel-password-bypass")).length() > 0) { excelPasswordBypass = props.getProperty("excel-password-bypass"); }

                        out = new PrintStream(new FileOutputStream(logFile.toString(), true));

                        if(services.isEmpty() || epicReportDir == null || epicMissedReportDir == null || sectraInboxDir == null || sectraProcessedDir == null || reportFileNameLookbackDays == null || processedFileNameLookbackDays == null || noUnstained == null || stainRegex == null || excelPassword == null || excelPasswordBypass == null) {
                            out.println();
                            out.println(String.format("%s - ERROR: invalid properties file", new Date()));
                            System.exit(1);
                        }

                    }

                }
                else {

                    // This is used if you are on manual override and want to
                    // process a single Epic report file in CSV or XLSX format
                    // which is specified as command-line argument. In this
                    // case, the only options you have are to filter on service
                    // and remove unstained slides. This is only intended to be
                    // used in emergencies where you need to put manifests in
                    // the Sectra inbox directory manually. Only administrators
                    // have direct access to the Sectra inbox directory.

                    if(cmd.hasOption(optionServicesCsv)) {
                        for(String service : cmd.getOptionValue(optionServicesCsv).split(",")) { services.add(service.trim().toUpperCase()); }
                    }

                    noUnstained = cmd.hasOption(optionNoUnstained);

                    if(cmd.hasOption(optionStainRegex)) { stainRegex = cmd.getOptionValue(optionStainRegex); }

                    singletonFile = new File(cmd.getArgs()[0]);
                    if(cmd.getArgs().length > 1) { excelPassword = cmd.getArgs()[1]; } // if processing a CSV, you don't need an Excel password

                }
            
            }
            catch (org.apache.commons.cli.ParseException e) {
                out.println(e.getMessage());
                formatter.printHelp("java -jar epic2sectra.jar ConvertCsv2 [options] {CSV-file-name}", options);
                System.exit(1);
            }
            
        }

        // *********************************************************************
        // 2. Load the filesToProcess list. The objective is to create a list of
        //    files that need to be processed ordered from most recent file to
        //    oldest, based on the yyyyMMdd_HHm timestamp that Epic puts in the
        //    file name of the report export. If you are processing a singleton
        //    file on manual override, this is skipped because we're just going
        //    to process whatever file you identified. Here are some scenarios:
        //
        //      LabSlidesOrderedTodayEUH_20230428_1150.csv <-- added to filesToProcess list
        //      LabSlidesOrderedTodayEUH_20230428_1050.csv <-- ignored, b/c there is later file on same day
        //
        //      LabSlidesOrderedTodayEUH_20230428_1150.SENT_TO_SECTRA_028.csv <-- latest file for day but ignored, b/c already processed
        //      LabSlidesOrderedTodayEUH_20230428_1050.csv <-- ignored, b/c there is later file on same day
        //
        //    The file-name-lookback-days parameter specifies how many days to
        //    look back (e.g., if today is 4/28 and lookback days is 1, we'll
        //    look for Epic report filenames with 20230428 and 20230427 in the
        //    file name.
        // *********************************************************************
        List<File> filesToProcess = new ArrayList<>();
        
        if(singletonFile == null) {
        
            List<String> recentDays = recentDaysListYYYYYMMDD(reportFileNameLookbackDays);
            // Epic_reports
            List<File> allFiles = new ArrayList<>();
            allFiles.addAll(Arrays.asList(epicReportDir.listFiles((File dir, String name) ->
                name.matches("(?i)^LabSlidesOrderedPriorDayEUH_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\.[^\\.]*)?\\.csv$")
                || name.matches("(?i)^LabSlidesOrderedTodayEUH_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\.[^\\.]*)?\\.csv$"))));
            // Epic_missed_slide_report
            allFiles.addAll(Arrays.asList(epicMissedReportDir.listFiles((File dir, String name) ->
                name.matches("(?i)^[^\\.]*_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\.[^\\.]*)?\\.xlsx$"))));
            // sort in reverse order based on the timestamp in the file name
            allFiles.sort(Comparator.comparing(f -> ((File)f).getName().replaceAll("(?i)^[^\\.]*_([0-9]{8}_[0-9]{4})(\\.[^\\.]*)?\\.(csv|xlsx)$", "$1")).reversed());
            // only select the latest "today" file and any files that have not been previously processed
            {
                String lastDay = "99999999";
                Pattern p1 = Pattern.compile("(?i)^LabSlidesOrderedTodayEUH_([0-9]{8})_[0-9]{4}(\\.[^\\.]*)?\\.csv$");
                Pattern p2 = Pattern.compile("(?i)^.[^\\.]*_[0-9]{8}_[0-9]{4}(\\.[^\\.]*)?\\.(csv|xlsx)$");
                for(File file : allFiles) {
                    Matcher m1 = p1.matcher(file.getName());
                    if(m1.matches()) {
                        if(!m1.group(1).equals(lastDay)) {
                            lastDay = m1.group(1);
                            if(m1.group(2) == null) { // if this is not null, the file has been processed (e.g, LabSlidesOrderedTodayEUH_20230428_1150.SENT_TO_SECTRA_028.csv)
                                filesToProcess.add(file);
                            }
                        }
                    }
                    else {
                        Matcher m2 = p2.matcher(file.getName());
                        if(m2.matches()) {
                            if(m2.group(1) == null) { // if this is not null, the file has been processed (e.g, Lab_Containers_20230428_1150.SENT_TO_SECTRA_028.csv)
                                filesToProcess.add(file);
                            }
                        }
                    }
                }
            }
            
        }
        else {
            
            filesToProcess.add(singletonFile);
            
        }
        
        // *********************************************************************
        // 3. Do some preliminary checks to make sure we should proceed.
        // *********************************************************************
        {
        
            if(filesToProcess.isEmpty()) {
                out.println();
                out.println(String.format("%s - nothing to do", new Date()));
                System.exit(0);
            }
            
            if(singletonFile == null) {

                out.println();
                out.println(String.format("%s - running with these parameters", new Date(), InetAddress.getLocalHost().getHostName()));
                out.println(String.format("    souce-code:             %s", "https://github.com/ghsmith/Epic2SectraV2"));
                out.println(String.format("    host-name:              %s", InetAddress.getLocalHost().getHostName()));
                out.println(String.format("    user-name:              %s", System.getProperty("user.name")));
                out.println(String.format("    services:               %s", services));
                out.println(String.format("    log-file:               %s", logFile.getPath()));
                out.println(String.format("    epic-report-dir:        %s", epicReportDir.getPath()));
                out.println(String.format("    epic-missed-report-dir: %s", epicMissedReportDir.getPath()));
                out.println(String.format("    sectra-inbox-dir:       %s", sectraInboxDir.getPath()));
                out.println(String.format("    sectra-processed-dir:   %s", sectraProcessedDir.getPath()));
                out.println(String.format("    report-file-name-lookback-days:    %d %s", reportFileNameLookbackDays, recentDaysListYYYYYMMDD(reportFileNameLookbackDays)));
                out.println(String.format("    processed-file-name-lookback-days: %d %s", processedFileNameLookbackDays, recentDaysListYYYYYMMDD(processedFileNameLookbackDays)));
                out.println(String.format("    no-unstained:           %s", noUnstained));
                out.println(String.format("    stain-regex:            %s", stainRegex));
                //out.println(String.format("    excel-password:         %s", excelPassword));
                //out.println(String.format("    excel-password-bypass:  %s", excelPasswordBypass));
                
                out.println();
                out.println(String.format("%s - the following Epic reports are ready to be processed (in order)", new Date()));
                if(singletonFile == null) {
                    out.println("    NOTE: For the LabSlidesOrderedTodayEUH_yyyyMMdd_HHmm.csv reports, only the");
                    out.println("          latest report for a day is a candidate for processing, since a later");
                    out.println("          \"Today\" report contains all of the records from an earlier");
                    out.println("          \"Today\" report on the same day. The report-file-name-lookback-days");
                    out.println(String.format("          parameter (currently = %d) controls how far back the system looks for", reportFileNameLookbackDays));
                    out.println("          files to process.");
                }
                for(File file : filesToProcess) {
                    out.println(String.format("    [%s] %s", file.getName().replaceAll("^[^\\.]*_([0-9]{8}_[0-9]{4})(\\.[^\\.]*)?\\.(csv|xlsx)$", "$1"), file.getPath()));
                }

                // inbox must be empty
                if(sectraInboxDir.listFiles((File dir, String name) -> name.matches("^.*\\.csv$")).length > 0) {
                    if(sectraInboxDir.getPath().equals(sectraProcessedDir.getPath())) {
                        out.println();
                        out.println(String.format("%s - WARNING: Sectra inbox is not empty", new Date()));
                        out.println("    NOTE: The Sectra inbox and Sectra processed directories are the same,");
                        out.println("          so you are probably testing something and the program will");
                        out.println("          proceed. This is normally a hard stop, though.");
                        out.println(String.format("    inbox    : %s", sectraInboxDir.getPath()));
                        out.println(String.format("    processed: %s", sectraProcessedDir.getPath()));
                    }
                    else {
                        out.println();
                        out.println(String.format("%s - ERROR: Sectra inbox is not empty", new Date()));
                        out.println(String.format("    %s", sectraInboxDir.getPath()));
                        out.println("    CONTACT SECTRA IF THIS PERSISTS - THE INBOX IS NORMALLY CLEARED ALMOST IMMEDIATELY");
                        for(File file : sectraInboxDir.listFiles((File dir, String name) -> name.matches("^.*\\.csv$"))) {
                            out.println(String.format("    %s", file.getPath()));
                        }
                        System.exit(1);
                    }
                }
                
            }
            
        }
        
        // *********************************************************************
        // 4. Load processed manifests so we can avoid sending in slides that
        //    we've already sent in. This won't be perfect and sending in a
        //    slide again doesn't have any real consequence, but we're trying
        //    to avoid doing it, anyway. If you are processing a singleton
        //    file on manual override, this is skipped.
        // *********************************************************************
        Map<String, Slide> processedSlideMap = null;

        if(singletonFile == null) {
            
            processedSlideMap = new HashMap<>();
            List<String> recentDays = recentDaysListYYYYYMMDD(processedFileNameLookbackDays);
            List<File> processedFiles = Arrays.asList(sectraProcessedDir.listFiles((File dir, String name) ->
                name.matches("(?i)^[^\\.]*_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.csv$")));
            processedFiles.sort(Comparator.comparing(f -> ((File)f).getName().replaceAll("(?i)^[^\\.]*_([0-9]{8}_[0-9]{4})(\\..*)?\\.csv$", "$1")).reversed());
            for(File file : processedFiles) {
                String fileNameTimestamp = file.getName().replaceAll("(?i)^[^\\.]*_([0-9]{8}_[0-9]{4})(\\..*)?\\.csv$", "$1");
                Reader reader = new BufferedReader(new FileReader(file));
                Iterable<CSVRecord> records =
                    CSVFormat.DEFAULT.withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim()
                        .parse(reader);
                for(CSVRecord record : records) {
                    try {
                        Slide slide = Slide.loadFromManifest(record);
                        slide.fileNameTimestamp = fileNameTimestamp;
                        if(!processedSlideMap.containsKey(slide.slideBarCode)) { // we're processing the files from latest to earliest, so we only take the latest record if the slide appears in multiple previously processed manifests
                            processedSlideMap.put(slide.slideBarCode, slide);
                        }
                    }
                    catch(Exception e) {
                        out.println();
                        out.println(String.format("%s - WARNING: problem loading slide from previously processed manifest for deduplication", new Date()));
                        out.println(String.format("    %s", file.getPath()));
                        out.println(String.format("    %s", e.getMessage()));
                    }
                }
            }

            out.println();
            out.println(String.format("%s - loaded %d slides from %d previously processed manifests", new Date(), processedSlideMap.size(), processedFiles.size()));
            out.println("    NOTE: Slide bar codes appearing in these files will be filtered out of the");
            out.println("          current manifest. This avoids sending the same slide bar code to");
            out.println("          Sectra multiple times. This is a form of deduplication. The");
            out.println(String.format("          processed-file-name-lookback-days parameter (currently = %d)", processedFileNameLookbackDays));
            out.println("          controls how far back the system looks for files to process.");
            for(File file : processedFiles) {
                out.println(String.format("    [%s] %s", file.getName().replaceAll("^[^\\.]*_([0-9]{8}_[0-9]{4})(\\..*)?\\.csv$", "$1"), file.getPath()));
            }
            
        }
        
        // *********************************************************************
        // 5. Convert from Epic report to Sectra manifest format.
        // *********************************************************************
        for(File file : filesToProcess) {
            
            String fileNameTimestamp = file.getName().replaceAll("^[^\\.]*_([0-9]{8}_[0-9]{4})(\\.[^\\.]*)?\\.(csv|xlsx)$", "$1");
                    
            File manifestFile = null;
            int rowsProcessed = -1;
            int rowsProcessedMaxAllowed = 300; // we would not do this many GI biopsy slides in a day
            
            try {
            
                out.println();
                out.println(String.format("%s - converting %s to Sectra manifest", new Date(), file.getName()));
            
                // make sure the file is stable using byte counts and MD5 hashes
                if(singletonFile == null) {
                    int[] byteCounts = new int[] { 0, 0 };
                    byte[][] md5Hashes = new byte[2][];
                    for(int x = 0; x < 2; x++) {
                        MessageDigest md = MessageDigest.getInstance("MD5");
                        try (
                            FileInputStream fileInputStream = new FileInputStream(file);
                            DigestInputStream dis = new DigestInputStream(fileInputStream, md);
                        ) {
                            //byteCounts[x] = dis.readAllBytes().length; - not in Java 1.8
                            byte[] buf = new byte[100000];
                            int readLen;
                            while ((readLen = dis.read(buf, 0, 100000)) != -1) { byteCounts[x] += readLen; }
                            md5Hashes[x] = md.digest();
                            out.println(String.format("    MD5 hash = %s file size = %d bytes", byteArrayToHex(md5Hashes[x]), byteCounts[x]));
                            if(x == 0) { Thread.sleep(5000); }
                        }
                    }
                    if(byteCounts[0] != byteCounts[1] || !Arrays.equals(md5Hashes[0], md5Hashes[1])) {
                        throw new Exception("file not stable over 5 seconds");
                    }
                    out.println("    file is stable over 5 seconds");
                }
                
                Set<String> errorSet = new HashSet<>();
                Set<String> filteredStainSet = new HashSet<>();

                int rowsSkipped = 0;
                int rowsSkippedError = 0;
                int rowsSkippedService = 0;
                int rowsSkippedUnstained = 0;
                int rowsSkippedStainRegex = 0;
                int rowsSkippedDuplicate = 0;
                int rowsStainUpdateAllowed = 0;

                List<Slide> slides = new ArrayList<>();

                if(file.getName().toLowerCase().endsWith(".csv")) {

                    // *********************************************************
                    // Epic rw_extract (i.e., the scheduled job) generates files
                    // in a CSV format.
                    // *********************************************************
                    
                    try (
                        FileInputStream fileInputStream = new FileInputStream(file);
                        Reader reader = new BufferedReader(new InputStreamReader(fileInputStream));
                    ) {

                        Iterable<CSVRecord> records =
                            CSVFormat.DEFAULT.withFirstRecordAsHeader()
                                .withIgnoreHeaderCase()
                                .withTrim()
                                .parse(reader);

                        for(CSVRecord record : records) {

                            List<String> errorList = new ArrayList<>();
                            Slide slide = Slide.load(record, errorList);

                            if(slide == null) {
                                rowsSkipped++;
                                rowsSkippedError++;
                                errorSet.addAll(errorList);
                                continue;
                            }

                            slide.fileNameTimestamp = fileNameTimestamp;
                            slides.add(slide);
                            
                        }
                        
                    }
                        
                }
                else if(file.getName().toLowerCase().endsWith(".xlsx")) {

                    // *********************************************************
                    // Reports run im Epic interactively use the XLSX format
                    // and have an Excel password.
                    // *********************************************************
                    
                    WorkbookFactory.addProvider(new XSSFWorkbookFactory());
                    Workbook workbook;
                    try {
                        workbook = WorkbookFactory.create(file, excelPassword);
                        rowsProcessedMaxAllowed = 50;
                    }
                    catch(Exception e) {
                        workbook = WorkbookFactory.create(file, excelPasswordBypass);
                    }
                    Sheet sheet = workbook.getSheetAt(0);

                    Iterator<Row> rowIterator = sheet.iterator();
                    Row headerRow = rowIterator.next();
                    Map<String, Integer> columnIndexByNameMap = new HashMap<>();
                    columnIndexByNameMap.put("Slide Bar Code", Integer.valueOf(headerRow.getFirstCellNum())); // Epic uses "Container" for two different columns
                    for(int x = headerRow.getFirstCellNum() + 1; x <= headerRow.getLastCellNum(); x++) {
                        if(headerRow.getCell(x) != null) {
                            columnIndexByNameMap.put(headerRow.getCell(x).getStringCellValue(), x);
                        }
                    }

                    while(rowIterator.hasNext()) {

                        Row dataRow = rowIterator.next();
                        List<String> errorList = new ArrayList<>();
                        Slide slide = Slide.load(dataRow, errorList, columnIndexByNameMap);
                        
                        if(slide == null) {
                            rowsSkipped++;
                            rowsSkippedError++;
                            errorSet.addAll(errorList);
                            continue;
                        }

                        slide.fileNameTimestamp = fileNameTimestamp;
                        slides.add(slide);
                        
                    }
                        
                }
                    
                manifestFile = new File(String.format("%s\\%s.sectra_%s.csv", (file.getParent() != null ? file.getParent() : "."), file.getName(), (new SimpleDateFormat("yyyyMMdd_HHmm")).format(new Date())));
                PrintStream manifestPrintStream = new PrintStream(manifestFile);

                manifestPrintStream.println(Slide.toManifestHeaderString());
                
                rowsProcessed = 0;

                for(Slide slide : slides) {

                    if(!services.isEmpty()) {
                        if(!services.contains(slide.service.toUpperCase())) {
                            rowsSkipped++;
                            rowsSkippedService++;
                            continue;
                        }
                    }

                    if(noUnstained) {
                        if(slide.stain.startsWith("US") || slide.stain.startsWith("Unstained")) {
                            rowsSkipped++;
                            rowsSkippedUnstained++;
                            continue;
                        }
                    }

                    if(stainRegex != null) {
                        if(slide.stain.matches(stainRegex)) {
                            rowsSkipped++;
                            rowsSkippedStainRegex++;
                            filteredStainSet.add(slide.stain);
                            continue;
                        }
                    }

                    if(processedSlideMap != null) {
                        if(processedSlideMap.containsKey(slide.slideBarCode)) {
                            // let stain updates go through
                            Slide previousSlide = processedSlideMap.get(slide.slideBarCode);
                            if(!slide.stain.equals(previousSlide.stain) && slide.fileNameTimestamp.compareTo(previousSlide.fileNameTimestamp) > 0) {
                                rowsStainUpdateAllowed++;
                            }
                            else {
                                rowsSkipped++;
                                rowsSkippedDuplicate++;
                                continue;
                            }
                        }
                    }

                    rowsProcessed++;

                    manifestPrintStream.println(slide.toManifestString());

                }

                manifestPrintStream.close();

                out.println(String.format("    %5d rows processed", rowsProcessed));
                out.println(String.format("    %5d rows skipped", rowsSkipped));
                out.println(String.format("          ...%5d skipped with errors %s", rowsSkippedError, errorSet));
                out.println(String.format("          ...%5d skipped because they do not match a service filter (service selection is: %s)", rowsSkippedService, services));
                out.println(String.format("          ...%5d skipped because unstained (unstained exclusion is: %s)", rowsSkippedUnstained, noUnstained ? "TURNED ON" : "TURNED OFF"));
                out.println(String.format("          ...%5d skipped because stain matches regular expression (stain filtering is: %s) %s", rowsSkippedStainRegex, filteredStainSet, stainRegex != null ? "TURNED ON" : "TURNED OFF"));
                if(singletonFile ==  null) {
                    out.println(String.format("          ...%5d skipped because they appear in a processed manifest (%d stain updates were allowed)", rowsSkippedDuplicate, rowsStainUpdateAllowed));
                }
                else {
                    out.println("          ... duplicate checking in previous manifests is DISABLED when running manually");
                }

                if(singletonFile == null) {

                    if(rowsProcessed > 0) {

                        out.println();
                        out.println(String.format("%s - created manifest", new Date()));
                        out.println(String.format("    %s", manifestFile.getPath()));

                        if(rowsProcessed <= rowsProcessedMaxAllowed) {
                            Path inboxTarget = Paths.get(sectraInboxDir.getPath() + "\\" + manifestFile.getName());
                            Files.move(manifestFile.toPath(), inboxTarget);
                            out.println();
                            out.println(String.format("%s - moved manifest to Sectra inbox", new Date()));
                            out.println(String.format("    %s", inboxTarget.toFile().getPath()));
                        }
                        else {
                            throw new Exception(String.format("manifest too large (the limit is %d slides in one manifest)", rowsProcessedMaxAllowed));
                        }

                    }
                    else {

                        manifestFile.delete();
                        out.println();
                        out.println(String.format("%s - no manifest created", new Date()));

                    }
                    
                    Path renameTarget = Paths.get(file.getParent() + "\\" + file.getName().replaceAll("(?i)\\.(csv|xlsx)$", String.format(".SENT_TO_SECTRA_%03d.$1", rowsProcessed)));
                    Files.move(file.toPath(), renameTarget);
                    out.println();
                    out.println(String.format("%s - renamed Epic report to prevent future processing", new Date()));
                    out.println(String.format("    %s", renameTarget.toFile().getPath()));

                }

                 // if you remove this break, it will process more than one file
                 // at a time - at the moment it only processes the first file
                 // in the filesToProcess list and the subsequent ones have to
                 // wait for subsequent invocations of the program - this
                 // throttles the number of files we send to Sectra at the same
                 // time and makes the previously processed semantics a lot
                 // simpler
                break;
                    
            }
            catch(Exception e) {

                // the idea of putting this exception handler in the
                // filesToProcess loop is to try and process the next file
                // if the current file breaks something - this might make things
                // a little less brittle and mitigate the risk of one bad
                // file shutting us down

                //try { manifestFile.delete(); } catch(Exception e1) { }
                out.println();
                out.println(String.format("%s - WARNING: Epic report not processed", new Date()));
                out.println(String.format("    %s", e.getMessage()));
                if(e.getMessage() == null || e.getMessage().length() == 0) { e.printStackTrace(out); }

                if(singletonFile == null) {

                    // if the report is growing, it should be stable next time we try in a few minutes so we should not rename the file
                    if(!e.getMessage().contains("file not stable")) {
                        Path renameTarget;
                        if(rowsProcessed == -1) {
                            renameTarget = Paths.get(file.getParent() + "\\" + file.getName().replaceAll("(?i)\\.(csv|xlsx)$", ".REJECTED.$1"));
                        }
                        else {
                            renameTarget = Paths.get(file.getParent() + "\\" + file.getName().replaceAll("(?i)\\.(csv|xlsx)$", String.format(".REJECTED_%03d.$1", rowsProcessed)));
                        }
                        Files.move(file.toPath(), renameTarget);
                        out.println();
                        out.println(String.format("%s - renamed Epic report to prevent future processing", new Date()));
                        out.println(String.format("    %s", renameTarget.toFile().getPath()));
                    }
                    
                }

            }
            
        }

        System.exit(0);
        
    }

    private static List<String> recentDaysListYYYYYMMDD(int lookbackDays) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        List<String> recentDays = new ArrayList<>();
        recentDays.add(sdf.format(cal.getTime())); // don't forget to add today
        for(int x = 0; x < lookbackDays; x++) { cal.add(Calendar.DATE, -1); recentDays.add(sdf.format(cal.getTime())); }
        return recentDays;
    }
    
    private static String byteArrayToHex(byte[] a) {
       StringBuilder sb = new StringBuilder(a.length * 2);
       for(byte b: a)
          sb.append(String.format("%02x", b));
       return sb.toString();
    }

}
