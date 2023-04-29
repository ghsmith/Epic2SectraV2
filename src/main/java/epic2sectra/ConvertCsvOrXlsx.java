package epic2sectra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
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

        // 1. Set configuration from command-line options and/or properties file.
        {

            Options options = new Options();

            Option optionServicesCsv = new Option("s", "services", true, "comma separated list of services to include (for filtering)");
            optionServicesCsv.setRequired(false);
            optionServicesCsv.setType(String.class);
            options.addOption(optionServicesCsv);

            Option optionNoUnstained = new Option("u", "no-unstained", false, "filter out unstained slides (stains that start with 'US...' or 'Unstained...')");
            optionNoUnstained.setRequired(false);
            options.addOption(optionNoUnstained);
            
            Option optionPropertiesFileName = new Option("z", "properties-file", true, "read options from a properties file");
            optionPropertiesFileName.setRequired(false);
            optionPropertiesFileName.setType(String.class);
            options.addOption(optionPropertiesFileName);

            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            CommandLine cmd = null; // not a good practice

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
                        if(props.get("services") != null && ((String)props.get("services")).length() > 0) {
                            for(String service : props.getProperty("services").split(",")) { services.add(service.trim().toUpperCase()); }
                        }
                        if(props.getProperty("log-file") != null && ((String)props.get("log-file")).length() > 0) { logFile = new File((String)props.get("log-file")); }
                        if(props.getProperty("epic-report-dir") != null && ((String)props.get("epic-report-dir")).length() > 0) { epicReportDir = new File((String)props.get("epic-report-dir")); }
                        if(props.getProperty("epic-missed-report-dir") != null && ((String)props.get("epic-missed-report-dir")).length() > 0) { epicMissedReportDir = new File((String)props.get("epic-missed-report-dir")); }
                        if(props.getProperty("sectra-inbox-dir") != null && ((String)props.get("sectra-inbox-dir")).length() > 0) { sectraInboxDir = new File((String)props.get("sectra-inbox-dir")); }
                        if(props.getProperty("sectra-processed-dir") != null && ((String)props.get("sectra-processed-dir")).length() > 0) { sectraProcessedDir = new File((String)props.get("sectra-processed-dir")); }
                        if(props.getProperty("report-file-name-lookback-days") != null && ((String)props.get("report-file-name-lookback-days")).length() > 0) { reportFileNameLookbackDays = Integer.valueOf(props.getProperty("report-file-name-lookback-days")); }
                        if(props.getProperty("processed-file-name-lookback-days") != null && ((String)props.get("processed-file-name-lookback-days")).length() > 0) { processedFileNameLookbackDays = Integer.valueOf(props.getProperty("processed-file-name-lookback-days")); }
                        if(props.getProperty("no-unstained") != null) {
                            noUnstained = props.getProperty("no-unstained").length() > 0; // any value in no-unstained turns it on
                        }
                        if(props.get("stain-regex") != null && ((String)props.get("stain-regex")).length() > 0) { stainRegex = props.getProperty("stain-regex"); }
                        if(props.get("excel-password") != null && ((String)props.get("excel-password")).length() > 0) { excelPassword = props.getProperty("excel-password"); }
                        if(props.get("excel-password-bypass") != null && ((String)props.get("excel-password-bypass")).length() > 0) { excelPasswordBypass = props.getProperty("excel-password-bypass"); }

                        out = new PrintStream(new FileOutputStream(logFile.toString(), true));

                        if(services.isEmpty() || epicReportDir == null || epicMissedReportDir == null || sectraInboxDir == null || sectraProcessedDir == null || reportFileNameLookbackDays == null || processedFileNameLookbackDays == null || noUnstained == null || stainRegex == null || excelPassword == null || excelPasswordBypass == null) {
                            out.println(String.format("%s - ERROR: invalid properties file", new Date()));
                            System.exit(1);
                        }

                        // inbox must be empty
                        if(sectraInboxDir.listFiles((File dir, String name) -> name.matches("^.*\\.csv$")).length > 0) {
                            out.println(String.format("%s - ERROR: Sectra inbox is not empty (%)", new Date(), sectraInboxDir.getPath()));
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
        List<File> filesToProcess = new ArrayList<>();
        
        if(propertiesFile != null) {
        
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            List<String> recentDays = new ArrayList<>();
            for(int x = 0; x < reportFileNameLookbackDays; x++) { cal.add(Calendar.DATE, -1); recentDays.add(sdf.format(cal.getTime())); }
            // Epic_reports
            List<File> allFiles = new ArrayList<>();
            allFiles.addAll(Arrays.asList(epicReportDir.listFiles((File dir, String name) ->
                name.matches("^LabSlidesOrderedPriorDayEUH_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.csv$")
                || name.matches("^LabSlidesOrderedTodayEUH_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.csv$"))));
            // Epic_missed_slide_report
            allFiles.addAll(Arrays.asList(epicMissedReportDir.listFiles((File dir, String name) ->
                name.matches("^.*_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.xlsx$"))));
            // sort in reverse order based on the timestamp in the file name
            allFiles.sort(Comparator.comparing(f -> ((File)f).getName().replaceAll("^[^\\.]*_([0-9]{8}_[0-9]{4})(\\..*)?\\.(csv|xlsx)$", "$1")).reversed());
            // only select the latest "today" file and any files that have not been previously processed
            {
                String lastDay = "99999999";
                Pattern p1 = Pattern.compile("^LabSlidesOrderedTodayEUH_([0-9]{8})_[0-9]{4}(\\..*)?\\.csv$");
                Pattern p2 = Pattern.compile("^.*_[0-9]{8}_[0-9]{4}(\\..*)?\\.(csv|xlsx)$");
                for(File file : allFiles) {
                    Matcher m1 = p1.matcher(file.getName());
                    if(m1.matches()) {
                        if(!m1.group(1).equals(lastDay)) {
                            if(m1.group(2) == null) { // if this is not null, the file has been processed (e.g, LabSlidesOrderedTodayEUH_20230428_1150.SENT_TO_SECTRA_028.csv)
                                filesToProcess.add(file);
                            }
                            lastDay = m1.group(1);
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
        
        if(filesToProcess.isEmpty()) {
            out.println(String.format("%s - nothing to do", new Date()));
            System.exit(0);
        }
        
        out.println(String.format("%s - the following Epic reports are ready to be processed", new Date()));
        for(File file : filesToProcess) {
            out.println(String.format("    %s", file.getPath()));
        }

        // 3. Load processed manifests so we can avoid sending in slides that
        //    we've already sent in. This won't be perfect and sending in a
        //    slide again doesn't have any real consequence, but we're trying
        //    to avoid doing it, anyway. If you are processing a singleton
        //    file on manual override, this is skipped.
        Map<String, Slide> processedSlideMap = null;

        if(propertiesFile != null) {
            
            processedSlideMap = new HashMap<>();
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            List<String> recentDays = new ArrayList<>();
            for(int x = 0; x < processedFileNameLookbackDays; x++) { cal.add(Calendar.DATE, -1); recentDays.add(sdf.format(cal.getTime())); }
            List<File> processedFiles = Arrays.asList(sectraProcessedDir.listFiles((File dir, String name) ->
                name.matches("^[^\\.]*_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.csv$")));
            processedFiles.sort(Comparator.comparing(f -> ((File)f).getName().replaceAll("^[^\\.]*_([0-9]{8}_[0-9]{4})(\\..*)?\\.(csv|xlsx)$", "$1")).reversed());
            for(File file : processedFiles) {
                Reader reader = new BufferedReader(new FileReader(file));
                Iterable<CSVRecord> records =
                    CSVFormat.DEFAULT.withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withTrim()
                        .parse(reader);
                for(CSVRecord record : records) {
                    try {
                        Slide slide = Slide.loadFromManifest(record);
                        if(!processedSlideMap.containsKey(slide.slideBarCode)) {
                            processedSlideMap.put(slide.slideBarCode, slide);
                        }
                    }
                    catch(ParseException e) {
                        out.println(String.format("%s - WARNING: Problem loading slide from processed manifest %s", new Date(), file.getPath()));
                    }
                }
            }

            out.println(String.format("%s - loaded %d slides from %d processed manifests", new Date(), processedSlideMap.size(), processedFiles.size()));
            for(File file : processedFiles) {
                out.println(String.format("    %s", file.getPath()));
            }
            
        }
        
        // 4. Convert from Epic report to Sectra manifest format.
        for(File file : filesToProcess) {
            
            File manifestFile = null;
            
            try {
            
                out.println(String.format("%s - converting %s to Sectra manifest", new Date(), file.getName()));
            
                // make sure the file is stable using byte counts and MD5 hashes
                {
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
                        out.println(String.format("%s - ERROR: file is not stable", new Date()));
                        System.exit(1);
                    }
                    out.println("    file is stable");
                }
                
                Set<String> globalErrorSet = new HashSet<>();
                Set<String> globalStainRegExSet = new HashSet<>();

                int rowsProcessed = 0;
                int rowsSkipped = 0;
                int rowsSkippedError = 0;
                int rowsSkippedService = 0;
                int rowsSkippedUnstained = 0;
                int rowsSkippedStainRegex = 0;
                int rowsSkippedDuplicate = 0;

                List<Slide> slides = new ArrayList<>();

                if(file.getName().endsWith(".csv")) {
                
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
                                globalErrorSet.addAll(errorList);
                                continue;
                            }

                            slides.add(slide);
                            
                        }
                        
                    }
                        
                }
                else if(file.getName().endsWith(".xlsx")) {

                    WorkbookFactory.addProvider(new XSSFWorkbookFactory());
                    Workbook workbook;
                    if(excelPassword != null) {
                        workbook = WorkbookFactory.create(file, excelPassword);
                    }
                    else {
                        workbook = WorkbookFactory.create(file);
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
                            globalErrorSet.addAll(errorList);
                            continue;
                        }

                        slides.add(slide);
                        
                    }
                        
                }
                    
                manifestFile = new File(String.format("%s\\%s.sectra_%s.csv", (file.getParent() != null ? file.getParent() : "."), file.getName(), (new SimpleDateFormat("yyyyMMdd_HHmm")).format(new Date())));
                PrintStream manifestPrintStream = new PrintStream(manifestFile);

                manifestPrintStream.println(Slide.toManifestHeaderString());
                
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
                            globalStainRegExSet.add(slide.stain);
                            continue;
                        }
                    }

                    if(processedSlideMap != null) {
                        if(processedSlideMap.containsKey(slide.slideBarCode)) {
                            rowsSkipped++;
                            rowsSkippedDuplicate++;
                            continue;
                        }
                        processedSlideMap.put(slide.slideBarCode, slide);
                    }

                    rowsProcessed++;

                    manifestPrintStream.println(slide.toManifestString());

                }

                manifestPrintStream.close();

                out.println(String.format("    %5d rows processed", rowsProcessed));
                out.println(String.format("    %5d rows skipped", rowsSkipped));
                out.println(String.format("          ...%5d skipped with errors %s", rowsSkippedError, globalErrorSet));
                out.println(String.format("          ...%5d skipped because they do not pass service filter", rowsSkippedService));
                out.println(String.format("          ...%5d skipped because unstained", rowsSkippedUnstained));
                out.println(String.format("          ...%5d skipped because stain matches regular expression %s", rowsSkippedStainRegex, globalStainRegExSet));
                out.println(String.format("          ...%5d skipped because they appear in a processed manifest", rowsSkippedDuplicate));

                if(rowsProcessed > 0) {
                    out.println(String.format("%s - created %s", new Date(), manifestFile.getName()));
                }
                else {
                    manifestFile.delete();
                    out.println("    no manifest is created since no rows were processed.");
                }

                break;
                    
            }
            catch(Exception e) {
                try { manifestFile.delete(); } catch(Exception e1) { }
                out.println(String.format("%s - ERROR: %s", new Date(), e.getMessage()));
            }
            
        }

        System.exit(0);
        
    }

    public static String byteArrayToHex(byte[] a) {
       StringBuilder sb = new StringBuilder(a.length * 2);
       for(byte b: a)
          sb.append(String.format("%02x", b));
       return sb.toString();
    }

}
