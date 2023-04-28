package epic2sectra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 *
 * @author Geoff
 */
public class ConvertCsvOrXlsx {

    public static void main(String[] args) throws IOException {

        List<String> services = new ArrayList<>();
        Path singletonFile = null;

        File propertiesFile = null;
        File logFile = null;
        File epicReportDir = null;
        File epicMissedReportDir = null;
        File sectraInboxDir = null;
        File sectraProcessedDir = null;
        int fileNameLookbackDays = -1;
        String stainRegex = null;
        String excelPassword = null;
        String excelPasswordBypass = null;
        
        PrintStream out = System.out;

        List<File> filesToProcess = new ArrayList<>();
        
        // set configuration from command-line options and/or properties file
        {

            Options options = new Options();

            Option optionServicesCsv = new Option("s", "services", true, "comma separated list of services to include (for filtering)");
            optionServicesCsv.setRequired(false);
            optionServicesCsv.setType(String.class);
            options.addOption(optionServicesCsv);

            Option optionPropertiesFileName = new Option("z", "properties-file", true, "read options from a properties file");
            optionPropertiesFileName.setRequired(false);
            optionPropertiesFileName.setType(String.class);
            options.addOption(optionPropertiesFileName);

            CommandLineParser parser = new DefaultParser();
            HelpFormatter formatter = new HelpFormatter();
            CommandLine cmd = null; // not a good practice

            try {
            
                cmd = parser.parse(options, args);

                if(cmd.hasOption(optionServicesCsv)) {
                    for(String service : cmd.getOptionValue(optionServicesCsv).split(",")) { services.add(service.trim().toUpperCase()); }
                }

                if(cmd.hasOption(optionPropertiesFileName)) {
                    String propertiesFileName = cmd.getOptionValue(optionPropertiesFileName);
                    propertiesFile = new File(propertiesFileName);

                    try(InputStream inputStream = new FileInputStream(propertiesFile)) {
                        Properties props = new Properties();
                        props.load(inputStream);
                        if(props.get("services") != null && ((String)props.get("services")).length() > 0) {
                            for(String service : props.getProperty("services").split(",")) { services.add(service.trim().toUpperCase()); }
                        }
                        if(props.get("log-file") != null && ((String)props.get("log-file")).length() > 0) { logFile = new File((String)props.get("log-file")); }
                        if(props.get("epic-report-dir") != null && ((String)props.get("epic-report-dir")).length() > 0) { epicReportDir = new File((String)props.get("epic-report-dir")); }
                        if(props.get("epic-missed-report-dir") != null && ((String)props.get("epic-missed-report-dir")).length() > 0) { epicMissedReportDir = new File((String)props.get("epic-missed-report-dir")); }
                        if(props.get("sectra-inbox-dir") != null && ((String)props.get("sectra-inbox-dir")).length() > 0) { sectraInboxDir = new File((String)props.get("sectra-inbox-dir")); }
                        if(props.get("sectra-processed-dir") != null && ((String)props.get("sectra-processed-dir")).length() > 0) { sectraProcessedDir = new File((String)props.get("sectra-processed-dir")); }
                        if(props.get("file-name-lookback-days") != null && ((String)props.get("file-name-lookback-days")).length() > 0) { fileNameLookbackDays = Integer.parseInt(props.getProperty("file-name-lookback-days")); }
                        if(props.get("stain-regex") != null && ((String)props.get("stain-regex")).length() > 0) { stainRegex = props.getProperty("stain-regex"); }
                        if(props.get("excel-password") != null && ((String)props.get("excel-password")).length() > 0) { excelPassword = props.getProperty("excel-password"); }
                        if(props.get("excel-password-bypass") != null && ((String)props.get("excel-password-bypass")).length() > 0) { excelPasswordBypass = props.getProperty("excel-password-bypass"); }

                        out = new PrintStream(new FileOutputStream(logFile.toString(), true));

                        if(services.isEmpty() || epicReportDir == null || epicMissedReportDir == null || sectraInboxDir == null || sectraProcessedDir == null || fileNameLookbackDays == -1 || stainRegex == null || excelPassword == null || excelPasswordBypass == null) {
                            out.println(String.format("%s - invalid properties file", new Date()));
                            System.exit(1);
                        }

                    }

                }
                else {

                    singletonFile = Paths.get(cmd.getArgs()[0]);

                }
            
            }
            catch (org.apache.commons.cli.ParseException e) {
                out.println(e.getMessage());
                formatter.printHelp("java -jar epic2sectra.jar ConvertCsv2 [options] {CSV-file-name}", options);
                System.exit(1);
            }
            
        }

        // load filesToProcess list
        if(propertiesFile != null) {
        
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            List<String> recentDays = new ArrayList<>();
            for(int x = 0; x < fileNameLookbackDays; x++) { cal.add(Calendar.DATE, -1); recentDays.add(sdf.format(cal.getTime())); }
            // Epic_reports
            List<File> allFiles = new ArrayList<>();
            allFiles.addAll(Arrays.asList(epicReportDir.listFiles((File dir, String name) ->
                name.matches("^LabSlidesOrderedPriorDayEUH_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.csv$")
                || name.matches("^LabSlidesOrderedTodayEUH_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.csv$"))));
            // Epic_missed_slide_report
            allFiles.addAll(Arrays.asList(epicMissedReportDir.listFiles((File dir, String name) ->
                name.matches("^.*_(" + String.join("|", recentDays) + ")_[0-9]{4}(\\..*)?\\.xlsx$"))));
            // sort in reverse order based on the timestamp in the file name
            allFiles.sort(Comparator.comparing(f -> ((File)f).getName().replaceAll("^.*([0-9]{8}_[0-9]{4})(\\..*)?\\.(csv|xlsx)$", "$1")).reversed());
            // remove all but the latest "today" file and any files that have been previously processed (e.g., LabSlidesOrderedToday_20230427_2350.SENT_TO_SECTRA_000.csv)
            {
                String lastDay = "99999999";
                Pattern p1 = Pattern.compile("^LabSlidesOrderedTodayEUH_([0-9]{8})_[0-9]{4}(\\..*)?\\.csv$");
                Pattern p2 = Pattern.compile("^.*_[0-9]{8}_[0-9]{4}(\\..*)?\\.(csv|xlsx)$");
                for(File file : allFiles) {
                    Matcher m1 = p1.matcher(file.getName());
                    if(m1.matches()) {
                        if(!m1.group(1).equals(lastDay)) {
                            if(m1.group(2) == null) {
                                filesToProcess.add(file);
                            }
                            lastDay = m1.group(1);
                        }
                    }
                    else {
                        Matcher m2 = p2.matcher(file.getName());
                        if(m2.matches()) {
                            if(m2.group(1) == null) {
                                filesToProcess.add(file);
                            }
                        }
                    }
                }
            }
            
        }
        
        for(File file : filesToProcess) {
            System.out.println(file.getName());
        }
        
    }
    
}
