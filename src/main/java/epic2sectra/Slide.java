package epic2sectra;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Row;

/**
 *
 * @author geoff
 */
public class Slide {

    static DateFormat dfDayIn = new SimpleDateFormat("MM/dd/yyyy");
    static DateFormat dfTimestamp1In = new SimpleDateFormat("MM/dd/yyyy HHmm");
    static DateFormat dfTimestamp2In = new SimpleDateFormat("MM/dd/yyyy hh:mm a");
    static DateFormat dfTimestamp2InBackup = new SimpleDateFormat("MM/dd/yyyy HH:mm");

    static DateFormat dfDayOut = new SimpleDateFormat("yyyyMMdd");
    static DateFormat dfTimestampOut = new SimpleDateFormat("yyyyMMddHHmmss");
    
    public Integer slideId;
    public String slideBarCode;
    public String service;
    public String accNo;
    public String partId;
    public String blockId;
    public String slideNo;
    public String stain;
    public String mrn;
    public String empi;
    public Date dob;
    public String lastName;
    public String firstName;
    public String gender;
    public Date collectionDt;
    public Date orderDt;
    
    public static Slide load(CSVRecord record, List<String> errorList) {

        // getting bar code by index (0) because Epic uses "Container" twice
        if(record.get(0) == null || record.get(0).length() == 0) { errorList.add("Slide Bar Code is NULL"); }
        if(record.get("Specialty") == null || record.get("Specialty").length() == 0) { errorList.add("Specialty is NULL"); }
        if(record.get("Container") == null || record.get("Container").length() == 0) { errorList.add("Container is NULL"); }
        if(record.get("Task") == null || record.get("Task").length() == 0) { errorList.add("Task is NULL"); }
        if(record.get("MRN") == null || record.get("MRN").length() == 0) { errorList.add("MRN is NULL"); }
        if(record.get("Patient Enterprise ID") == null || record.get("Patient Enterprise ID").length() == 0) { errorList.add("Patient Enterprise ID is NULL"); }
        if(record.get("Birth Date") == null || record.get("Birth Date").length() == 0) { errorList.add("Birth Date is NULL"); }
        if(record.get("Patient Last Name") == null || record.get("Patient Last Name").length() == 0) { errorList.add("Patient Last Name is NULL"); }
        if(record.get("Patient First Name") == null || record.get("Patient First Name").length() == 0) { errorList.add("Patient First Name is NULL"); }
        // Collected is allowed to be NULL, in which case we use the Ordered Instant as the Collected
        if(record.get("Ordered Instant") == null || record.get("Ordered Instant").length() == 0) { errorList.add("Ordered Instant is NULL"); }
        Date parsedBirthDate = null;
        if(record.get("Birth Date") != null && record.get("Birth Date").length() > 0) {
            try { parsedBirthDate = dfDayIn.parse(record.get("Birth Date")); } catch(ParseException e) { errorList.add("Birth Date date format can't be parsed"); }
        }
        Date parsedCollected = null;
        if(record.get("Collected") != null && record.get("Collected").length() > 0) {
            try { parsedCollected = dfTimestamp1In.parse(record.get("Collected")); }
            catch(ParseException e0)  {
                try { parsedCollected = dfDayIn.parse(record.get("Collected")); } catch(ParseException e1) { errorList.add("Collected date format can't be parsed"); }
            }
        }
        Date parsedOrderedInstant = null;
        if(record.get("Ordered Instant") != null && record.get("Ordered Instant").length() > 0) {
            try { parsedOrderedInstant = dfTimestamp2In.parse(record.get("Ordered Instant")); }
            catch(ParseException e0) { 
                try { parsedOrderedInstant = dfTimestamp2InBackup.parse(record.get("Ordered Instant")); } catch(ParseException e1) { errorList.add("Ordered Instant date format can't be parsed"); }
            }
        }

        if(!errorList.isEmpty()) {
            return null;
        }

        String gender = record.get("Gender").substring(0, 1);

        Slide slide = new Slide();

        slide.slideBarCode = record.get(0);
        slide.service = record.get("Specialty");
        slide.accNo = record.get("Specimen/Case ID");
        slide.partId = record.get("Container").split(",")[1].trim();
        slide.blockId = record.get("Container").split(",")[2].trim();
        slide.slideNo = record.get("Container").split(",")[3].trim();
        slide.stain = record.get("Task");
        slide.mrn = record.get("MRN");
        slide.empi = record.get("Patient Enterprise ID");
        slide.dob = parsedBirthDate;
        slide.lastName = record.get("Patient Last Name");
        slide.firstName = record.get("Patient First Name");
        slide.gender = gender;
        slide.collectionDt = parsedCollected != null ? parsedCollected : parsedOrderedInstant;
        slide.orderDt = parsedOrderedInstant;

        return slide;

    }

    public static Slide load(Row dataRow, List<String> errorList, Map<String, Integer> columnIndexByNameMap) {

        if(dataRow.getCell(columnIndexByNameMap.get("Slide Bar Code")) == null) { errorList.add("Slide Bar Code is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Specialty")) == null) { errorList.add("Specialty is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Container")) == null) { errorList.add("Container is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Task")) == null) { errorList.add("Task is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("MRN")) == null) { errorList.add("MRN is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Patient Enterprise ID")) == null) { errorList.add("Patient Enterprise ID is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Birth Date")) == null) { errorList.add("Birth Date is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Patient Last Name")) == null) { errorList.add("Patient Last Name is NULL"); }
        if(dataRow.getCell(columnIndexByNameMap.get("Patient First Name")) == null) { errorList.add("Patient First Name is NULL"); }
        // Collected is allowed to be NULL, in which case we use the Ordered Instant as the Collected
        if(dataRow.getCell(columnIndexByNameMap.get("Ordered Instant")) == null) { errorList.add("Ordered Instant is NULL"); }

        if(!errorList.isEmpty()) {
            return null;
        }

        String gender = dataRow.getCell(columnIndexByNameMap.get("Gender")).getStringCellValue().substring(0, 1);

        Slide slide = new Slide();

        slide.slideBarCode = dataRow.getCell(columnIndexByNameMap.get("Slide Bar Code")).getStringCellValue();
        slide.service = dataRow.getCell(columnIndexByNameMap.get("Specialty")).getStringCellValue();
        slide.accNo = dataRow.getCell(columnIndexByNameMap.get("Specimen/Case ID")).getStringCellValue();
        slide.partId = dataRow.getCell(columnIndexByNameMap.get("Container")).getStringCellValue().split(",")[1].trim();
        slide.blockId =dataRow.getCell(columnIndexByNameMap.get("Container")).getStringCellValue().split(",")[2].trim();
        slide.slideNo = dataRow.getCell(columnIndexByNameMap.get("Container")).getStringCellValue().split(",")[3].trim();
        slide.stain = dataRow.getCell(columnIndexByNameMap.get("Task")).getStringCellValue();
        slide.mrn = dataRow.getCell(columnIndexByNameMap.get("MRN")).getStringCellValue();
        slide.empi = dataRow.getCell(columnIndexByNameMap.get("Patient Enterprise ID")).getStringCellValue();
        slide.dob = dataRow.getCell(columnIndexByNameMap.get("Birth Date")).getDateCellValue();
        slide.lastName = dataRow.getCell(columnIndexByNameMap.get("Patient Last Name")).getStringCellValue();
        slide.firstName = dataRow.getCell(columnIndexByNameMap.get("Patient First Name")).getStringCellValue();
        slide.gender = gender;
        slide.collectionDt = dataRow.getCell(columnIndexByNameMap.get("Collected")) != null
            ? dataRow.getCell(columnIndexByNameMap.get("Collected")).getDateCellValue()
            : dataRow.getCell(columnIndexByNameMap.get("Ordered Instant")).getDateCellValue();
        slide.orderDt = dataRow.getCell(columnIndexByNameMap.get("Ordered Instant")).getDateCellValue();
        
        return slide;
        
    }    
    
    public static String toManifestHeaderString() {
        
        return(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            "slideBarCode",
            "service",
            "accNo",
            "partId",
            "blockId",
            "slideNo",
            "stain",
            "mrn",
            "empi",
            "dob",
            "lastName",
            "firstName",
            "gender",
            "collectionDt",
            "orderDt"
        ));
        
    }
    
    public String toManifestString() {

        return(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            slideBarCode,
            service,
            accNo,
            partId,
            blockId,
            slideNo,
            stain,
            mrn,
            empi,
            dfDayOut.format(dob),
            lastName,
            firstName,
            gender,
            dfTimestampOut.format(collectionDt),
            dfTimestampOut.format(orderDt)
        ));
        
    }
    
}