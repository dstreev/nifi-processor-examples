package com.streever.data.quality;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * The intent of this class is to check the incoming stream, parse record (defaulted by \n)
 * and check that the contents match an expected format.
 *
 * Created by dstreev on 2016-10-06.
 */
public class RecordLayoutValidator {

    private String expectedRecordFormatRegEx = null;
    private Boolean hasHeader = Boolean.TRUE;
    private Boolean hasFooter = Boolean.FALSE;
    private Map<Long, String> errors = new TreeMap<Long, String>();
    private long recordCount = 0l;
    private boolean valid = Boolean.TRUE;

    private InputStream inputStream = null;

    public boolean isValid() {
        return valid;
    }

    public Map<Long, String> getErrors() {
        return errors;
    }

    public Boolean getHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(Boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public Boolean getHasFooter() {
        return hasFooter;
    }

    public void setHasFooter(Boolean hasFooter) {
        this.hasFooter = hasFooter;
    }

    public String getExpectedRecordFormatRegEx() {
        return expectedRecordFormatRegEx;
    }

    public void setExpectedRecordFormatRegEx(String expectedRecordFormatRegEx) {
        this.expectedRecordFormatRegEx = expectedRecordFormatRegEx;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
        init();
    }

    protected void init() {
        errors.clear();
        recordCount = 0l;
        valid = Boolean.TRUE;
    }

    public RecordLayoutValidator() {
    }

    public RecordLayoutValidator(InputStream inputStream) {
        setInputStream(inputStream);
    }

    public void validate() {
        String rtn = null;
        // Check that we have the minimum required elements
        if (inputStream == null || expectedRecordFormatRegEx == null) {
            return;
        }

        // Build the Compile RegEx Pattern
        Pattern regExPattern = Pattern.compile(expectedRecordFormatRegEx);
        recordCount = 0;

        // Wrap InputStream in a BufferedReader.
        BufferedReader inBuff = new BufferedReader(new InputStreamReader(inputStream));

        String line = null;
        try {
            // Iterate till the end of the buffer or when rtn isn't null.
            while ((line = inBuff.readLine()) != null && rtn == null) {
                if (hasHeader && recordCount == 0) {
                    // Header
                } else {
                    if (!isMatch(regExPattern, line)) {
                        // Doesn't Match. Record Line and offending record.
                        errors.put(recordCount, line);
                        valid = Boolean.FALSE;
                    }
                }
                recordCount++;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private boolean isMatch(Pattern pattern, String value) {
        Matcher matcher = pattern.matcher(value);
        return matcher.find();
    }

}
