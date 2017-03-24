package com.streever.data.quality;

import com.streever.parsers.FilePartByRegEx;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by dstreev on 2016-10-06.
 */
public class RecordLayoutValidatorTest {

    private ClassLoader classLoader = null;

    private static final String FILE_ONE = "MyTestFile_1.txt";
    private static final String FILE_TWO = "MyTestFile_2.txt";

    @Before
    public void init() {
        classLoader = getClass().getClassLoader();
    }

    @Test
    public void RegExTest001() {

        InputStream fileIn = null;
        try {
            fileIn = new FileInputStream(classLoader.getResource(FILE_ONE).getFile());
            RecordLayoutValidator comp = new RecordLayoutValidator();

            comp.setInputStream(fileIn);
            comp.setExpectedRecordFormatRegEx("^(\\w|\\s)*,-?\\d{1,10}\\.\\d{0,10},-?-?\\d{1,10}\\.\\d{0,10},-?\\d{1,10}\\.\\d{0,10}$");
            comp.setHasHeader(true);
            comp.setHasFooter(false);

            comp.validate();

            if (!comp.isValid()) {
                for (Long line: comp.getErrors().keySet()) {
                    System.out.println("Line #: " + line + " Content: '" + comp.getErrors().get(line) + "'");
                }
            }
            assertEquals("Validation Failed", true, comp.isValid());

        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse(true);
        } finally {
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

//    @Test
//    public void RegExTest002() {
//
//        InputStream fileIn = null;
//        try {
//            fileIn = new FileInputStream(classLoader.getResource(FILE_TWO).getFile());
//            FilePartByRegEx comp = new FilePartByRegEx();
//
//            comp.setInputStream(fileIn);
//            comp.setRegex("^(\\d{4}-\\d{2}-\\d{2}).*");
//            comp.setRegexGroupSupport(true);
//            comp.setOccurrence(3);
//
//            String value = comp.getValue();
//
//            assertEquals("Match not found:", "2015-06-30", value);
//
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            assertFalse(true);
//        } finally {
//            if (fileIn != null) {
//                try {
//                    fileIn.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//    }

}
