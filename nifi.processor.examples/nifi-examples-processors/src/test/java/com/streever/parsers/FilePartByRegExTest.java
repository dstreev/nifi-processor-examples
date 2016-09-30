package com.streever.parsers;

import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

/**
 * Created by dstreev on 2016-09-30.
 */
public class FilePartByRegExTest {

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
            FilePartByRegEx comp = new FilePartByRegEx();

            comp.setInputStream(fileIn);
            comp.setRegex("^PortFin,([-|+]\\d{4}).*");
            comp.setRegexGroupSupport(true);
            comp.setOccurrence(1);

            String value = comp.getValue();

            assertEquals("Match not found:", "-7688", value);

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

    @Test
    public void RegExTest002() {

        InputStream fileIn = null;
        try {
            fileIn = new FileInputStream(classLoader.getResource(FILE_TWO).getFile());
            FilePartByRegEx comp = new FilePartByRegEx();

            comp.setInputStream(fileIn);
            comp.setRegex("^(\\d{4}-\\d{2}-\\d{2}).*");
            comp.setRegexGroupSupport(true);
            comp.setOccurrence(3);

            String value = comp.getValue();

            assertEquals("Match not found:", "2015-06-30", value);

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

}
