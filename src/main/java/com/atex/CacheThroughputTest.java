package com.atex;

import com.polopoly.service.cm.api.RealContentId;
import com.polopoly.service.cm.api.StatusCode;
import com.polopoly.service.cm.api.content.ContentTransfer;
import com.polopoly.service.cm.basic.storage.ContentStorageLocalDisk;
import com.polopoly.service.cm.standard.StdContentId;
import com.polopoly.service.cm.standard.StdRealContentId;
import com.polopoly.service.cm.standard.content.StdContentData;
import com.polopoly.service.cm.standard.content.StdContentInfo;
import com.polopoly.service.cm.standard.content.StdContentTransfer;
import com.polopoly.service.cm.standard.content.StdVersionInfo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static java.lang.Integer.parseInt;

/**
 * Naïve performance cache benchmark.
 *
 * It will use the ContentStorageLocalDisk to write n number of ContentTransfers.
 *
 * This may, or may not, reflect the performance of a real persistence cache.
 */
public class CacheThroughputTest {

    public static final StdRealContentId PRINCIPAL_RID = new StdRealContentId(18, 10, 100);
    public static final String DONE_COLOR = "\t\t\u001B[32mDone!\u001B[0m";

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            printUsage();
        }

        int numContent = 10000;
        String cachePath = args[0];

        if (args.length == 2) {
            try {
                numContent = parseInt(args[1]);
            } catch (NumberFormatException nfe) {
                printUsage();
            }
        }

        performTest(cachePath, numContent);
    }

    private static void printUsage() {
        System.err.println("usage: java -jar persistence-cache-test.jar /path/to/cache [num_content]");
        System.exit(1);
    }

    private static void performTest(String cacheLocation, int numContent) throws IOException {
        ContentStorageLocalDisk cache = new ContentStorageLocalDisk(new File(cacheLocation));

        System.out.printf("Writing x %d...", numContent);
        long writeStart = System.nanoTime();
        for (int i = 1; i < numContent; i++) {
            cache.store(getContentTransfer(new StdRealContentId(1, i, 100), PRINCIPAL_RID));
            cache.sync();
        }
        double writeElapsed = (System.nanoTime() - writeStart) / 1e9;
        System.out.println(DONE_COLOR);

        System.out.printf("Reading x %d... ", numContent);
        long readStart = System.nanoTime();
        for (int i = 1; i < numContent; i++) {
            cache.loadContentData(new StdRealContentId(1, i, 100));
        }
        System.out.println(DONE_COLOR);
        double readElapsed = (System.nanoTime() - readStart) / 1e9;

        System.out.printf("Writes per second:\t%13.2f%n", numContent / writeElapsed);
        System.out.printf("Reads per second:\t%13.2f%n", numContent / readElapsed);
    }

    static ContentTransfer getContentTransfer(RealContentId rcid, RealContentId pcid) {
        StdContentInfo cinfo = new StdContentInfo(
                rcid.getContentId(),
                true,
                "principal_1",
                "principal_2",
                (long) (rcid.getMinor() * 1000),
                new StdContentId(1, 1),
                new StdContentId(1, 2),
                rcid);
        StdVersionInfo vinfo = new StdVersionInfo(
                rcid,
                0,
                rcid.getCommitId() + (long) (rcid.getMinor() * 1000),
                "principal_3",
                pcid);
        return new StdContentTransfer(rcid, cinfo, vinfo, new StdContentData(rcid), StatusCode.OK);
    }
}
