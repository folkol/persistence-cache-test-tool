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

import static java.lang.Integer.parseInt;
import static java.util.UUID.randomUUID;

/**
 * <p>Naïve performance cache benchmark.</p>
 *
 * <p>It will use the ContentStorageLocalDisk to write n number of ContentTransfers,
 * containing a few random smaller components and one large component.</p>
 *
 * <p>(This may, or may not, reflect the performance of a real persistence cache.)</p>
 */
public class CacheThroughputTest {

    public static final StdRealContentId PRINCIPAL_RID = new StdRealContentId(18, 10, 100);
    public static final String DONE_COLOR = "\t\t\u001B[32mDone!\u001B[0m";
    public static final String BACON =
            "Bacon ipsum dolor amet fatback shoulder pig salami, turkey biltong ham\n" +
            "hock cow sirloin flank bacon tail jerky. Kevin brisket shank pork chop\n" +
            "meatball salami rump shankle ribeye ball tip picanha. Doner turducken\n" +
            "kevin, jowl shoulder spare ribs corned beef biltong ham hock boudin\n" +
            "brisket pork chop capicola rump prosciutto. Tri-tip tail hamburger\n" +
            "jerky ball tip. Turkey capicola filet mignon tail cow ham hock\n" +
            "meatball.\n" +
            "\n" +
            "Pork loin frankfurter jerky shankle doner venison meatloaf boudin\n" +
            "landjaeger prosciutto kevin porchetta. Fatback swine t-bone leberkas\n" +
            "salami ball tip pork. Hamburger pork fatback, shankle flank landjaeger\n" +
            "swine sausage chicken short ribs strip steak jerky salami. Drumstick\n" +
            "tongue ball tip strip steak.\n" +
            "\n" +
            "Pork belly kevin shoulder bresaola salami, t-bone ham alcatra chicken\n" +
            "tri-tip. Fatback pancetta prosciutto drumstick jowl frankfurter turkey\n" +
            "spare ribs landjaeger. Pork loin pig meatloaf shank picanha shankle\n" +
            "landjaeger leberkas beef ribs beef chuck t-bone. Pig fatback swine\n" +
            "salami pancetta. Short ribs pork shank turducken.";

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            printUsage();
        }

        int numContent = 100000;
        String cachePath = args[0];

        if (args.length == 2) {
            try {
                numContent = parseInt(args[1]);
            } catch (NumberFormatException nfe) {
                printUsage();
            }
        }

        ContentStorageLocalDisk cache = new ContentStorageLocalDisk(new File(cachePath));
        performTest(cache, numContent);
    }

    private static void printUsage() {
        System.err.println("usage: java -jar persistence-test.jar /path/to/cache [num_content]");
        System.exit(1);
    }

    private static void performTest(ContentStorageLocalDisk cache, int numContent) {
        long writeStart = System.nanoTime();
        writeTest(numContent, cache);
        double writeElapsed = (System.nanoTime() - writeStart) / 1e9;

        long readStart = System.nanoTime();
        readTest(numContent, cache);
        double readElapsed = (System.nanoTime() - readStart) / 1e9;

        System.out.printf("Writes per second:\t%13.2f%n", numContent / writeElapsed);
        System.out.printf("Reads per second:\t%13.2f%n", numContent / readElapsed);
    }

    private static void readTest(int numContent, ContentStorageLocalDisk cache) {
        System.out.printf("Reading x %d... ", numContent);
        for (int i = 1; i < numContent; i++) {
            cache.loadContentData(new StdRealContentId(1, i, 100));
        }
        System.out.println(DONE_COLOR);
    }

    private static void writeTest(int numContent, ContentStorageLocalDisk cache) {
        System.out.printf("Writing x %d...", numContent);
        for (int i = 1; i < numContent; i++) {
            cache.store(getContentTransfer(new StdRealContentId(1, i, 100), PRINCIPAL_RID));
            cache.sync();
        }
        System.out.println(DONE_COLOR);
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
        return new StdContentTransfer(rcid, cinfo, vinfo, createContentData(rcid), StatusCode.OK);
    }

    private static StdContentData createContentData(RealContentId rcid) {
        StdContentData scd = new StdContentData(rcid);
        scd.setComponent(randomUUID().toString(), randomUUID().toString(), randomUUID().toString());
        scd.setComponent(randomUUID().toString(), randomUUID().toString(), randomUUID().toString());
        scd.setComponent("foo", "bar", BACON);
        return scd;
    }
}
