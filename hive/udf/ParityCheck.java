package radar.udf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Test helper (not used by Hive): reads "lang<TAB>raw_title" lines and prints
 * "is_content<TAB>normalized" so tests/unit/test_udf_parity.py can compare the
 * Java rules with the Python ones on the same inputs.
 */
public final class ParityCheck {
    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        PrintStream out = new PrintStream(System.out, false, "UTF-8");
        String line;
        while ((line = in.readLine()) != null) {
            int tab = line.indexOf('\t');
            String lang = line.substring(0, tab);
            String title = line.substring(tab + 1);
            String norm = TitleRules.normalizeTitle(title)
                    .replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r");
            out.print((TitleRules.isContentPage(lang, title) ? "1" : "0") + "\t" + norm + "\n");
        }
        out.flush();
    }
}
