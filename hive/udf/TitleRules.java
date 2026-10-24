package radar.udf;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Title rules shared by the Hive UDFs. This is a line-by-line port of
 * mapreduce/common_title.py, so Hive and the MapReduce jobs drop and clean
 * exactly the same titles. tests/unit/test_udf_parity.py runs both versions on
 * the same inputs and fails if they ever disagree.
 *
 * Plain Java with no Hive imports, so it can be compiled and tested on its own.
 */
public final class TitleRules {

    public static final int MAX_TITLE_LEN = 255;

    /** English canonical namespace names and aliases (valid on all Wikipedias). */
    static final Set<String> CANONICAL = set(
            "book", "book talk", "category", "category talk", "draft", "draft talk",
            "education program", "education program talk", "event", "event talk", "file",
            "file talk", "gadget", "gadget definition", "gadget definition talk", "gadget talk",
            "help", "help talk", "image", "image talk", "media", "mediawiki", "mediawiki talk",
            "module", "module talk", "portal", "portal talk", "project", "project talk", "special",
            "talk", "template", "template talk", "timedtext", "timedtext talk", "topic", "user",
            "user talk", "wikipedia", "wikipedia talk", "wp", "wt");

    /** Localised namespace names per language. */
    static final Map<String, Set<String>> LOCAL = new HashMap<>();

    /** Main pages: huge, always-on traffic that is never an event. */
    static final Map<String, Set<String>> MAIN_PAGES = new HashMap<>();

    static {
        LOCAL.put("hi", set(
            "चित्र", "चित्र वार्ता", "प्रवेशद्वार", "प्रवेशद्वार वार्ता", "फ़ाइल", "फ़ाइल वार्ता",
            "मसौदा", "मसौदा वार्ता", "मीडिया", "मीडियाविकि", "मीडियाविकि वार्ता", "मॉड्यूल",
            "मॉड्यूल वार्ता", "वार्ता", "विकिपीडिया", "विकिपीडिया वार्ता", "विशेष", "श्रेणी",
            "श्रेणी वार्ता", "सदस्य", "सदस्य वार्ता", "सहायता", "सहायता वार्ता", "साँचा",
            "साँचा वार्ता"));
        LOCAL.put("ja", set(
            "category‐ノート", "mediawiki‐ノート", "portal‐ノート", "template‐ノート", "wikipedia-ノート",
            "wikipedia‐ノート", "カテゴリ", "テンプレート", "ノート", "ファイル", "ファイル-ノート", "ファイル‐ノート", "プロジェクト",
            "プロジェクト‐ノート", "ヘルプ", "ヘルプ‐ノート", "ポータル", "ポータル‐ノート", "メディア", "モジュール", "モジュール‐ノート",
            "利用者", "利用者-会話", "利用者‐会話", "特別", "画像", "画像‐ノート"));
        LOCAL.put("pt", set(
            "ajuda", "ajuda discussão", "anexo", "anexo discussão", "arquivo", "arquivo discussão",
            "categoria", "categoria discussão", "discussão", "especial", "ficheiro",
            "ficheiro discussão", "imagem", "imagem discussão", "livro", "livro discussão",
            "mediawiki discussão", "multimédia", "módulo", "módulo discussão", "portal discussão",
            "predefinição", "predefinição discussão", "rascunho", "rascunho discussão", "usuário",
            "usuário discussão", "usuário(a)", "usuário(a) discussão", "utilizador",
            "utilizador discussão", "wikipédia", "wikipédia discussão"));
        LOCAL.put("ta", set(
            "உதவி", "உதவி பேச்சு", "ஊடகம்", "சிறப்பு", "தொகுதி", "தொகுதி பேச்சு", "பகுப்பு",
            "பகுப்பு பேச்சு", "படிமப் பேச்சு", "படிமம்", "பயனர்", "பயனர் பேச்சு", "பேச்சு",
            "மீடியாவிக்கி", "மீடியாவிக்கி பேச்சு", "வரைவு", "வரைவு பேச்சு", "வலைவாசல்",
            "வலைவாசல் பேச்சு", "வார்ப்புரு", "வார்ப்புரு பேச்சு", "விக்கிப்பீடியா",
            "விக்கிப்பீடியா பேச்சு"));
        MAIN_PAGES.put("en", set("Main_Page"));
        MAIN_PAGES.put("hi", set("मुखपृष्ठ"));
        MAIN_PAGES.put("ja", set("メインページ"));
        MAIN_PAGES.put("pt", set("Wikipédia:Página_principal"));
        MAIN_PAGES.put("ta", set("முதற்_பக்கம்"));
    }

    private TitleRules() { }

    private static Set<String> set(String... items) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(items)));
    }

    /** False for '-', empty titles, main pages and namespace pages (same as is_content_page). */
    public static boolean isContentPage(String lang, String rawTitle) {
        if (rawTitle == null || rawTitle.isEmpty() || rawTitle.equals("-")) {
            return false;
        }
        Set<String> mains = MAIN_PAGES.get(lang);
        if (mains != null && mains.contains(rawTitle)) {
            return false;
        }
        int colon = rawTitle.indexOf(':');
        if (colon >= 0) {
            String p = prefixKey(rawTitle.substring(0, colon));
            Set<String> local = LOCAL.get(lang);
            if (CANONICAL.contains(p) || (local != null && local.contains(p))) {
                return false;
            }
        }
        return true;
    }

    /** URL-decode, underscores to spaces, no tabs/newlines, collapse spaces, max 255 characters. */
    public static String normalizeTitle(String rawTitle) {
        if (rawTitle == null) {
            return null;
        }
        String t = unquote(rawTitle);
        t = t.replace('_', ' ').replace('\t', ' ').replace('\r', ' ').replace('\n', ' ');
        t = collapseSpaces(t);
        if (t.codePointCount(0, t.length()) > MAX_TITLE_LEN) {
            t = t.substring(0, t.offsetByCodePoints(0, MAX_TITLE_LEN));
        }
        return t;
    }

    /** Python: title.split(":", 1)[0].replace("_", " ").strip().casefold() */
    static String prefixKey(String prefix) {
        return casefold(pyStrip(prefix.replace('_', ' ')));
    }

    /** Close match to Python's str.casefold(): upper then lower handles ß, ligatures and final sigma. */
    static String casefold(String s) {
        return s.toUpperCase(Locale.ROOT).toLowerCase(Locale.ROOT);
    }

    /** The characters Python's str.isspace() accepts, so split()/strip() behave the same. */
    static boolean isPySpace(char c) {
        return (c >= 0x09 && c <= 0x0D) || (c >= 0x1C && c <= 0x20) || c == 0x85 || c == 0xA0
                || c == 0x1680 || (c >= 0x2000 && c <= 0x200A) || c == 0x2028 || c == 0x2029
                || c == 0x202F || c == 0x205F || c == 0x3000;
    }

    static String pyStrip(String s) {
        int a = 0, b = s.length();
        while (a < b && isPySpace(s.charAt(a))) a++;
        while (b > a && isPySpace(s.charAt(b - 1))) b--;
        return s.substring(a, b);
    }

    /** Python: " ".join(t.split()) */
    static String collapseSpaces(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        boolean pendingSpace = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (isPySpace(c)) {
                pendingSpace = sb.length() > 0;
            } else {
                if (pendingSpace) {
                    sb.append(' ');
                    pendingSpace = false;
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Python's urllib.parse.unquote(s, errors="replace"): runs of ASCII characters
     * are percent-decoded to bytes and read as UTF-8 (bad bytes become U+FFFD),
     * non-ASCII characters are kept as they are, and a '%' not followed by two hex
     * digits stays literally. '+' is NOT turned into a space.
     */
    static String unquote(String s) {
        if (s.indexOf('%') < 0) {
            return s;
        }
        StringBuilder out = new StringBuilder(s.length());
        int i = 0, n = s.length();
        while (i < n) {
            int j = i;
            if (s.charAt(i) < 0x80) {
                while (j < n && s.charAt(j) < 0x80) j++;
                out.append(decodeAsciiRun(s.substring(i, j)));
            } else {
                while (j < n && s.charAt(j) >= 0x80) j++;
                out.append(s, i, j);
            }
            i = j;
        }
        return out.toString();
    }

    private static String decodeAsciiRun(String run) {
        if (run.indexOf('%') < 0) {
            return run;
        }
        byte[] buf = new byte[run.length()];
        int k = 0;
        for (int i = 0; i < run.length(); i++) {
            char c = run.charAt(i);
            if (c == '%' && i + 2 < run.length() && hex(run.charAt(i + 1)) >= 0 && hex(run.charAt(i + 2)) >= 0) {
                buf[k++] = (byte) (hex(run.charAt(i + 1)) * 16 + hex(run.charAt(i + 2)));
                i += 2;
            } else {
                buf[k++] = (byte) c;
            }
        }
        return decodeUtf8(buf, k);
    }

    /**
     * UTF-8 decoding with the same error handling as Python's errors="replace":
     * each maximal invalid subpart becomes one U+FFFD, and encoded surrogates
     * (ED A0..BF) or overlong forms are rejected. Written out by hand so the result
     * does not depend on the JVM version Hive runs on.
     */
    static String decodeUtf8(byte[] buf, int n) {
        StringBuilder sb = new StringBuilder(n);
        int i = 0;
        while (i < n) {
            int b = buf[i] & 0xFF;
            if (b < 0x80) {
                sb.append((char) b);
                i++;
                continue;
            }
            int need, lo = 0x80, hi = 0xBF;
            if (b >= 0xC2 && b <= 0xDF) {
                need = 1;
            } else if (b == 0xE0) {
                need = 2; lo = 0xA0;
            } else if ((b >= 0xE1 && b <= 0xEC) || b == 0xEE || b == 0xEF) {
                need = 2;
            } else if (b == 0xED) {
                need = 2; hi = 0x9F;
            } else if (b == 0xF0) {
                need = 3; lo = 0x90;
            } else if (b >= 0xF1 && b <= 0xF3) {
                need = 3;
            } else if (b == 0xF4) {
                need = 3; hi = 0x8F;
            } else {
                sb.append('\uFFFD');
                i++;
                continue;
            }
            int cp = b & (need == 1 ? 0x1F : need == 2 ? 0x0F : 0x07);
            int j = i + 1, got = 0;
            while (got < need && j < n) {
                int c = buf[j] & 0xFF;
                boolean ok = got == 0 ? (c >= lo && c <= hi) : (c >= 0x80 && c <= 0xBF);
                if (!ok) {
                    break;
                }
                cp = (cp << 6) | (c & 0x3F);
                j++;
                got++;
            }
            if (got == need) {
                sb.appendCodePoint(cp);
            } else {
                sb.append('\uFFFD');
            }
            i = j;
        }
        return sb.toString();
    }

    private static int hex(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    }
}
