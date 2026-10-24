package radar.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;

/**
 * is_content_page(lang, raw_title): false for '-', main pages and namespace pages
 * (English names on every wiki plus the wiki's own localised names). Takes the
 * language because Hindi श्रेणी: or Japanese 特別: are namespaces only on their own wiki.
 */
@Description(name = "is_content_page",
        value = "_FUNC_(lang, raw_title) - false for '-', main pages and namespace pages such as Special: or श्रेणी:",
        extended = "Example: SELECT _FUNC_('en', 'Category:1990s_births');  -> false")
public class IsContentPage extends GenericUDF {

    private transient PrimitiveObjectInspector langIn;
    private transient PrimitiveObjectInspector titleIn;
    private final BooleanWritable out = new BooleanWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentLengthException("is_content_page takes 2 arguments: (lang, raw_title)");
        }
        for (int i = 0; i < 2; i++) {
            if (args[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i, "is_content_page expects strings");
            }
        }
        langIn = (PrimitiveObjectInspector) args[0];
        titleIn = (PrimitiveObjectInspector) args[1];
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Object l = args[0].get();
        Object t = args[1].get();
        String lang = l == null ? "" : PrimitiveObjectInspectorUtils.getString(l, langIn);
        String title = t == null ? null : PrimitiveObjectInspectorUtils.getString(t, titleIn);
        out.set(TitleRules.isContentPage(lang, title));
        return out;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "is_content_page(" + children[0] + ", " + children[1] + ")";
    }
}
