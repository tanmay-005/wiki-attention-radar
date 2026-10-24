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
import org.apache.hadoop.io.Text;

/** normalize_title(title): the same cleaning MR1 applies (see TitleRules). */
@Description(name = "normalize_title",
        value = "_FUNC_(title) - URL-decodes, turns _ into spaces, collapses whitespace, caps at 255 characters",
        extended = "Example: SELECT _FUNC_('Virat_Kohli%27s_career');  -> Virat Kohli's career")
public class NormalizeTitle extends GenericUDF {

    private transient PrimitiveObjectInspector in;
    private final Text out = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("normalize_title takes exactly 1 argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "normalize_title expects a string");
        }
        in = (PrimitiveObjectInspector) args[0];
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Object o = args[0].get();
        if (o == null) {
            return null;
        }
        String s = PrimitiveObjectInspectorUtils.getString(o, in);
        if (s == null) {
            return null;
        }
        out.set(TitleRules.normalizeTitle(s));
        return out;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "normalize_title(" + children[0] + ")";
    }
}
