package UDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUdtf extends GenericUDTF {

    List<String> datalist = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIS)throws
            UDFArgumentException {

        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");

        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        String data = args[0].toString();

        String splitkey = args[1].toString();

        String[] words = data.split(splitkey);

        for (String word : words) {
            datalist.clear();
            datalist.add(word);

            forward(datalist);

        }

    }

    @Override
    public void close() throws HiveException {

    }

}
