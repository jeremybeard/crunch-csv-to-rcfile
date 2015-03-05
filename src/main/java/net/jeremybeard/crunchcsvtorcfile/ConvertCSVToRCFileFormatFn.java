package net.jeremybeard.crunchcsvtorcfile;

import java.io.UnsupportedEncodingException;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;

@SuppressWarnings("serial")
public class ConvertCSVToRCFileFormatFn extends MapFn<String, Pair<Void, BytesRefArrayWritable>> {
    private int numCols;
    private BytesRefArrayWritable bytes;
    
    @Override    
    public void initialize() {
        numCols = getConfiguration().getInt("hive.io.rcfile.column.number.conf", 0);
        bytes = new BytesRefArrayWritable(numCols);
    }

    @Override
    public Pair<Void, BytesRefArrayWritable> map(String csvLine) {
        byte[] fieldData = null;
        bytes.clear();
        
        String[] csvFields = csvLine.split(",");
        
        for (int i = 0; i < numCols; i++) {
            String fieldVal = csvFields[i];
            try {
                fieldData = fieldVal.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            BytesRefWritable cu = new BytesRefWritable(fieldData, 0, fieldData.length);
            bytes.set(i, cu);
        }
        
        return Pair.of((Void)null, bytes);
    }
}
