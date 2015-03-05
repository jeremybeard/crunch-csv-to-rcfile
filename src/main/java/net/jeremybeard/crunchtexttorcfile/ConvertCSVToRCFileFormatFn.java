package net.jeremybeard.crunchtexttorcfile;

import java.io.UnsupportedEncodingException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;

@SuppressWarnings("serial")
public class ConvertCSVToRCFileFormatFn extends DoFn<String, Pair<Void, BytesRefArrayWritable>> {
    private int numCols;
    private BytesRefArrayWritable bytes;
    
    @Override    
    public void initialize() {
        numCols = getConfiguration().getInt("hive.io.rcfile.column.number.conf", 0);
        bytes = new BytesRefArrayWritable(numCols);
    }

    @Override
    public void process(String csvLine, Emitter<Pair<Void, BytesRefArrayWritable>> emitter) {
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
        
        emitter.emit(Pair.of((Void)null, bytes));
    }
}
