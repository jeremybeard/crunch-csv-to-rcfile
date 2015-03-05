package net.jeremybeard.crunchtexttorcfile;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceOutputFormat;

public class ConvertCSVToRCFile extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new ConvertCSVToRCFile(), args);
    }

    public int run(String[] args) throws Exception {
        int numCols = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];

        Pipeline pipeline = new MRPipeline(ConvertCSVToRCFile.class, getConf());
        
        PCollection<String> textRecords = pipeline.readTextFile(inputPath);
        
        PTable<Void, BytesRefArrayWritable> rcFileFormattedRecords =
                textRecords.parallelDo(new ConvertCSVToRCFileFormatFn(),
                                           Writables.tableOf(Writables.nulls(),
                                                             Writables.writables(BytesRefArrayWritable.class)));
        
        RCFileMapReduceOutputFormat.setColumnNumber(getConf(), numCols);
        
        pipeline.write(rcFileFormattedRecords, To.formattedFile(outputPath, RCFileMapReduceOutputFormat.class));
        
        PipelineResult result = pipeline.done();

        return result.succeeded() ? 0 : 1;
    }
}
