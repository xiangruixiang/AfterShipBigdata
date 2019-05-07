package GoogleService.Spanner;

import com.alibaba.fastjson.JSONObject;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Estimates the size of the {@code Struct}.
 */
public class ToJson extends PTransform<PCollection<Struct>, PCollection<String>> {

    public static ToJson create() {
        return new ToJson();
    }

    private ToJson() {
    }

    @Override
    public PCollection<String> expand(PCollection<Struct> input) {
        return input.apply(ParDo.of(new EstimateStructSizeFn()));
    }

    /**
     * Estimates the size of a Spanner row. For simplicity, arrays and structs aren't supported.
     */
    public static class EstimateStructSizeFn extends DoFn<Struct, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
           // Struct row = c.element();
            JSONObject jsonObject = JSONObject.parseObject(c.element().toString());

            c.output(jsonObject.toJSONString());
        }
    }

}