package GoogleService.Spanner;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Estimates the size of the {@code Struct}.
 */
public class EstimateSize extends PTransform<PCollection<Struct>, PCollection<Long>> {

    public static EstimateSize create() {
        return new EstimateSize();
    }

    private EstimateSize() {
    }

    @Override
    public PCollection<Long> expand(PCollection<Struct> input) {
        return input.apply(ParDo.of(new EstimateStructSizeFn()));
    }

    /**
     * Estimates the size of a Spanner row. For simplicity, arrays and structs aren't supported.
     */
    public static class EstimateStructSizeFn extends DoFn<Struct, Long> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Struct row = c.element();
            long sum = 0;
            for (int i = 0; i < row.getColumnCount(); i++) {
                if (row.isNull(i)) {
                    continue;
                }

                switch (row.getColumnType(i).getCode()) {
                    case BOOL:
                        sum += 1;
                        break;
                    case INT64:
                    case FLOAT64:
                        sum += 8;
                        break;
                    case TIMESTAMP:
                    case DATE:
                        sum += 12;
                        break;
                    case BYTES:
                        sum += row.getBytes(i).length();
                        break;
                    case STRING:
                        sum += row.getString(i).length();
                        break;
                    case ARRAY:
                        throw new IllegalArgumentException("Arrays are not supported :(");
                    case STRUCT:
                        throw new IllegalArgumentException("Structs are not supported :(");
                    default:
                        throw new IllegalArgumentException("Unsupported type :(");
                }
            }
            c.output(sum);
        }
    }

}