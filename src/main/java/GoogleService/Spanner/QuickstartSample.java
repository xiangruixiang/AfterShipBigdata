package GoogleService.Spanner;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;


public class QuickstartSample {
    public static void main(String... args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: QuickStartSample <instance_id> <database_id>");
            return;
        }
        // Instantiates a client
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();

        // Name of your instance & database.
        String instanceId = args[0];
        String databaseId = args[1];
        try {
            // Creates a database client
            DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                    options.getProjectId(), instanceId, databaseId));
            // Queries the database
            ResultSet resultSet = dbClient.singleUse().executeQuery(Statement.of("SELECT 1"));

            System.out.println("\n\nResults:");
            // Prints the results
            while (resultSet.next()) {
                System.out.printf("%d\n\n", resultSet.getLong(0));
            }
        } finally {
            // Closes the client which will free up the resources used
            spanner.close();
        }
    }
}