import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

/** Exports only source identities whose ASCII spaces were changed by btrim. */
public final class InventoryRawKeyExport {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("usage: InventoryRawKeyExport OUTPUT_SQL");
        Path output = Path.of(args[0]).toAbsolutePath();
        Files.createDirectories(output.getParent());
        String url = "jdbc:mysql://127.0.0.1:23306/StoreServer?useSSL=false"
            + "&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=UTF-8"
            + "&connectTimeout=10000&socketTimeout=150000";
        try (var connection = DriverManager.getConnection(url,
                 System.getenv("MYSQL_USER"), System.getenv("MYSQL_PASSWORD"));
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            connection.setReadOnly(true);
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.setQueryTimeout(120);
                try (var rows = statement.executeQuery("SELECT COUNT(*) FROM sys_warehouse "
                        + "WHERE CAST(wh_id AS BINARY) <> CAST(TRIM(wh_id) AS BINARY)")) {
                    rows.next();
                    if (rows.getLong(1) != 0) {
                        throw new IllegalStateException("Warehouse map also requires raw-key restoration; export cancelled");
                    }
                }
            }
            writer.write("\\set ON_ERROR_STOP on\n\\encoding UTF8\nBEGIN;\n");
            writer.write("CREATE TEMP TABLE inventory_source_raw_keys("
                + "source_table text NOT NULL,source_id integer NOT NULL,uid integer NOT NULL,"
                + "upc_hex text NOT NULL,wh_id_hex text NOT NULL,last_update_time integer NOT NULL,"
                + "PRIMARY KEY(source_table,source_id)) ON COMMIT DROP;\n");
            writer.write("COPY inventory_source_raw_keys FROM STDIN WITH (FORMAT csv);\n");
            long total = 0;
            for (String table : new String[]{"mem_user_inventory", "mem_user_inventory_lock",
                                             "mem_user_inventory_tracking"}) {
                long count = 0;
                String query = "SELECT id,uid,HEX(upc),HEX(wh_id),last_update_time FROM " + table
                    + " WHERE CAST(upc AS BINARY) <> CAST(TRIM(upc) AS BINARY)"
                    + " OR CAST(wh_id AS BINARY) <> CAST(TRIM(wh_id) AS BINARY) ORDER BY id";
                try (var statement = connection.createStatement()) {
                    statement.setQueryTimeout(120);
                    try (var rows = statement.executeQuery(query)) {
                        while (rows.next()) {
                            writer.write(table + "," + rows.getInt(1) + "," + rows.getInt(2)
                                + "," + rows.getString(3) + "," + rows.getString(4)
                                + "," + rows.getInt(5) + "\n");
                            count++;
                        }
                    }
                }
                total += count;
                System.out.println(table + "_WHITESPACE_ROWS=" + count);
            }
            writer.write("\\.\n");
            writer.write("-- Keep this transaction open; execute 29_pg_restore_raw_inventory_keys.sql next.\n");
            connection.rollback();
            System.out.println("EXPORTED_RAW_KEYS=" + total);
        } catch (Exception failure) {
            Files.deleteIfExists(output);
            throw failure;
        }
    }
}
