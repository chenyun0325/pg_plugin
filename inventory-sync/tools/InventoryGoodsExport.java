import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

public final class InventoryGoodsExport {
    private static final String QUERY = """
        SELECT
            resolved.upc,
            MIN(resolved.goods_name) AS goods_name,
            COUNT(DISTINCT resolved.goods_name) AS name_variants
        FROM (
            SELECT
                op.upc,
                COALESCE(NULLIF(cp1.product, ''), NULLIF(sp1.product, '')) AS goods_name
            FROM (
                SELECT DISTINCT uid, upc
                  FROM StoreServer.mem_user_inventory
                 WHERE upc <> ''
            ) op
            LEFT JOIN StoreServer.mem_customer_product cp1 FORCE INDEX (u_key)
              ON cp1.upc = op.upc AND cp1.uid = op.uid
            LEFT JOIN StoreServer.sys_product sp1 FORCE INDEX (u_key)
              ON sp1.upc = op.upc
        ) resolved
        GROUP BY resolved.upc
        ORDER BY resolved.upc
        """;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: InventoryGoodsExport OUTPUT_SQL");
        }
        String user = requiredEnv("MYSQL_USER");
        String password = requiredEnv("MYSQL_PASSWORD");
        String url = "jdbc:mysql://127.0.0.1:23306/StoreServer"
            + "?useSSL=false&allowPublicKeyRetrieval=true"
            + "&serverTimezone=UTC&characterEncoding=UTF-8"
            + "&connectTimeout=10000&socketTimeout=600000";

        Path output = Path.of(args[0]).toAbsolutePath().normalize();
        Files.createDirectories(output.getParent());
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        Files.deleteIfExists(partial);

        int uniqueUpcs = 0;
        int invalidCount = 0;
        int conflictCount = 0;
        List<String> diagnostics = new ArrayList<>();

        try (BufferedWriter writer = Files.newBufferedWriter(partial, StandardCharsets.UTF_8);
             Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement statement = connection.prepareStatement(
                 QUERY, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            connection.setReadOnly(true);
            statement.setFetchSize(Integer.MIN_VALUE);
            statement.setQueryTimeout(600);

            writer.write("\\set ON_ERROR_STOP on\n");
            writer.write("\\encoding UTF8\n");
            writer.write("BEGIN;\n");
            writer.write("CREATE TEMP TABLE goods_map_import_raw(");
            writer.write("upc_hex text NOT NULL,goods_name_hex text NOT NULL,goods_version integer NOT NULL");
            writer.write(") ON COMMIT DROP;\n");
            writer.write("COPY goods_map_import_raw(upc_hex,goods_name_hex,goods_version) ");
            writer.write("FROM STDIN WITH (FORMAT csv);\n");

            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    uniqueUpcs++;
                    String upc = rows.getString(1);
                    String name = rows.getString(2);
                    int variants = rows.getInt(3);
                    upc = upc == null ? "" : upc;

                    if (upc.isEmpty() || codePoints(upc) > 255) {
                        invalidCount++;
                        remember(diagnostics, "invalid UPC: " + upc);
                        continue;
                    }
                    if (name == null || name.isEmpty() || codePoints(name) > 256) {
                        invalidCount++;
                        remember(diagnostics, "invalid/missing goods_name for UPC " + upc);
                        continue;
                    }
                    if (variants > 1) {
                        conflictCount++;
                        remember(diagnostics, "conflicting goods names for UPC " + upc
                            + " (variants=" + variants + ")");
                        continue;
                    }
                    writer.write(hex(upc));
                    writer.write(',');
                    writer.write(hex(name));
                    writer.write(",0\n");
                }
            }

            writer.write("\\.\n");
            writer.write("""
                CREATE TEMP TABLE goods_map_import ON COMMIT DROP AS
                SELECT
                    convert_from(decode(upc_hex,'hex'),'UTF8') AS upc,
                    convert_from(decode(goods_name_hex,'hex'),'UTF8')::varchar(256) AS goods_name,
                    goods_version
                FROM goods_map_import_raw;
                ALTER TABLE goods_map_import ALTER COLUMN upc TYPE varchar(255);
                ALTER TABLE goods_map_import ADD PRIMARY KEY(upc);
                INSERT INTO operations.inventory_goods_map(upc,goods_name,goods_version)
                SELECT upc,goods_name,goods_version
                FROM goods_map_import
                ON CONFLICT(upc) DO UPDATE SET
                    goods_name=EXCLUDED.goods_name,
                    goods_version=EXCLUDED.goods_version;
                DO $$
                DECLARE expected_count bigint; actual_count bigint;
                BEGIN
                    SELECT count(*) INTO expected_count FROM goods_map_import;
                    SELECT count(*) INTO actual_count
                    FROM goods_map_import source
                    JOIN operations.inventory_goods_map target
                      ON target.upc=source.upc
                     AND target.goods_name=source.goods_name
                     AND target.goods_version=source.goods_version;
                    IF actual_count <> expected_count THEN
                        RAISE EXCEPTION 'goods map verification failed: expected %, got %',
                            expected_count,actual_count;
                    END IF;
                END;
                $$;
                COMMIT;
                SELECT count(*) AS imported_goods FROM operations.inventory_goods_map;
                """);
        }

        if (invalidCount > 0 || conflictCount > 0) {
            Files.deleteIfExists(partial);
            diagnostics.forEach(System.err::println);
            throw new IllegalStateException(
                "export rejected: invalid=" + invalidCount + ", conflicts=" + conflictCount);
        }

        Files.move(partial, output, StandardCopyOption.REPLACE_EXISTING);
        System.out.printf("UNIQUE_UPCS=%d EXPORTED=%d OUTPUT=%s%n",
            uniqueUpcs, uniqueUpcs, output);
    }

    private static void remember(List<String> diagnostics, String value) {
        if (diagnostics.size() < 50) {
            diagnostics.add(value);
        }
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing environment variable " + name);
        }
        return value;
    }

    private static int codePoints(String value) {
        return value.codePointCount(0, value.length());
    }

    private static String hex(String value) {
        return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
    }
}
