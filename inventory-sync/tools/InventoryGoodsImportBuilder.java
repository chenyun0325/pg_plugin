import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.GZIPInputStream;

public final class InventoryGoodsImportBuilder {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                "usage: InventoryGoodsImportBuilder INPUT_TSV_GZ OUTPUT_SQL");
        }
        Path input = Path.of(args[0]).toAbsolutePath().normalize();
        Path output = Path.of(args[1]).toAbsolutePath().normalize();
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        Files.createDirectories(output.getParent());
        Files.deleteIfExists(partial);

        long rows = 0;
        long fatalInvalid = 0;
        long skippedInvalidUpcs = 0;
        long truncatedNames = 0;
        long exported = 0;
        long customerNameConflicts = 0;
        long conflictResolvedBySystemName = 0;
        long conflictResolvedByMinimumName = 0;
        long missingNameFallbacks = 0;
        List<String> diagnostics = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                 new GZIPInputStream(Files.newInputStream(input)), StandardCharsets.UTF_8));
             BufferedWriter writer = Files.newBufferedWriter(partial, StandardCharsets.UTF_8)) {
            writeHeader(writer);
            String line;
            while ((line = reader.readLine()) != null) {
                rows++;
                String[] fields = line.split("\t", -1);
                if (fields.length != 4) {
                    fatalInvalid++;
                    remember(diagnostics, "invalid TSV field count at row " + rows);
                    continue;
                }
                String upcHex = fields[0];
                String customerNameHex = fields[1];
                String systemNameHex = fields[3];
                int customerVariants;
                try {
                    customerVariants = Integer.parseInt(fields[2]);
                } catch (NumberFormatException error) {
                    fatalInvalid++;
                    remember(diagnostics, "invalid variant count at row " + rows);
                    continue;
                }
                String upc = decode(upcHex);
                if (upc.isEmpty() || codePoints(upc) > 255) {
                    skippedInvalidUpcs++;
                    remember(diagnostics, "skipped invalid UPC: " + printable(upc));
                    continue;
                }

                boolean hasCustomerName = !isNull(customerNameHex) && customerVariants > 0;
                boolean hasSystemName = !isNull(systemNameHex);
                String selectedNameHex;
                if (hasCustomerName) {
                    if (customerVariants > 1) {
                        customerNameConflicts++;
                        if (hasSystemName) {
                            selectedNameHex = systemNameHex;
                            conflictResolvedBySystemName++;
                        } else {
                            selectedNameHex = customerNameHex;
                            conflictResolvedByMinimumName++;
                        }
                    } else {
                        selectedNameHex = customerNameHex;
                    }
                } else if (hasSystemName) {
                    selectedNameHex = systemNameHex;
                } else {
                    selectedNameHex = upcHex;
                    missingNameFallbacks++;
                }

                String name = decode(selectedNameHex);
                if (name.isEmpty()) {
                    fatalInvalid++;
                    remember(diagnostics, "empty goods_name for UPC " + upc);
                    continue;
                }
                if (codePoints(name) > 256) {
                    name = truncateCodePoints(name, 256);
                    selectedNameHex = hex(name);
                    truncatedNames++;
                }
                writer.write(upcHex);
                writer.write(',');
                writer.write(selectedNameHex);
                writer.write(",0\n");
                exported++;
            }
            writeFooter(writer);
        }

        if (fatalInvalid > 0) {
            Files.deleteIfExists(partial);
            diagnostics.forEach(System.err::println);
            throw new IllegalStateException(
                "import build rejected: rows=" + rows + ", fatal_invalid=" + fatalInvalid);
        }
        Files.move(partial, output, StandardCopyOption.REPLACE_EXISTING);
        System.out.printf(
            "ROWS=%d EXPORTED=%d SKIPPED_INVALID_UPCS=%d TRUNCATED_NAMES=%d "
                + "CUSTOMER_NAME_CONFLICTS=%d "
                + "CONFLICTS_USING_SYSTEM_NAME=%d CONFLICTS_USING_MIN_NAME=%d "
                + "MISSING_NAME_USING_UPC=%d OUTPUT=%s%n",
            rows, exported, skippedInvalidUpcs, truncatedNames, customerNameConflicts,
            conflictResolvedBySystemName, conflictResolvedByMinimumName,
            missingNameFallbacks, output);
    }

    private static void writeHeader(BufferedWriter writer) throws Exception {
        writer.write("\\set ON_ERROR_STOP on\n");
        writer.write("\\encoding UTF8\n");
        writer.write("BEGIN;\n");
        writer.write("CREATE TEMP TABLE goods_map_import_raw(");
        writer.write("upc_hex text NOT NULL,goods_name_hex text NOT NULL,goods_version integer NOT NULL");
        writer.write(") ON COMMIT DROP;\n");
        writer.write("COPY goods_map_import_raw(upc_hex,goods_name_hex,goods_version) ");
        writer.write("FROM STDIN WITH (FORMAT csv);\n");
    }

    private static void writeFooter(BufferedWriter writer) throws Exception {
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

    private static void remember(List<String> diagnostics, String value) {
        if (diagnostics.size() < 50) {
            diagnostics.add(value);
        }
    }

    private static String decode(String value) {
        return new String(HexFormat.of().parseHex(value), StandardCharsets.UTF_8);
    }

    private static String hex(String value) {
        return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean isNull(String value) {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL");
    }

    private static int codePoints(String value) {
        return value.codePointCount(0, value.length());
    }

    private static String truncateCodePoints(String value, int maximum) {
        int end = value.offsetByCodePoints(0, maximum);
        return value.substring(0, end);
    }

    private static String printable(String value) {
        return value.replace("\r", "\\r").replace("\n", "\\n");
    }
}
