package com.new_erp.api.controller;

import com.new_erp.api.repository.DynamicDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
public class DataController {

    private static final Logger log = LoggerFactory.getLogger(DataController.class);
    private final DynamicDataRepository dynamicDataRepository;

    public DataController(DynamicDataRepository dynamicDataRepository) {
        this.dynamicDataRepository = dynamicDataRepository;
    }

    @GetMapping("/api/data/{schema}/{table}")
    public ResponseEntity<List<Map<String, Object>>> fetchData(
            @PathVariable String schema,
            @PathVariable String table,
            @RequestParam(required = false) String dateColumn,
            @RequestParam(required = false) String fromDate,
            @RequestParam(required = false) String toDate,
            @RequestParam Map<String, String> filterParams) {

        // Validate schema and table names
        if (!isValidSchemaAndTable(schema, table)) {
            log.warn("Invalid schema/table combination: {} / {}", schema, table);
            return ResponseEntity.badRequest().body(Collections.singletonList(Map.of("error", "Invalid schema or table name")));
        }

        try {
            // Create a map to hold valid filters
            Map<String, Object> filters = new HashMap<>();

            // Only add valid filters that are present in the database table
            for (Map.Entry<String, String> entry : filterParams.entrySet()) {
                String key = entry.getKey();
                // Validate if the key is a valid filter column
                if (isValidFilterColumn(schema, table, key)) {
                    filters.put(key, entry.getValue());
                } else {
                    log.warn("Invalid filter column: {}", key);
                }
            }

            List<Map<String, Object>> data = dynamicDataRepository.fetchData(schema, table, filters, dateColumn, fromDate, toDate);
            return ResponseEntity.ok(data);

        } catch (IllegalArgumentException e) {
            log.error("Error fetching data: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().body(Collections.singletonList(Map.of("error", e.getMessage())));
        } catch (Exception e) {
            log.error("Unexpected error: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Collections.singletonList(Map.of("error", "Internal Server Error")));
        }
    }

    // Method to validate the schema and table names
    private boolean isValidSchemaAndTable(String schema, String table) {
        List<String> allowedSchemas = List.of("mdm_internal");
        List<String> allowedTables = List.of("location_master_raw_tb"); // Add other tables as necessary

        return allowedSchemas.contains(schema) && allowedTables.contains(table);
    }

    // Method to validate filter columns
    private boolean isValidFilterColumn(String schema, String table, String column) {
        // Define valid columns for the specified table
        Set<String> validColumns = Set.of(
                "location_code",
                "location_name",
                "address",
                "country",
                "state",
                "city",
                "ownership_type",
                "remarks",
                "is_active",
                "is_deleted",
                "load_timestamp"
        );

        // Validate if the column exists in the valid column set
        boolean isValid = validColumns.contains(column);
        if (!isValid) {
            log.warn("Attempted to use invalid filter column: {} for table {} in schema {}", column, table, schema);
        }
        return isValid;
    }
}
