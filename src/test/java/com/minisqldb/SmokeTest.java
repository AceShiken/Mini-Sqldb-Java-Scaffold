package com.minisqldb;

import com.minisqldb.config.DatabaseConfig;
import org.junit.jupiter.api.Test;


import java.nio.file.Path;


import static org.junit.jupiter.api.Assertions.*;


public class SmokeTest {
    @Test
    void openClose() throws Exception {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.dataDir = Path.of("build/test-data");
        try (Database db = Database.open(cfg)) {
            assertNotNull(db.catalog());
        }
    }
}
