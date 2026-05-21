import threading

import pytest

from archive import TickArchive, default_db_path


class TestDefaultDbPath:
    def test_format(self):
        path = default_db_path()
        # e.g. ./data/raw_price_2026-05-21_133509.db
        assert path.startswith("./data/raw_price_")
        assert path.endswith(".db")
        # Filename part after prefix should be YYYY-MM-DD_HHMMSS (17 chars)
        name = path.removeprefix("./data/raw_price_").removesuffix(".db")
        assert len(name) == 17, f"Expected 17 chars like 2026-05-21_134727, got {name!r}"


class TestTickArchive:
    def test_schema_columns(self):
        archive = TickArchive(":memory:")
        cur = archive._conn.execute("PRAGMA table_info(ticks)")
        columns = [row[1] for row in cur.fetchall()]
        assert columns == ["timestamp", "symbol", "bid", "offer", "state"]
        archive.close()

    def test_insert_and_retrieve(self):
        archive = TickArchive(":memory:")
        archive.insert(1_716_000_000.0, "CS.D.GBPUSD.TODAY.IP", 1.2345, 1.2347, "Open")
        row = archive._conn.execute("SELECT * FROM ticks").fetchone()
        assert row == (1_716_000_000.0, "CS.D.GBPUSD.TODAY.IP", 1.2345, 1.2347, "Open")
        archive.close()

    def test_multiple_inserts(self):
        archive = TickArchive(":memory:")
        for i in range(5):
            archive.insert(float(i), f"SYM{i}", float(i), float(i) + 0.01, "Open")
        count = archive._conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
        assert count == 5
        archive.close()

    def test_thread_safe_concurrent_inserts(self):
        archive = TickArchive(":memory:")
        errors = []

        def insert_rows(n):
            try:
                for i in range(n):
                    archive.insert(float(i), "SYM", 1.0, 1.01, "Open")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=insert_rows, args=(50,)) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        count = archive._conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
        assert count == 200
        archive.close()

    def test_create_data_directory(self, tmp_path):
        db_path = str(tmp_path / "subdir" / "test.db")
        archive = TickArchive(db_path)
        archive.insert(1.0, "SYM", 1.0, 1.01, "Open")
        row = archive._conn.execute("SELECT symbol FROM ticks").fetchone()
        assert row[0] == "SYM"
        archive.close()
