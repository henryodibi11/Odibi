import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from odibi.config import NodeConfig
from odibi.connections.local import LocalConnection
from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.patterns.merge import MergePattern
from odibi.patterns.scd2 import SCD2Pattern
from odibi.state import LocalFileStateBackend, StateManager

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("TestCampaign")

BASE_DIR = Path("test_campaign")
DATA_DIR = BASE_DIR / "data"
STATE_DIR = BASE_DIR / ".odibi"


class TestCampaign:
    def __init__(self):
        # Enable Arrow (Default) now that validation is fixed
        self.engine = PandasEngine()
        self.results = {}
        self.connection = None

    def setup(self):
        """Prepare test environment."""
        if BASE_DIR.exists():
            shutil.rmtree(BASE_DIR)
        DATA_DIR.mkdir(parents=True)
        STATE_DIR.mkdir(parents=True)
        logger.info(f"Initialized test environment at {BASE_DIR}")
        # Initialize connection relative to CWD because we pass absolute paths mostly
        # OR simply point it to "."
        self.connection = LocalConnection(base_path=".")

        # Register connection as 'local_data' for resolution tests (pointing to data dir)
        data_conn = LocalConnection(base_path=str(DATA_DIR))
        self.engine = PandasEngine(connections={"local_data": data_conn})

    def fail(self, phase: str, message: str):
        logger.error(f"[{phase}] FAILED: {message}")
        self.results[phase] = "FAILED"
        raise RuntimeError(f"{phase} failed: {message}")

    def pass_phase(self, phase: str):
        logger.info(f"[{phase}] PASSED")
        self.results[phase] = "PASSED"

    def _generate_dummy_data(self, path: Path, rows: int = 10):
        """Generate simple CSV data."""
        df = pd.DataFrame(
            {
                "id": range(1, rows + 1),
                "name": [f"Name_{i}" for i in range(1, rows + 1)],
                "value": np.random.randint(1, 100, rows),
                "updated_at": datetime.now(),
            }
        )
        df.to_csv(path, index=False)
        return df

    # ==========================================
    # Phase 1: Core Foundations
    # ==========================================
    def run_phase_1(self):
        phase = "Phase 1 - Core Foundations"
        logger.info(f"Starting {phase}...")

        try:
            # Test 1: Read CSV
            input_csv = DATA_DIR / "source.csv"
            self._generate_dummy_data(input_csv)

            df = self.engine.read(self.connection, format="csv", path=str(input_csv))
            if len(df) != 10:
                self.fail(phase, f"Read count mismatch. Expected 10, got {len(df)}")

            # Test 2: Write Parquet
            output_parquet = DATA_DIR / "output.parquet"
            self.engine.write(df, self.connection, format="parquet", path=str(output_parquet))

            if not output_parquet.exists():
                self.fail(phase, "Output parquet file not created")

            # Test 3: Validation
            schema_rules = {"required_columns": ["id", "name"], "types": {"id": "int"}}
            failures = self.engine.validate_schema(df, schema_rules)
            if failures:
                self.fail(phase, f"Validation failed incorrectly: {failures}")

            bad_rules = {"types": {"id": "bool"}}  # Should fail
            failures = self.engine.validate_schema(df, bad_rules)
            if not failures:
                self.fail(phase, "Validation should have failed for type mismatch")

            self.pass_phase(phase)
        except Exception as e:
            self.fail(phase, str(e))

    # ==========================================
    # Phase 3: State & HWM (Skipping Phase 2 Catalog)
    # ==========================================
    def run_phase_3(self):
        phase = "Phase 3 - State & HWM"
        logger.info(f"Starting {phase}...")

        try:
            backend = LocalFileStateBackend(project_root=str(BASE_DIR))
            manager = StateManager(project_root=str(BASE_DIR), backend=backend)

            # Test HWM
            manager.set_hwm("test_node", "2024-01-01")
            val = manager.get_hwm("test_node")

            if val != "2024-01-01":
                self.fail(phase, f"HWM mismatch. Expected '2024-01-01', got {val}")

            # Verify persistence
            import json

            with open(BASE_DIR / ".odibi/state.json") as f:
                state = json.load(f)
                if state["incremental"]["test_node"] != "2024-01-01":
                    self.fail(phase, "State not persisted to disk correctly")

            self.pass_phase(phase)
        except Exception as e:
            self.fail(phase, str(e))

    # ==========================================
    # Phase 4: Base Patterns (Merge)
    # ==========================================
    def run_phase_4(self):
        phase = "Phase 4 - Merge Pattern"
        logger.info(f"Starting {phase}...")

        try:
            target_path = DATA_DIR / "merge_target.parquet"

            # 1. Initial State
            initial_df = pd.DataFrame({"id": [1, 2], "val": ["A", "B"]})
            initial_df.to_parquet(target_path)

            # 2. Incoming Data (Update 2, Insert 3)
            source_df = pd.DataFrame({"id": [2, 3], "val": ["B_updated", "C"]})

            # 3. Execute Pattern
            # Note: We must use absolute path for target because of the Path Resolution Gap
            params = {"target": str(target_path.absolute()), "keys": ["id"], "strategy": "upsert"}

            config = NodeConfig(name="merge_test", transformer="merge", params=params)
            pattern = MergePattern(self.engine, config)

            p_ctx = PandasContext()
            ctx = EngineContext(context=p_ctx, df=source_df, engine_type=EngineType.PANDAS)
            pattern.execute(ctx)

            # 4. Validate
            result_df = pd.read_parquet(target_path)
            result_df = result_df.sort_values("id").reset_index(drop=True)

            # Expected: 1=A, 2=B_updated, 3=C
            if len(result_df) != 3:
                self.fail(phase, f"Result count mismatch. Expected 3, got {len(result_df)}")

            row_2 = result_df[result_df["id"] == 2].iloc[0]
            if row_2["val"] != "B_updated":
                self.fail(phase, f"Update failed. Expected B_updated, got {row_2['val']}")

            row_3 = result_df[result_df["id"] == 3].iloc[0]
            if row_3["val"] != "C":
                self.fail(phase, "Insert failed.")

            self.pass_phase(phase)
        except Exception as e:
            self.fail(phase, str(e))

    # ==========================================
    # Phase 5: Advanced Patterns (SCD2)
    # ==========================================
    def run_phase_5(self):
        phase = "Phase 5 - SCD2 Pattern"
        logger.info(f"Starting {phase}...")

        try:
            target_path = DATA_DIR / "scd2_target.parquet"

            # 1. Initial State
            initial_df = pd.DataFrame(
                {"id": [1], "attr": ["old"], "valid_to": [None], "is_current": [True]}
            )
            # Need to ensure types for None/NaN handling
            initial_df["valid_to"] = initial_df["valid_to"].astype(object)
            initial_df.to_parquet(target_path)

            # 2. Incoming Change
            source_df = pd.DataFrame({"id": [1], "attr": ["new"], "eff_date": ["2024-01-02"]})

            # 3. Execute Pattern
            params = {
                "target": str(target_path.absolute()),
                "keys": ["id"],
                "track_cols": ["attr"],
                "effective_time_col": "eff_date",
                "end_time_col": "valid_to",
                "current_flag_col": "is_current",
            }

            config = NodeConfig(name="scd2_test", transformer="scd2", params=params)
            pattern = SCD2Pattern(self.engine, config)

            p_ctx = PandasContext()
            ctx = EngineContext(context=p_ctx, df=source_df, engine_type=EngineType.PANDAS)

            # SCD2 returns the full dataframe to be written.
            # The pattern implementation in Odibi returns the context, but the execute method returns df.
            # Wait, look at scd2.py:
            # result_ctx = scd2(context, scd_params)
            # return result_ctx.df
            # And scd2 function returns context.with_df(result)

            # However, scd2 logic (pandas) calculates result but DOES NOT WRITE IT.
            # It relies on the Node execution loop to write it?
            # No, scd2 implementation says: "Returns the FULL history dataset (to be written via Overwrite)."
            # But wait, _merge_pandas actually WRITES inside the transformer.
            # _scd2_pandas DOES NOT WRITE. It returns the DF.
            # So we must write it manually here to simulate the Node behavior.

            full_history_df = pattern.execute(ctx)

            # Simulate Overwrite Write
            full_history_df.to_parquet(target_path)

            # 4. Validate
            result = pd.read_parquet(target_path).sort_values("valid_to", na_position="last")

            if len(result) != 2:
                self.fail(phase, f"Expected 2 rows (history + current), got {len(result)}")

            old_rec = result.iloc[0]
            new_rec = result.iloc[1]

            if old_rec["is_current"] is not False or old_rec["valid_to"] != "2024-01-02":
                self.fail(phase, f"Old record not closed correctly: {old_rec.to_dict()}")

            if new_rec["is_current"] is not True or new_rec["attr"] != "new":
                self.fail(phase, "New record not created correctly")

            self.pass_phase(phase)
        except Exception as e:
            # Debug
            import traceback

            traceback.print_exc()
            self.fail(phase, str(e))

    # ==========================================
    # Phase 6: Logical Path Resolution
    # ==========================================
    def run_phase_resolution(self):
        phase = "Phase 6 - Logical Path Resolution"
        logger.info(f"Starting {phase}...")
        try:
            # Create source
            source_df = pd.DataFrame({"id": [1], "val": ["A"]})

            # Config with logical target: "local_data.logical_table.parquet"
            params = {
                "target": "local_data.logical_table.parquet",
                "keys": ["id"],
                "strategy": "upsert",
            }

            config = NodeConfig(name="resolution_test", transformer="merge", params=params)
            pattern = MergePattern(self.engine, config)

            # Context
            p_ctx = PandasContext()
            ctx = EngineContext(
                context=p_ctx, df=source_df, engine_type=EngineType.PANDAS, engine=self.engine
            )

            pattern.execute(ctx)

            # Verify file exists at resolved path
            expected_path = DATA_DIR / "logical_table.parquet"
            if not expected_path.exists():
                self.fail(phase, f"File not found at expected path: {expected_path}")

            self.pass_phase(phase)
        except Exception as e:
            self.fail(phase, str(e))

    # ==========================================
    # Phase 11: Scaling (Mini)
    # ==========================================
    def run_phase_11(self):
        phase = "Phase 11 - Scaling"
        logger.info(f"Starting {phase}...")

        try:
            # Generate 10k rows
            rows = 10000
            input_csv = DATA_DIR / "scale_source.csv"
            self._generate_dummy_data(input_csv, rows=rows)

            import time

            start = time.time()

            df = self.engine.read(self.connection, format="csv", path=str(input_csv))
            # Simple transform
            df["new_col"] = df["value"] * 2

            output_parquet = DATA_DIR / "scale_output.parquet"
            self.engine.write(df, self.connection, format="parquet", path=str(output_parquet))

            duration = time.time() - start
            logger.info(f"Processed {rows} rows in {duration:.2f}s")

            if duration > 5.0:  # Should be very fast for 10k
                logger.warning("Performance seems slow (>5s for 10k rows)")

            self.pass_phase(phase)
        except Exception as e:
            self.fail(phase, str(e))

    def run_all(self):
        self.setup()
        print("\n=== ODIBI TEST CAMPAIGN ===\n")
        self.run_phase_1()
        self.run_phase_3()
        self.run_phase_4()
        self.run_phase_5()
        self.run_phase_resolution()
        self.run_phase_11()
        print("\n=== CAMPAIGN FINISHED ===\n")
        print(self.results)


if __name__ == "__main__":
    TestCampaign().run_all()
