"""Data quality validation for processed weather DataFrames."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

TEMP_MIN_ALLOWED = -50.0
TEMP_MAX_ALLOWED = 50.0
RAIN_MIN_ALLOWED = 0.0
RAIN_MAX_ALLOWED = 500.0


class DataValidator:
    """Run basic data-quality checks on weather DataFrames."""

    def check_null_values(self, df: pd.DataFrame) -> dict[str, bool]:
        """Check each column for null values.

        Args:
            df: DataFrame to inspect.

        Returns:
            Mapping of column name to ``True`` when the column
            contains at least one null.
        """
        return {col: bool(df[col].isna().any()) for col in df.columns}

    def check_value_ranges(self, df: pd.DataFrame) -> dict[str, bool]:
        """Check that numeric columns stay within physical bounds.

        Temperature columns must be within [-50, 50] Celsius and
        rain within [0, 500] mm.

        Args:
            df: DataFrame to inspect.

        Returns:
            Mapping of column name to ``True`` when all values are
            within the allowed range (or the column is not checked).
        """
        checks: dict[str, bool] = {}

        for col in ("temp_max_c", "temp_min_c", "temp_range"):
            if col in df.columns:
                series = pd.to_numeric(df[col], errors="coerce")
                checks[col] = bool(
                    series.dropna().between(TEMP_MIN_ALLOWED, TEMP_MAX_ALLOWED).all()
                )

        if "rain_mm" in df.columns:
            rain = pd.to_numeric(df["rain_mm"], errors="coerce")
            checks["rain_mm"] = bool(
                rain.dropna().between(RAIN_MIN_ALLOWED, RAIN_MAX_ALLOWED).all()
            )

        return checks

    def full_validation_report(self, df: pd.DataFrame) -> str:
        """Build a human-readable validation summary.

        Args:
            df: DataFrame to validate.

        Returns:
            Multi-line report string describing nulls, ranges and an
            overall PASS/FAIL verdict.
        """
        nulls = self.check_null_values(df)
        ranges = self.check_value_ranges(df)

        lines: list[str] = ["=== Data Validation Report ==="]
        lines.append(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        lines.append("")

        lines.append("Null check:")
        for col, has_nulls in nulls.items():
            lines.append(f"  - {col}: {'FAIL (nulls found)' if has_nulls else 'OK'}")
        lines.append("")

        lines.append("Range check:")
        for col, in_range in ranges.items():
            lines.append(f"  - {col}: {'OK' if in_range else 'FAIL (out of range)'}")

        all_ok = not any(nulls.values()) and all(ranges.values())
        lines.append("")
        lines.append(f"Overall: {'PASS' if all_ok else 'FAIL'}")

        report = "\n".join(lines)
        logger.info("Validation report generated: %s", "PASS" if all_ok else "FAIL")
        return report
