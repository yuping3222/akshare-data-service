"""tests/test_store_missing_ranges.py

Comprehensive tests for missing_ranges.py
"""


from akshare_data.store.missing_ranges import find_missing_ranges


class TestFindMissingRanges:
    """Tests for find_missing_ranges function."""

    def test_no_existing_ranges(self):
        """Test when there are no existing ranges returns full range."""
        result = find_missing_ranges("2024-01-01", "2024-01-10", [])
        assert result == [("2024-01-01", "2024-01-10")]

    def test_single_existing_range_fully_covered(self):
        """Test when existing range fully covers target range."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-01", "2024-01-10")]
        )
        assert result == []

    def test_single_existing_range_at_start(self):
        """Test when existing range is at the start."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-01", "2024-01-05")]
        )
        assert result == [("2024-01-06", "2024-01-10")]

    def test_single_existing_range_at_end(self):
        """Test when existing range is at the end."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-06", "2024-01-10")]
        )
        assert result == [("2024-01-01", "2024-01-05")]

    def test_single_existing_range_in_middle(self):
        """Test when existing range is in the middle."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-04", "2024-01-07")]
        )
        assert result == [("2024-01-01", "2024-01-03"), ("2024-01-08", "2024-01-10")]

    def test_multiple_existing_ranges_sorted(self):
        """Test with multiple existing ranges that need sorting."""
        result = find_missing_ranges(
            "2024-01-01",
            "2024-01-15",
            [("2024-01-10", "2024-01-15"), ("2024-01-01", "2024-01-05")],
        )
        assert result == [("2024-01-06", "2024-01-09")]

    def test_existing_range_extends_beyond_target_end(self):
        """Test when existing range extends beyond target end."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-08", "2024-01-20")]
        )
        assert result == [("2024-01-01", "2024-01-07")]

    def test_existing_range_starts_beyond_target_start(self):
        """Test when existing range starts beyond target start."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-15", "2024-01-20")]
        )
        assert result == [("2024-01-01", "2024-01-10")]

    def test_adjacent_existing_ranges(self):
        """Test with adjacent existing ranges (no gap between them)."""
        result = find_missing_ranges(
            "2024-01-01",
            "2024-01-10",
            [("2024-01-01", "2024-01-03"), ("2024-01-04", "2024-01-06")],
        )
        assert result == [("2024-01-07", "2024-01-10")]

    def test_overlapping_existing_ranges(self):
        """Test with overlapping existing ranges."""
        result = find_missing_ranges(
            "2024-01-01",
            "2024-01-10",
            [("2024-01-01", "2024-01-05"), ("2024-01-03", "2024-01-08")],
        )
        assert result == [("2024-01-09", "2024-01-10")]

    def test_existing_range_same_as_target(self):
        """Test when existing range equals target range."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-01", "2024-01-10")]
        )
        assert result == []

    def test_single_day_range(self):
        """Test with single day range."""
        result = find_missing_ranges("2024-01-01", "2024-01-01", [])
        assert result == [("2024-01-01", "2024-01-01")]

    def test_empty_existing_ranges_with_single_day(self):
        """Test with single day target and empty existing ranges."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-01", [("2024-01-01", "2024-01-01")]
        )
        assert result == []

    def test_gap_before_and_after_existing_range(self):
        """Test with gaps both before and after existing range."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-31", [("2024-01-10", "2024-01-20")]
        )
        assert result == [("2024-01-01", "2024-01-09"), ("2024-01-21", "2024-01-31")]

    def test_existing_range_starts_exactly_at_target_start(self):
        """Test when existing range starts exactly at target start."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-01", "2024-01-02")]
        )
        assert result == [("2024-01-03", "2024-01-10")]

    def test_existing_range_ends_exactly_at_target_end(self):
        """Test when existing range ends exactly at target end."""
        result = find_missing_ranges(
            "2024-01-01", "2024-01-10", [("2024-01-09", "2024-01-10")]
        )
        assert result == [("2024-01-01", "2024-01-08")]

    def test_unsorted_existing_ranges_get_sorted(self):
        """Test that existing ranges are sorted by start date."""
        result = find_missing_ranges(
            "2024-01-01",
            "2024-01-10",
            [
                ("2024-01-07", "2024-01-10"),
                ("2024-01-01", "2024-01-03"),
                ("2024-01-04", "2024-01-06"),
            ],
        )
        assert result == []

    def test_multiple_gaps(self):
        """Test with multiple gaps between existing ranges."""
        result = find_missing_ranges(
            "2024-01-01",
            "2024-01-31",
            [
                ("2024-01-05", "2024-01-07"),
                ("2024-01-15", "2024-01-17"),
                ("2024-01-25", "2024-01-27"),
            ],
        )
        assert result == [
            ("2024-01-01", "2024-01-04"),
            ("2024-01-08", "2024-01-14"),
            ("2024-01-18", "2024-01-24"),
            ("2024-01-28", "2024-01-31"),
        ]
