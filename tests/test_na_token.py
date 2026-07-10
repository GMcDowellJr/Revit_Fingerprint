import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from na_token import is_na_token, is_blank_or_na


class TestIsNaToken:
    def test_recognizes_common_spellings(self):
        for value in [
            "na", "NA", "Na",
            "n/a", "N/A", "n a",
            "not applicable", "Not Applicable", "NOT APPLICABLE",
            "not_applicable", "__NOT_APPLICABLE__", "__not_applicable__",
            "not-applicable",
        ]:
            assert is_na_token(value), f"expected {value!r} to be recognized as NA"

    def test_real_values_are_not_na(self):
        for value in ["Sutter", "BC_2270", "imperial", "Template", "Nashville", "N/A Consulting"]:
            assert not is_na_token(value), f"expected {value!r} to NOT be recognized as NA"

    def test_blank_is_not_na(self):
        # Blank is a distinct "todo" signal, not NA — is_na_token only
        # recognizes explicit "not applicable" spellings.
        assert not is_na_token("")


class TestIsBlankOrNa:
    def test_blank_variants_are_ignore(self):
        assert is_blank_or_na("")
        assert is_blank_or_na("   ")

    def test_na_variants_are_ignore(self):
        assert is_blank_or_na("__NOT_APPLICABLE__")
        assert is_blank_or_na("n/a")
        assert is_blank_or_na("  NA  ")

    def test_real_values_are_not_ignore(self):
        assert not is_blank_or_na("Sutter")
        assert not is_blank_or_na("BC_2270")
