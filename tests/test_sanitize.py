"""Unit tests for the filename sanitization helper."""

from snow_ddl_extractor.writer import sanitize_filename


class TestSanitizeFilename:
    """Tests for ``sanitize_filename``."""

    def test_plain_name_unchanged(self) -> None:
        assert sanitize_filename("MY_TABLE") == "MY_TABLE"

    def test_replaces_illegal_characters(self) -> None:
        result = sanitize_filename('a<b>c:d"e/f\\g|h?i*j')
        assert "<" not in result
        assert ">" not in result
        assert ":" not in result
        assert '"' not in result
        assert "/" not in result
        assert "\\" not in result
        assert "|" not in result
        assert "?" not in result
        assert "*" not in result

    def test_strips_leading_trailing_dots_and_spaces(self) -> None:
        assert sanitize_filename("  ..name.. ") == "name"

    def test_empty_string_returns_placeholder(self) -> None:
        assert sanitize_filename("") == "_unnamed_"

    def test_only_dots_returns_placeholder(self) -> None:
        assert sanitize_filename("...") == "_unnamed_"

    def test_reserved_windows_name_gets_suffix(self) -> None:
        result = sanitize_filename("CON")
        assert result != "CON"
        assert result.startswith("CON")

    def test_reserved_name_case_insensitive(self) -> None:
        result = sanitize_filename("nul")
        assert result.upper() != "NUL"

    def test_control_characters_replaced(self) -> None:
        result = sanitize_filename("abc\x00\x1fdef")
        assert "\x00" not in result
        assert "\x1f" not in result

    def test_preserves_underscores_and_hyphens(self) -> None:
        assert sanitize_filename("MY-TABLE_V2") == "MY-TABLE_V2"

    def test_parentheses_preserved(self) -> None:
        # Procedure signatures may contain parens — they are valid in filenames.
        assert sanitize_filename("MY_PROC(VARCHAR)") == "MY_PROC(VARCHAR)"
