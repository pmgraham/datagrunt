"""Seeded edge-case corpus: filename -> exact file bytes.

Every parity test runs over ALL of these files. Names describe the edge case.
The repo has no static CSV fixtures (the main suite generates them at
runtime), so this corpus is the differential-parity oracle.
"""

CORPUS: dict[str, bytes] = {
    "basic_comma.csv": b"name,age,city\nalice,30,nyc\nbob,25,la\n",
    "semicolon.csv": b"name;age;city\nalice;30;nyc\nbob;25;la\n",
    "pipe.csv": b"name|age|city\nalice|30|nyc\nbob|25|la\n",
    "tab_in_csv.csv": b"name\tage\tcity\nalice\t30\tnyc\n",
    "tab_file.tsv": b"name\tage\tcity\nalice\t30\tnyc\n",
    "tsv_upper.TSV": b"a\tb\n1\t2\n",
    "space_delimited.csv": b"name age city\nalice 30 nyc\nbob 25 la\n",
    "dot_delimited.csv": b"user.id.name\n1.2.alice\n3.4.bob\n",
    "apostrophe_values.csv": b"name,note\nO'Brien,user's choice\nAnn,plain\n",
    "quoted_embedded_newline.csv": b'id,note\n1,"line one\nline two"\n2,plain\n',
    "quoted_embedded_quotes.csv": b'id,note\n1,"say ""hi"" now"\n2,ok\n',
    "quoted_with_delims.csv": b'id,note\n1,"a,b,c"\n2,"d,e"\n',
    "leading_comments.csv": b"# comment one\n# comment two\nname,age\nalice,30\n",
    "comments_and_blanks.csv": b"# c1\n\n# c2\n\nname,age\nalice,30\n",
    "blank_lines_between_rows.csv": b"name,age\nalice,30\n\nbob,25\n\n",
    "legacy_mac.csv": b"name,age\ralice,30\rbob,25\r",
    "legacy_mac_comments.csv": b"# c\rname,age\ralice,30\r",
    "crlf.csv": b"name,age\r\nalice,30\r\nbob,25\r\n",
    "ragged.csv": b"a,b,c\n1,2,3\n4,5\n6,7,8\n",
    "ragged_extra.csv": b"a,b\n1,2,3\n",
    "non_utf8.csv": b"name,city\nJos\xe9,NYC\nAnn,LA\n",
    "utf8_bom.csv": b"\xef\xbb\xbfname,age\nalice,30\n",
    "empty.csv": b"",
    "blank.csv": b"   \n\n  \t\n",
    "whitespace_single_line.csv": b"   ",
    "single_column.csv": b"name\nalice\nbob\n",
    "quoted_single_column.csv": b'"name"\n"alice"\n"bob"\n',
    "duplicate_headers.csv": b"col_a,col_a,col_a_1\n1,2,3\n",
    "special_headers.csv": b"%,(),123abc,col a,COL-B\n1,2,3,4,5\n",
    "unicode_headers.csv": "näme,ÄGE,城市\n1,2,3\n".encode("utf-8"),
    "skipinitialspace.csv": b'a, b, c\n1, "x", 3\n2, "y", 4\n',
    "colon_delimited.csv": b"a:b:c\n1:2:3\n4:5:6\n",
    "punct_header_consistent.csv": b"a'b'c\n1'2'3\n4'5'6\n",
    "no_trailing_newline.csv": b"a,b\n1,2",
    "single_row_only.csv": b"just one header row\n",
    "txt_extension.txt": b"a,b\n1,2\n",
    # Invalid-UTF-8 edge cases: the Rust DecodedReader must drop these bytes
    # exactly as CPython's errors="ignore" incremental decoder does.
    "lone_continuation.csv": b"a\x80b,c\n1,2\n",  # stray 0x80 mid-token
    "truncated_multibyte.csv": b"name,city\nAnn,caf\xc3",  # ends mid 2-byte char
    "interior_nul.csv": b"a,b\n1,x\x00y\n",  # embedded NUL inside a field
    # Data rows whose first field legitimately starts with '#': must NOT be
    # treated as a comment by the comment-skip logic (only LEADING comments are).
    "hex_color_values.csv": b"color,hex\nred,#ff0000\ngreen,#00ff00\n",
    "hash_first_data.csv": b"a,b\n#x,2\n3,4\n",
    # Ragged row that appears just PAST the 10,000-data-row scan cap
    # (ragged.rs MAX_DATA_ROWS_CHECKED). Validates Rust and Python cap identically.
    "ragged_beyond_10k_cap.csv": b"a,b,c\n" + b"1,2,3\n" * 10_000 + b"9,9\n",
    # Quoted field with an embedded newline straddling the 64 KiB streaming
    # decoder chunk boundary (io.rs CHUNK_SIZE). Exercises row counting across
    # the chunk split for both backends.
    "chunk_boundary_quoted_newline.csv": b"a,b\n" + b"x" * 65_528 + b',"line1\nline2"\n',
    # Legacy-Mac (\r) line endings WITH a quoted field that itself contains a
    # \r. Python reads these via newline=None (universal translation), so the
    # quoted \r must stay part of one record rather than splitting the row.
    # Exercises the legacy-mac branch and quoting together for both backends.
    "legacy_mac_quoted_cr.csv": b'id,note\r1,"line one\rline two"\r2,plain\r',
    # Giant-line entries for per-line cap parity (issue #222 / #176).
    # These verify that both backends truncate to exactly MAX_LINE_CHARS chars
    # and that char-based (not byte-based) counting governs the boundary.
    #
    # (a) Giant ASCII line: 2,252,800 chars > MAX_LINE_CHARS (2,097,152).
    #     Both backends must yield exactly MAX_LINE_CHARS chars from this line.
    "giant_ascii_line.csv": b"a," * 1_126_400 + b"\n",
    # (b) Giant line with a 3-byte UTF-8 char (€) placed right at the char
    #     boundary. The line uses a comma-delimited "a," pattern so no single
    #     field exceeds CPython's csv field-size limit (the cap is per physical
    #     line, not per field); the '€' lands at char index MAX_LINE_CHARS-1
    #     (the last char kept by truncation). 2Mi-2 pattern chars + 'a' + '€'
    #     puts '€' exactly on the boundary, then ",more" overflows the cap.
    #     Verifies Rust counts CHARS (not bytes) and lands on the multibyte char.
    "giant_multibyte_near_cap.csv": (b"a," * (1024 * 1024 - 1) + b"a" + "€".encode("utf-8") + b",more,fields,here\n"),
    # (c) Giant line terminated with \r\n followed by a normal data line.
    #     After capping the giant line, the next line must still be read
    #     correctly — validates stream-skip alignment for both backends.
    "giant_crlf_line.csv": b"b," * 1_126_400 + b"\r\nnext_line\n",
    # (d) Giant line with a LEADING invalid UTF-8 byte followed by a long run
    #     of comma-delimited 4-byte chars (😀). Repro for the raw-byte-budget
    #     bug: the leading \xff is dropped by errors="ignore", so a byte-budgeted
    #     decoder yields MAX_LINE_CHARS-1 chars while Python decodes the whole
    #     line then slices [:MAX_LINE_CHARS]. Both backends must yield exactly
    #     MAX_LINE_CHARS decoded chars (budget on DECODED chars, not raw bytes).
    #
    #     Uses a comma-delimited "😀," pattern (each field is a single 😀, well
    #     under CPython's csv field-size limit — the cap is per physical line,
    #     not per field). 1_800_000 reps = 9,000,000 raw bytes (> 8 MiB
    #     MAX_LINE_READ_BYTES, so the streaming cap branch fires) and 3,600,000
    #     decoded chars (> 2 Mi MAX_LINE_CHARS).
    "giant_invalid_leading_byte.csv": (b"\xff" + "😀,".encode("utf-8") * 1_800_000 + b"\n"),
    # (e) Same shape as (d) but WITHOUT the leading invalid byte: no dropped
    #     bytes. Both backends must still yield exactly MAX_LINE_CHARS chars —
    #     verifies the fix does not regress clean 4-byte-char giant lines.
    "giant_4byte_chars_no_invalid.csv": ("😀,".encode("utf-8") * 1_800_000 + b"\n"),
    # C0 information separators FS/GS/RS/US (U+001C-001F) parity (issue #176).
    # Python str.strip()/str.split() treat these four as whitespace; Rust's
    # trim()/split_whitespace() (Unicode White_Space) do not. They are the ONLY
    # such divergence across all of Unicode, so both backends must agree on
    # comment/blank/strip/split decisions for lines containing them.
    #
    # (a) A leading comment whose '#' is preceded by a C0 separator: after
    #     stripping it IS a comment (count_leading_comments / probe must skip it).
    "c0_leading_comment.csv": b"\x1c# leading comment\nname,age\nalice,30\n",
    # (b) A line of only C0 separators between data rows: stripped it is blank,
    #     so it must not become a sample/data row (probe blankness, ragged).
    "c0_blank_only_line.csv": b"name,age\n\x1c\x1d\x1e\x1f\nalice,30\n",
    # (c) A whole file of only C0 separators: the probe must report blank=True
    #     with no surviving rows (the equivalent BlankFile.is_blank path is
    #     locked by a Rust unit test, as it is not exposed to _native).
    "c0_all_whitespace.csv": b"\x1c\x1d\x1e\x1f\n",
    # (d) Fields separated only by C0 separators: Python's bare split() treats
    #     them as whitespace runs (3 fields per row), driving space-delimiter
    #     inference; Rust split_whitespace() must match.
    "c0_field_separators.csv": b"a\x1cb\x1cc\n1\x1d2\x1d3\nx\x1ey\x1ez\n",
    # (e) Header with leading/trailing C0 separators: stripping must yield the
    #     same first_row / sample_rows as Python for both backends.
    "c0_leading_trailing_strip.csv": b"\x1cname,age\x1d\nalice,30\nbob,25\n",
    # KNOWN DIVERGENCE (see #NNN), found by tests/parity/test_parity_property.py.
    # A file whose bytes are non-empty but decode to the EMPTY string under
    # errors="ignore", with no line terminator: every byte is dropped by the
    # decoder, so there is no text left to form a line.
    #   count_leading_physical_lines_before_header -> Rust 1, Python 0.
    # Rust's universal_lines works on the byte stream and emits a final
    # (now empty) line because bytes remained unterminated; CPython iterates
    # DECODED text, which is "", so it yields no lines at all. Appending a
    # terminator (b"\x80\n") makes both return 1, and b"\x80a" makes both
    # return 1 — only the fully-dropped, unterminated case diverges.
    "invalid_utf8_only_no_newline.csv": b"\x80",
    # KNOWN DIVERGENCE (see #NNN), found by tests/parity/test_parity_property.py.
    # sniff_dialect's own `delimiter` differs on characters where CPython's `\w`
    # and the Rust regex crate's `\w` disagree, because the sniffer's delimiter
    # class is `[^\w\n"']`:
    #   - CPython `\w` is str.isalnum()-based, so it MATCHES category No
    #     (U+00B2 '²', U+00BD '½', U+2460 '①') -> not a delimiter candidate.
    #   - The regex crate's `\w` is [\p{Alphabetic}\p{M}\p{Nd}\p{Pc}\p{Join_Control}],
    #     which excludes No -> '²' IS a candidate and wins.
    # Rust sniffs delimiter '²'; Python sniffs '"'. The reverse holds for marks
    # (U+0301, category Mn): Python picks it, Rust does not.
    # Every other sniffed field agrees, so the corpus dialect tests (which
    # normalize through dialect_properties_from_rust and drop `delimiter`)
    # cannot see this; the property suite compares the raw dicts.
    "sniff_delimiter_word_class.csv": b'"\'"\'\n"\xc2\xb2\'"',
}
