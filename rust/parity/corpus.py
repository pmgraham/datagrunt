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
}
