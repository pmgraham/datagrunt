#!/usr/bin/env python3
"""Script to export Root PDFs to Markdown using different engines and compare the outputs."""

import glob
import time
import sys
from pathlib import Path
import polars as pl

# Add repository root to python path
_REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_REPO_ROOT))

from datagrunt import PDFWriter


def get_root_pdfs():
    """Find all PDFs under data/pdfs/ starting with 'Root_Base'."""
    pdf_dir = _REPO_ROOT / "data" / "pdfs"
    return sorted(glob.glob(str(pdf_dir / "**" / "Root_Base*.pdf"), recursive=True))


def benchmark_markdown_export(pdf_path):
    """Export a single PDF to Markdown under different configurations and return stats."""
    path_obj = Path(pdf_path)
    file_name = path_obj.name
    
    configs = [
        {"engine": "pdfium", "native": False, "label": "pdfium_structured"},
        {"engine": "pdfium", "native": True, "label": "pdfium_native"},
        {"engine": "pymupdf", "native": False, "label": "pymupdf"},
    ]

    results = []

    for cfg in configs:
        engine = cfg["engine"]
        native = cfg["native"]
        label = cfg["label"]

        out_dir = _REPO_ROOT / "outputs" / "markdown_exports" / label
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{path_obj.stem}.md"
        img_dir = out_dir / f"{path_obj.stem}_images"

        print(f"  Exporting with {label}...", end="", flush=True)
        start_time = time.perf_counter()
        try:
            writer = PDFWriter(str(pdf_path), engine=engine, native=native, workers=4)
            # Write markdown and save images to disk
            writer.write_markdown(
                export_filename=str(out_file),
                image_output_dir=str(img_dir)
            )
            elapsed_sec = time.perf_counter() - start_time

            # Read the generated markdown file to get character count
            with open(out_file, "r", encoding="utf-8") as f:
                md_content = f.read()

            results.append({
                "file_name": file_name,
                "config": label,
                "success": True,
                "export_time_s": round(elapsed_sec, 2),
                "md_chars": len(md_content),
                "md_size_kb": round(out_file.stat().st_size / 1024.0, 2),
                "error": ""
            })
            print(f" Done in {elapsed_sec:.2f}s ({len(md_content)} chars).")
        except Exception as e:
            elapsed_sec = time.perf_counter() - start_time
            results.append({
                "file_name": file_name,
                "config": label,
                "success": False,
                "export_time_s": round(elapsed_sec, 2),
                "md_chars": 0,
                "md_size_kb": 0.0,
                "error": str(e)
            })
            print(f" Failed in {elapsed_sec:.2f}s: {e}")

    return results


def main():
    pdfs = get_root_pdfs()
    if not pdfs:
        print("No root PDFs found in data folder.")
        sys.exit(1)

    print(f"Found {len(pdfs)} root PDFs. Starting Markdown export benchmark...")
    
    all_results = []
    for pdf in pdfs:
        print(f"Processing {Path(pdf).name} ({Path(pdf).stat().st_size/1024/1024:.2f} MB)...")
        results = benchmark_markdown_export(pdf)
        all_results.extend(results)

    df = pl.DataFrame(all_results)
    
    print("\n" + "=" * 80)
    print("DETAILED MARKDOWN EXPORT RESULTS")
    print("=" * 80)
    with pl.Config(tbl_rows=100):
        print(df.select([
            "file_name", "config", "success", "export_time_s", "md_chars", "md_size_kb"
        ]))

    print("\n" + "=" * 80)
    print("SUMMARY BY CONFIGURATION")
    print("=" * 80)
    summary_df = df.group_by("config").agg([
        pl.col("success").mean().alias("success_rate") * 100,
        pl.col("export_time_s").mean().alias("avg_export_time_s"),
        pl.col("export_time_s").sum().alias("sum_export_time_s"),
        pl.col("md_chars").mean().alias("avg_md_chars"),
        pl.col("md_size_kb").mean().alias("avg_md_size_kb"),
    ])
    with pl.Config():
        print(summary_df)

    # Save to CSV
    output_path = _REPO_ROOT / "outputs" / "markdown_export_benchmark.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)
    print(f"\nSaved detailed results to {output_path}")


if __name__ == "__main__":
    main()
