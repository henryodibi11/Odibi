"""File upload and processing utilities for multimodal chat.

Supports:
- Images (PNG, JPG, GIF, WebP) -> base64 for vision models
- CSV/Excel -> parsed table text
- PDF -> base64 or text extraction
- Text/Code files -> raw text
"""

import base64
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".ico"}
TEXT_EXTENSIONS = {
    # Plain text and documentation
    ".txt",
    ".md",
    ".rst",
    ".tex",
    ".adoc",
    ".org",
    ".log",
    # Config files
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".env",
    ".properties",
    ".gitignore",
    ".dockerignore",
    ".editorconfig",
    # Build/DevOps (no extension handling below)
    # Python
    ".py",
    ".pyx",
    ".pyi",
    ".pyw",
    # JavaScript/TypeScript
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".mjs",
    ".cjs",
    # Web
    ".html",
    ".htm",
    ".css",
    ".scss",
    ".sass",
    ".less",
    ".vue",
    ".svelte",
    # Data/Query
    ".sql",
    ".graphql",
    ".gql",
    # Shell
    ".sh",
    ".bash",
    ".zsh",
    ".fish",
    ".ps1",
    ".bat",
    ".cmd",
    # Systems
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".cc",
    ".cxx",
    ".cs",
    ".vb",
    # JVM
    ".java",
    ".scala",
    ".kt",
    ".kts",
    ".groovy",
    ".gradle",
    # Other languages
    ".go",
    ".rs",
    ".swift",
    ".rb",
    ".php",
    ".pl",
    ".pm",
    ".lua",
    ".r",
    ".R",
    ".jl",
    ".m",
    ".mm",
    ".f90",
    ".f95",
    ".f03",
    ".hs",
    ".ml",
    ".mli",
    ".clj",
    ".cljs",
    ".ex",
    ".exs",
    ".erl",
    ".zig",
    ".nim",
    ".v",
    ".d",
    ".ada",
    ".pas",
    # Markup/Data
    ".xml",
    ".xsl",
    ".xslt",
    ".svg",
    ".rss",
    ".atom",
    # Other
    ".makefile",
    ".cmake",
    ".dockerfile",
}
# Files without extensions that should be treated as text
TEXT_FILENAMES = {
    "dockerfile",
    "makefile",
    "gemfile",
    "rakefile",
    "procfile",
    "vagrantfile",
    "jenkinsfile",
    "brewfile",
    "cakefile",
    "requirements.txt",
    "setup.py",
    "setup.cfg",
    "pyproject.toml",
    "package.json",
    "tsconfig.json",
    "composer.json",
    "cargo.toml",
    "go.mod",
    "go.sum",
    ".gitignore",
    ".dockerignore",
    ".env",
    ".env.example",
}
EXCEL_EXTENSIONS = {".xlsx", ".xls", ".xlsm", ".xlsb"}
CSV_EXTENSIONS = {".csv", ".tsv"}
PDF_EXTENSIONS = {".pdf"}
PARQUET_EXTENSIONS = {".parquet", ".pq"}
FEATHER_EXTENSIONS = {".feather", ".ftr", ".arrow"}
PICKLE_EXTENSIONS = {".pickle", ".pkl", ".joblib"}
NOTEBOOK_EXTENSIONS = {".ipynb"}
SQLITE_EXTENSIONS = {".sqlite", ".sqlite3", ".db", ".db3"}
WORD_EXTENSIONS = {".docx"}


@dataclass
class ProcessedFile:
    """Result of processing an uploaded file."""

    filename: str
    file_type: str
    content: Any
    is_multimodal: bool = False
    mime_type: Optional[str] = None
    error: Optional[str] = None


def get_file_extension(filepath: str) -> str:
    """Get lowercase file extension."""
    return Path(filepath).suffix.lower()


def get_mime_type(filepath: str) -> str:
    """Get MIME type for a file."""
    mime_type, _ = mimetypes.guess_type(filepath)
    return mime_type or "application/octet-stream"


def encode_image_base64(filepath: str) -> str:
    """Encode image file to base64 string."""
    with open(filepath, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def process_image(filepath: str) -> ProcessedFile:
    """Process image file for vision models."""
    try:
        filename = os.path.basename(filepath)
        mime_type = get_mime_type(filepath)
        base64_data = encode_image_base64(filepath)

        content = {
            "type": "image_url",
            "image_url": {"url": f"data:{mime_type};base64,{base64_data}"},
        }

        return ProcessedFile(
            filename=filename,
            file_type="image",
            content=content,
            is_multimodal=True,
            mime_type=mime_type,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="image",
            content=None,
            error=f"Failed to process image: {e}",
        )


def process_text_file(filepath: str) -> ProcessedFile:
    """Process text/code file."""
    try:
        filename = os.path.basename(filepath)
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        ext = get_file_extension(filepath)
        lang = ext.lstrip(".") if ext else "text"

        formatted = f"**File: {filename}**\n```{lang}\n{content}\n```"

        return ProcessedFile(
            filename=filename,
            file_type="text",
            content=formatted,
            is_multimodal=False,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="text",
            content=None,
            error=f"Failed to read text file: {e}",
        )


def process_csv(filepath: str) -> ProcessedFile:
    """Process CSV file to text table."""
    try:
        filename = os.path.basename(filepath)

        try:
            import pandas as pd

            df = pd.read_csv(filepath)
            if len(df) > 100:
                preview = df.head(100).to_markdown(index=False)
                content = f"**File: {filename}** (showing first 100 of {len(df)} rows)\n\n{preview}"
            else:
                content = f"**File: {filename}**\n\n{df.to_markdown(index=False)}"
        except ImportError:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            if len(lines) > 100:
                content = f"**File: {filename}** (showing first 100 of {len(lines)} lines)\n```csv\n{''.join(lines[:100])}```"
            else:
                content = f"**File: {filename}**\n```csv\n{''.join(lines)}```"

        return ProcessedFile(
            filename=filename,
            file_type="csv",
            content=content,
            is_multimodal=False,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="csv",
            content=None,
            error=f"Failed to process CSV: {e}",
        )


def process_excel(filepath: str) -> ProcessedFile:
    """Process Excel file to text table."""
    try:
        filename = os.path.basename(filepath)

        try:
            import pandas as pd

            sheets = pd.read_excel(filepath, sheet_name=None)

            parts = [f"**File: {filename}**\n"]
            for sheet_name, df in sheets.items():
                if len(df) > 50:
                    preview = df.head(50).to_markdown(index=False)
                    parts.append(
                        f"\n### Sheet: {sheet_name} (showing first 50 of {len(df)} rows)\n{preview}"
                    )
                else:
                    parts.append(f"\n### Sheet: {sheet_name}\n{df.to_markdown(index=False)}")

            content = "\n".join(parts)
        except ImportError:
            return ProcessedFile(
                filename=filename,
                file_type="excel",
                content=None,
                error="pandas and openpyxl required for Excel files. Install with: pip install pandas openpyxl",
            )

        return ProcessedFile(
            filename=filename,
            file_type="excel",
            content=content,
            is_multimodal=False,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="excel",
            content=None,
            error=f"Failed to process Excel: {e}",
        )


def process_pdf(filepath: str, use_vision: bool = True) -> ProcessedFile:
    """Process PDF file.

    Args:
        filepath: Path to PDF file
        use_vision: If True, encode as base64 for vision models.
                   If False, attempt text extraction.
    """
    try:
        filename = os.path.basename(filepath)

        if use_vision:
            mime_type = "application/pdf"
            base64_data = encode_image_base64(filepath)

            content = {
                "type": "file",
                "file": {
                    "filename": filename,
                    "file_data": f"data:{mime_type};base64,{base64_data}",
                },
            }

            return ProcessedFile(
                filename=filename,
                file_type="pdf",
                content=content,
                is_multimodal=True,
                mime_type=mime_type,
            )
        else:
            try:
                import pypdf

                reader = pypdf.PdfReader(filepath)
                text_parts = []
                for i, page in enumerate(reader.pages):
                    text = page.extract_text()
                    if text:
                        text_parts.append(f"--- Page {i + 1} ---\n{text}")

                content = f"**File: {filename}**\n\n" + "\n\n".join(text_parts)

                return ProcessedFile(
                    filename=filename,
                    file_type="pdf",
                    content=content,
                    is_multimodal=False,
                )
            except ImportError:
                base64_data = encode_image_base64(filepath)
                content = {
                    "type": "file",
                    "file": {
                        "filename": filename,
                        "file_data": f"data:application/pdf;base64,{base64_data}",
                    },
                }
                return ProcessedFile(
                    filename=filename,
                    file_type="pdf",
                    content=content,
                    is_multimodal=True,
                    mime_type="application/pdf",
                )

    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="pdf",
            content=None,
            error=f"Failed to process PDF: {e}",
        )


def process_parquet(filepath: str) -> ProcessedFile:
    """Process Parquet file to text table."""
    try:
        filename = os.path.basename(filepath)

        try:
            import pandas as pd

            df = pd.read_parquet(filepath)

            # Show schema info
            schema_info = f"**Columns ({len(df.columns)}):** " + ", ".join(
                f"`{col}` ({df[col].dtype})" for col in df.columns
            )

            if len(df) > 50:
                preview = df.head(50).to_markdown(index=False)
                content = f"**File: {filename}** ({len(df):,} rows)\n\n{schema_info}\n\n### Preview (first 50 rows)\n{preview}"
            else:
                content = f"**File: {filename}** ({len(df):,} rows)\n\n{schema_info}\n\n{df.to_markdown(index=False)}"

            return ProcessedFile(
                filename=filename,
                file_type="parquet",
                content=content,
                is_multimodal=False,
            )
        except ImportError:
            return ProcessedFile(
                filename=filename,
                file_type="parquet",
                content=None,
                error="pandas and pyarrow required for Parquet files. Install with: pip install pandas pyarrow",
            )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="parquet",
            content=None,
            error=f"Failed to process Parquet: {e}",
        )


def process_feather(filepath: str) -> ProcessedFile:
    """Process Feather/Arrow file to text table."""
    try:
        filename = os.path.basename(filepath)

        try:
            import pandas as pd

            df = pd.read_feather(filepath)

            schema_info = f"**Columns ({len(df.columns)}):** " + ", ".join(
                f"`{col}` ({df[col].dtype})" for col in df.columns
            )

            if len(df) > 50:
                preview = df.head(50).to_markdown(index=False)
                content = f"**File: {filename}** ({len(df):,} rows)\n\n{schema_info}\n\n### Preview (first 50 rows)\n{preview}"
            else:
                content = f"**File: {filename}** ({len(df):,} rows)\n\n{schema_info}\n\n{df.to_markdown(index=False)}"

            return ProcessedFile(
                filename=filename,
                file_type="feather",
                content=content,
                is_multimodal=False,
            )
        except ImportError:
            return ProcessedFile(
                filename=filename,
                file_type="feather",
                content=None,
                error="pandas and pyarrow required for Feather files. Install with: pip install pandas pyarrow",
            )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="feather",
            content=None,
            error=f"Failed to process Feather: {e}",
        )


def process_pickle(filepath: str) -> ProcessedFile:
    """Process Pickle file - show structure/preview."""
    try:
        filename = os.path.basename(filepath)
        import pickle

        with open(filepath, "rb") as f:
            obj = pickle.load(f)

        obj_type = type(obj).__name__

        # Try to get a useful representation
        try:
            import pandas as pd

            if isinstance(obj, pd.DataFrame):
                schema_info = f"**Columns ({len(obj.columns)}):** " + ", ".join(
                    f"`{col}` ({obj[col].dtype})" for col in obj.columns
                )
                if len(obj) > 50:
                    preview = obj.head(50).to_markdown(index=False)
                    content = f"**File: {filename}** (DataFrame, {len(obj):,} rows)\n\n{schema_info}\n\n### Preview\n{preview}"
                else:
                    content = f"**File: {filename}** (DataFrame, {len(obj):,} rows)\n\n{schema_info}\n\n{obj.to_markdown(index=False)}"
            elif isinstance(obj, (list, dict)):
                import json

                preview = json.dumps(obj, indent=2, default=str)[:5000]
                content = f"**File: {filename}** ({obj_type})\n```json\n{preview}\n```"
            else:
                preview = repr(obj)[:3000]
                content = f"**File: {filename}** ({obj_type})\n```\n{preview}\n```"
        except Exception:
            preview = repr(obj)[:3000]
            content = f"**File: {filename}** ({obj_type})\n```\n{preview}\n```"

        return ProcessedFile(
            filename=filename,
            file_type="pickle",
            content=content,
            is_multimodal=False,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="pickle",
            content=None,
            error=f"Failed to process Pickle: {e}",
        )


def process_notebook(filepath: str) -> ProcessedFile:
    """Process Jupyter notebook."""
    try:
        filename = os.path.basename(filepath)
        import json

        with open(filepath, "r", encoding="utf-8") as f:
            nb = json.load(f)

        cells = nb.get("cells", [])
        parts = [f"**Notebook: {filename}** ({len(cells)} cells)\n"]

        for i, cell in enumerate(cells[:30]):  # Limit to first 30 cells
            cell_type = cell.get("cell_type", "unknown")
            source = "".join(cell.get("source", []))

            if cell_type == "markdown":
                parts.append(f"\n### Cell {i + 1} (Markdown)\n{source[:1000]}")
            elif cell_type == "code":
                parts.append(f"\n### Cell {i + 1} (Code)\n```python\n{source[:1500]}\n```")

                # Show outputs if present
                outputs = cell.get("outputs", [])
                for out in outputs[:2]:  # Limit outputs
                    if out.get("output_type") == "stream":
                        text = "".join(out.get("text", []))[:500]
                        parts.append(f"**Output:**\n```\n{text}\n```")
                    elif out.get("output_type") == "execute_result":
                        data = out.get("data", {})
                        if "text/plain" in data:
                            text = "".join(data["text/plain"])[:500]
                            parts.append(f"**Result:**\n```\n{text}\n```")

        if len(cells) > 30:
            parts.append(f"\n_... and {len(cells) - 30} more cells_")

        content = "\n".join(parts)

        return ProcessedFile(
            filename=filename,
            file_type="notebook",
            content=content,
            is_multimodal=False,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="notebook",
            content=None,
            error=f"Failed to process notebook: {e}",
        )


def process_sqlite(filepath: str) -> ProcessedFile:
    """Process SQLite database - show schema and sample data."""
    try:
        filename = os.path.basename(filepath)
        import sqlite3

        conn = sqlite3.connect(filepath)
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]

        parts = [f"**Database: {filename}** ({len(tables)} tables)\n"]

        for table in tables[:20]:  # Limit tables shown
            # Get schema
            cursor.execute(f"PRAGMA table_info('{table}')")
            columns = cursor.fetchall()
            col_info = ", ".join(f"`{c[1]}` {c[2]}" for c in columns)

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM '{table}'")
            count = cursor.fetchone()[0]

            parts.append(f"\n### {table} ({count:,} rows)\n**Columns:** {col_info}")

            # Show sample data
            if count > 0:
                cursor.execute(f"SELECT * FROM '{table}' LIMIT 5")
                rows = cursor.fetchall()
                col_names = [c[1] for c in columns]

                try:
                    import pandas as pd

                    df = pd.DataFrame(rows, columns=col_names)
                    parts.append(f"\n**Sample:**\n{df.to_markdown(index=False)}")
                except ImportError:
                    sample = "\n".join(str(r) for r in rows)
                    parts.append(f"\n**Sample:**\n```\n{sample}\n```")

        conn.close()

        if len(tables) > 20:
            parts.append(f"\n_... and {len(tables) - 20} more tables_")

        content = "\n".join(parts)

        return ProcessedFile(
            filename=filename,
            file_type="sqlite",
            content=content,
            is_multimodal=False,
        )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="sqlite",
            content=None,
            error=f"Failed to process SQLite: {e}",
        )


def process_word(filepath: str) -> ProcessedFile:
    """Process Word document (.docx)."""
    try:
        filename = os.path.basename(filepath)

        try:
            from docx import Document

            doc = Document(filepath)

            parts = [f"**Document: {filename}**\n"]

            for para in doc.paragraphs[:200]:  # Limit paragraphs
                if para.text.strip():
                    # Check if heading
                    if para.style.name.startswith("Heading"):
                        level = para.style.name[-1] if para.style.name[-1].isdigit() else "2"
                        parts.append(f"\n{'#' * int(level)} {para.text}")
                    else:
                        parts.append(para.text)

            content = "\n\n".join(parts)

            return ProcessedFile(
                filename=filename,
                file_type="word",
                content=content,
                is_multimodal=False,
            )
        except ImportError:
            return ProcessedFile(
                filename=filename,
                file_type="word",
                content=None,
                error="python-docx required for Word files. Install with: pip install python-docx",
            )
    except Exception as e:
        return ProcessedFile(
            filename=os.path.basename(filepath),
            file_type="word",
            content=None,
            error=f"Failed to process Word document: {e}",
        )


def process_file(filepath: str, pdf_use_vision: bool = True) -> ProcessedFile:
    """Process any supported file type.

    Args:
        filepath: Path to the file
        pdf_use_vision: For PDFs, use vision (base64) or text extraction

    Returns:
        ProcessedFile with content ready for LLM
    """
    ext = get_file_extension(filepath)
    filename = os.path.basename(filepath).lower()

    if ext in IMAGE_EXTENSIONS:
        return process_image(filepath)
    elif ext in CSV_EXTENSIONS:
        return process_csv(filepath)
    elif ext in EXCEL_EXTENSIONS:
        return process_excel(filepath)
    elif ext in PDF_EXTENSIONS:
        return process_pdf(filepath, use_vision=pdf_use_vision)
    elif ext in PARQUET_EXTENSIONS:
        return process_parquet(filepath)
    elif ext in FEATHER_EXTENSIONS:
        return process_feather(filepath)
    elif ext in PICKLE_EXTENSIONS:
        return process_pickle(filepath)
    elif ext in NOTEBOOK_EXTENSIONS:
        return process_notebook(filepath)
    elif ext in SQLITE_EXTENSIONS:
        return process_sqlite(filepath)
    elif ext in WORD_EXTENSIONS:
        return process_word(filepath)
    elif ext in TEXT_EXTENSIONS:
        return process_text_file(filepath)
    elif filename in TEXT_FILENAMES or any(filename.endswith(f) for f in TEXT_FILENAMES):
        return process_text_file(filepath)
    else:
        # Try to read as text file
        try:
            return process_text_file(filepath)
        except Exception:
            return ProcessedFile(
                filename=os.path.basename(filepath),
                file_type="unknown",
                content=None,
                error=f"Unsupported file type: {ext}",
            )


def build_multimodal_content(
    text: str,
    files: list[Union[str, ProcessedFile]],
    pdf_use_vision: bool = True,
) -> Union[str, list[dict]]:
    """Build message content with text and files.

    Args:
        text: User's text message
        files: List of file paths or ProcessedFile objects

    Returns:
        Either a string (text only) or list of content blocks (multimodal)
    """
    if not files:
        return text

    processed = []
    text_parts = []
    has_multimodal = False

    for f in files:
        if isinstance(f, str):
            pf = process_file(f, pdf_use_vision=pdf_use_vision)
        else:
            pf = f

        if pf.error:
            text_parts.append(f"⚠️ Error processing {pf.filename}: {pf.error}")
        elif pf.is_multimodal:
            processed.append(pf)
            has_multimodal = True
        else:
            text_parts.append(pf.content)

    combined_text = text
    if text_parts:
        combined_text = text + "\n\n" + "\n\n".join(text_parts)

    if not has_multimodal:
        return combined_text

    content_blocks = [{"type": "text", "text": combined_text}]

    for pf in processed:
        content_blocks.append(pf.content)

    return content_blocks


def format_file_preview(filepath: str) -> str:
    """Generate a preview string for display in chat."""
    filename = os.path.basename(filepath)
    ext = get_file_extension(filepath)

    if ext in IMAGE_EXTENSIONS:
        return f"🖼️ {filename}"
    elif ext in CSV_EXTENSIONS:
        return f"📊 {filename}"
    elif ext in EXCEL_EXTENSIONS:
        return f"📗 {filename}"
    elif ext in PDF_EXTENSIONS:
        return f"📄 {filename}"
    elif ext in PARQUET_EXTENSIONS or ext in FEATHER_EXTENSIONS:
        return f"🗃️ {filename}"
    elif ext in PICKLE_EXTENSIONS:
        return f"🥒 {filename}"
    elif ext in NOTEBOOK_EXTENSIONS:
        return f"📓 {filename}"
    elif ext in SQLITE_EXTENSIONS:
        return f"🗄️ {filename}"
    elif ext in WORD_EXTENSIONS:
        return f"📃 {filename}"
    elif ext in TEXT_EXTENSIONS:
        return f"📝 {filename}"
    else:
        return f"📎 {filename}"
