import argparse
import re
import shutil
from pathlib import Path


ARTIFACT_NAMES = {
    "manifest.json",
    "run_results.json",
    "sources.json",
}


def safe_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value)


def archive_artifacts(run_id: str, logical_date: str) -> Path:
    source_dir = Path("dbt") / "target"
    archive_dir = Path("docs") / "run_artifacts" / logical_date / safe_token(run_id)
    archive_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    if source_dir.exists():
        for name in ARTIFACT_NAMES:
            source = source_dir / name
            if source.exists():
                shutil.copy2(source, archive_dir / name)
                copied += 1

        compiled_dir = source_dir / "compiled"
        if compiled_dir.exists():
            destination = archive_dir / "compiled"
            if destination.exists():
                shutil.rmtree(destination)
            shutil.copytree(compiled_dir, destination)

    marker = archive_dir / "README.txt"
    marker.write_text(
        "Archived dbt artifacts for pipeline observability.\n"
        f"logical_date={logical_date}\n"
        f"run_id={run_id}\n"
        f"json_artifacts_copied={copied}\n",
        encoding="utf-8",
    )
    print(f"Archived dbt artifacts to {archive_dir}")
    return archive_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--logical-date", required=True)
    args = parser.parse_args()
    archive_artifacts(args.run_id, args.logical_date)

