"""
scaffold.py — tạo cấu trúc thư mục src/ và tests/ cho media-data-generator
Chạy từ root project: python scaffold.py
"""

from pathlib import Path

ROOT = Path(__file__).parent

# ── src ────────────────────────────────────────────────────────────────────────

SRC = ROOT / "src" / "coursework"

STRUCTURE = {
    "core": [
        "__init__.py",
        "base_generator.py",
        "config.py",
        "schema.py",
        "utils.py",
    ],
    "offline": [
        "__init__.py",
        "base_table_generator.py",
        "users.py",
        "videos.py",
        "playback_history.py",
        "interactions.py",
        "ad_impressions.py",
        "offline_data_generator.py",
    ],
    "streaming": [
        "__init__.py",
        "base_event_generator.py",
        "playback_start.py",
        "pause.py",
        "skip.py",
        "ad_impression.py",
        "subscription_cancel.py",
        "stream_data_generator.py",
    ],
    "features": [
        "__init__.py",
        "offline_calculator.py",
        "streaming_calculator.py",
        "feature_engineer.py",
    ],
    "pipeline": [
        "__init__.py",
        "orchestrator.py",
        "quality_report.py",
    ],
}

SRC_ROOT_FILES = ["__init__.py", "__main__.py"]

# ── tests ──────────────────────────────────────────────────────────────────────

TESTS = ROOT / "tests"

TEST_FILES = [
    "conftest.py",
    "test_utils.py",
    "test_schema_evolution.py",
    "test_referential_integrity.py",
    "test_pipeline.py",
]

# ── other ──────────────────────────────────────────────────────────────────────

OTHER_DIRS = [
    ROOT / "config",
    ROOT / "data" / "offline",
    ROOT / "data" / "streaming",
    ROOT / "data" / "features",
    ROOT / "notebooks",
]

OTHER_FILES = {
    ROOT / "config" / "default.yaml": "# GeneratorConfig — chỉnh thông số ở đây\n",
    ROOT / "config" / "test.yaml": "# Config nhỏ dùng cho pytest\n",
    ROOT / "Makefile": (
        ".PHONY: generate test clean\n\n"
        "generate:\n\tuv run python -m coursework\n\n"
        "test:\n\tuv run pytest tests/ -v\n\n"
        "clean:\n\trm -rf data/offline/* data/streaming/* data/features/*\n"
    ),
}

# ── docstrings mặc định cho từng file ─────────────────────────────────────────

DOCSTRINGS = {
    "__init__.py": "",
    "__main__.py": '"""Entry point — uv run python -m coursework"""\n',
    "base_generator.py": '"""BaseGenerator — abstract base class cho tất cả generators."""\n',
    "config.py": '"""GeneratorConfig — load và validate config từ YAML."""\n',
    "schema.py": '"""DataSchema — định nghĩa Parquet schema và Avro schema."""\n',
    "utils.py": '"""RandomDataUtils — zipf weights, duplicate injection, late timestamp."""\n',
    "base_table_generator.py": '"""BaseTableGenerator — abstract base cho offline table generators."""\n',
    "users.py": '"""UsersGenerator."""\n',
    "videos.py": '"""VideosGenerator."""\n',
    "playback_history.py": '"""PlaybackHistoryGenerator — bao gồm session_id."""\n',
    "interactions.py": '"""InteractionsGenerator."""\n',
    "ad_impressions.py": '"""AdImpressionsGenerator — schema evolution: NULL trước schema_change_date."""\n',
    "offline_data_generator.py": '"""OfflineDataGenerator — orchestrate tất cả offline table generators."""\n',
    "base_event_generator.py": '"""BaseEventGenerator — abstract base cho streaming event generators."""\n',
    "playback_start.py": '"""PlaybackStartEventGenerator."""\n',
    "pause.py": '"""PauseEventGenerator."""\n',
    "skip.py": '"""SkipEventGenerator."""\n',
    "ad_impression.py": '"""AdImpressionEventGenerator."""\n',
    "subscription_cancel.py": '"""SubscriptionCancelEventGenerator."""\n',
    "stream_data_generator.py": '"""StreamDataGenerator — orchestrate streaming events, burst, late arrivals."""\n',
    "offline_calculator.py": '"""OfflineFeatureCalculator — tính 90-day stable features từ Parquet."""\n',
    "streaming_calculator.py": '"""StreamingFeatureCalculator — rolling window features từ event files."""\n',
    "feature_engineer.py": '"""FeatureEngineer — merge offline + streaming features."""\n',
    "orchestrator.py": '"""PipelineOrchestrator — chạy toàn bộ pipeline."""\n',
    "quality_report.py": '"""QualityReport — profile data và sinh báo cáo chất lượng."""\n',
    "conftest.py": '"""Pytest fixtures dùng chung."""\n',
    "test_utils.py": '"""Test RandomDataUtils: zipf_weights, inject_duplicates, generate_late_ts."""\n',
    "test_schema_evolution.py": '"""Test: midpoint/thirdQuartile là NULL trước schema_change_date."""\n',
    "test_referential_integrity.py": '"""Test: mọi user_id và video_id trong streaming/fact tables tồn tại trong dims."""\n',
    "test_pipeline.py": '"""Smoke test: chạy toàn bộ pipeline với config nhỏ (test.yaml)."""\n',
}


def write_file(path: Path, content: str) -> None:
    if path.exists():
        print(f"  skip (đã tồn tại)  {path.relative_to(ROOT)}")
        return
    path.write_text(content, encoding="utf-8")
    print(f"  created            {path.relative_to(ROOT)}")


def main() -> None:
    print("\n=== scaffold.py ===\n")

    # src package root
    SRC.mkdir(parents=True, exist_ok=True)
    for filename in SRC_ROOT_FILES:
        write_file(SRC / filename, DOCSTRINGS.get(filename, ""))

    # src subpackages
    for pkg, files in STRUCTURE.items():
        pkg_dir = SRC / pkg
        pkg_dir.mkdir(exist_ok=True)
        for filename in files:
            write_file(pkg_dir / filename, DOCSTRINGS.get(filename, ""))

    # tests
    TESTS.mkdir(exist_ok=True)
    for filename in TEST_FILES:
        write_file(TESTS / filename, DOCSTRINGS.get(filename, ""))

    # other dirs & files
    for d in OTHER_DIRS:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  mkdir              {d.relative_to(ROOT)}/")

    for path, content in OTHER_FILES.items():
        write_file(path, content)

    print("\n✓ xong — cấu trúc đã được tạo.\n")
    print("Bước tiếp theo:")
    print("  uv run python -m coursework             # chạy pipeline")
    print("  uv run pytest tests/ -v                 # chạy tests")
    print("  make generate / make test               # hoặc dùng Makefile\n")


if __name__ == "__main__":
    main()