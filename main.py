"""Run the full pipeline and generate a quality report."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from data_generator.core.config import GeneratorConfig
from data_generator.pipeline.orchestrator import PipelineOrchestrator
from data_generator.pipeline.quality_report import QualityReport


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/default.yaml"

    if not Path(config_path).exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    print(f"Using config: {config_path}")
    print()

    orchestrator = PipelineOrchestrator(config_path)
    config = GeneratorConfig(Path(config_path))
    run_duration = config.get("run_duration_minutes", 5)

    print("=== Step 1: Offline data generation ===")
    offline_paths = orchestrator.run_offline()
    for table, path in offline_paths.items():
        print(f"  {table}: {path}")
    print()

    print(f"=== Step 2: Streaming data generation ({run_duration} min) ===")
    stream_paths = orchestrator.run_streaming(run_duration=run_duration)
    for hour, path in stream_paths.items():
        print(f"  hour={hour}: {path}")
    print()

    print("=== Step 3: Feature engineering ===")
    feature_path = orchestrator.run_feature_engineering()
    print(f"  features: {feature_path}")
    print()

    print("=== Step 4: Quality report ===")
    qr = QualityReport("data/reports", config=config)
    metrics = {}

    for table in ["users", "videos", "playback_history", "interactions", "ad_impressions"]:
        path = offline_paths.get(table)
        if path:
            metrics[table] = qr.profile_offline_tables(table, path)

    stream_root = list(Path("data/streaming").iterdir()) if Path("data/streaming").exists() else []
    if stream_root:
        metrics["streaming"] = qr.profile_stream("data/streaming")

    if Path(feature_path).exists():
        metrics["features"] = qr.profile_features(feature_path)

    report = qr.generate_report(metrics)
    print(report)
    print()
    print(f"Report saved to: {qr.summary()['last_report_path']}")


if __name__ == "__main__":
    main()
