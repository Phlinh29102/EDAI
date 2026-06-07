"""PipelineOrchestrator - run the full pipeline end-to-end."""
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.core.utils import RandomDataUtils
from data_generator.features.feature_engineer import FeatureEngineer
from data_generator.offline.offline_data_generator import OfflineDataGenerator
from data_generator.streaming.stream_data_generator import StreamDataGenerator
from data_pipeline.bronze.ingest_offline import OfflineBronzeIngestor


class PipelineOrchestrator:
    """Coordinate offline generation, streaming generation, and feature engineering."""

    def __init__(self, config_path: str | Path) -> None:
        self.config = GeneratorConfig(Path(config_path))
        schema = DataSchema()
        utils = RandomDataUtils(seed=self.config.get("random_seed"))
        data_dir = self.config.get("data_dir", {})

        self.offline_gen = OfflineDataGenerator(
            config=self.config,
            schema=schema,
            utils=utils,
            output_path=data_dir.get("offline", "data/offline"),
        )
        self.stream_gen = StreamDataGenerator(
            config=self.config,
            utils=utils,
            output_path=data_dir.get("streaming", "data/streaming"),
            schema=schema,
        )
        self.offline_bronze = OfflineBronzeIngestor(
            source_path=data_dir.get("offline", "data/offline"),
            output_path=data_dir.get("bronze", "data/bronze"),
        )
        self.feature_eng = FeatureEngineer(self.config)

    def run_offline(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, str]:
        """Generate and save all offline tables.

        Args:
            start_date: Ignored; date ranges are read from config.
            end_date: Ignored; date ranges are read from config.

        Returns:
            Dict[str, str]: Mapping of table name to written Parquet path.
        """
        return self.offline_gen.generate_all()

    def run_offline_bronze(self, batch_id: Optional[str] = None) -> Dict[str, str]:
        """Ingest generated offline Parquet tables into Bronze raw tables.

        Args:
            batch_id: Optional stable batch id for the ingestion run.

        Returns:
            Dict[str, str]: Mapping of source table name to Bronze Parquet path.
        """
        return self.offline_bronze.ingest_all(batch_id=batch_id)

    def run_streaming(self, run_duration: int) -> Dict[str, str]:
        """Generate and save streaming events.

        Args:
            run_duration: Number of minutes of streaming data to generate.

        Returns:
            Dict[str, str]: Mapping of hour partition to written Avro path.
        """
        start_ts = datetime.now(timezone.utc)
        user_contexts = self._build_streaming_contexts()
        return self.stream_gen.generate_and_save(
            start_ts=start_ts,
            minutes=run_duration,
            user_contexts=user_contexts,
        )

    def run_feature_engineering(self) -> str:
        """Compute, merge, and save unified user features.

        Returns:
            str: Path to the written feature Parquet file.
        """
        window_end = datetime.now(timezone.utc)
        features = self.feature_eng.merge_features(window_end)
        return self.feature_eng.save_features(features, window_end)

    def run_all(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        run_duration: int = 60,
    ) -> Dict[str, Any]:
        """Run the full pipeline: offline, streaming, then features.

        Args:
            start_date: Passed to run_offline (unused, config-driven).
            end_date: Passed to run_offline (unused, config-driven).
            run_duration: Minutes of streaming data to generate.

        Returns:
            Dict[str, Any]: Paths for all outputs.
        """
        offline_paths = self.run_offline(start_date, end_date)
        offline_bronze_paths = self.run_offline_bronze()
        stream_paths = self.run_streaming(run_duration)
        feature_path = self.run_feature_engineering()
        return {
            "offline": offline_paths,
            "offline_bronze": offline_bronze_paths,
            "streaming": stream_paths,
            "features": feature_path,
        }

    def summary(self) -> Dict[str, Any]:
        """Return a summary of the orchestrator and all sub-components.

        Returns:
            Dict[str, Any]: Orchestrator state and sub-generator summaries.
        """
        return {
            "orchestrator": self.__class__.__name__,
            "config_path": str(self.config.config_path),
            "offline": self.offline_gen.summary(),
            "offline_bronze": self.offline_bronze.summary(),
            "streaming": self.stream_gen.summary(),
            "features": self.feature_eng.summary(),
        }

    def _build_streaming_contexts(self) -> List[Dict[str, Any]]:
        tables = self.feature_eng.load_offline()
        users = tables.get("users", pd.DataFrame())
        videos = tables.get("videos", pd.DataFrame())

        if users.empty:
            return []

        contexts: List[Dict[str, Any]] = []
        for idx, (_, user) in enumerate(users.iterrows()):
            video = videos.iloc[idx % len(videos)] if not videos.empty else None
            context: Dict[str, Any] = {
                "user_id": user.get("user_id", f"user_{idx + 1:08d}"),
                "session_id": f"session_{idx + 1:010d}",
            }
            if video is not None:
                context["video_id"] = video.get("video_id")
                context["genre_id"] = video.get("video_genre")
            contexts.append(context)

        return contexts
