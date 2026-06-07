"""OfflineDataGenerator - orchestrate all offline table generators."""
from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from data_generator.core.config import GeneratorConfig
from data_generator.core.schema import DataSchema
from data_generator.core.utils import RandomDataUtils
from data_generator.offline.ad_impressions import AdImpressionGenerator
from data_generator.offline.base_table_generator import BaseTableGenerator
from data_generator.offline.interactions import InteractionGenerator
from data_generator.offline.playback_history import PlaybackHistoryGenerator
from data_generator.offline.users import UsersGenerator
from data_generator.offline.videos import VideoGenerator


class _GeneratorAdapter(BaseTableGenerator):
    """Adapt standalone table generators to the BaseTableGenerator interface."""

    def __init__(
        self,
        config: GeneratorConfig,
        schema,
        utils: RandomDataUtils,
        generator,
    ) -> None:
        super().__init__(config=config, schema=schema, utils=utils)
        self.generator = generator

    def generate(self) -> pd.DataFrame:
        return self.generator.generate()

    def summary(self) -> Dict[str, Any]:
        base_summary = super().summary()
        if hasattr(self.generator, "summary"):
            base_summary["generator_config"] = self.generator.summary()
        return base_summary


class OfflineDataGenerator:
    """Generate and save all offline datasets."""

    def __init__(
        self,
        config: GeneratorConfig,
        schema: DataSchema,
        utils: RandomDataUtils,
        output_path: str,
    ) -> None:
        """
        Initialize the offline data generator.

        Args:
            config (GeneratorConfig): Loaded generator configuration.
            schema (DataSchema): Offline table schemas.
            utils (RandomDataUtils): Shared random-data utility instance.
            output_path (str): Root directory for offline Parquet outputs.
        """
        self.config = config
        self.schema = schema
        self.utils = utils
        self.output_path = output_path
        self.table_generators: Dict[str, BaseTableGenerator] = {}

    def generate_all(self) -> Dict[str, str]:
        """
        Generate and save every offline table.

        Returns:
            Dict[str, str]: Mapping of table name to written Parquet path.
        """
        self._build_dimension_generators()
        users_df = self.table_generators["users"].post_process()
        videos_df = self.table_generators["videos"].post_process()

        self._build_fact_generators(
            user_ids=users_df["user_id"].tolist(),
            video_ids=videos_df["video_id"].tolist(),
        )
        playback_df = self.table_generators["playback_history"].post_process()
        self._build_dependent_fact_generators(
            user_ids=users_df["user_id"].tolist(),
            video_ids=videos_df["video_id"].tolist(),
            playback_history_df=playback_df,
        )

        return self._save_tables()

    def save_all(self) -> None:
        """Generate and save all offline tables."""
        self.generate_all()

    def summary(self) -> Dict[str, Any]:
        """
        Provide a summary of all offline table generators.

        Returns:
            Dict[str, Any]: Offline generator state and table summaries.
        """
        return {
            "generator": self.__class__.__name__,
            "output_path": self.output_path,
            "tables": {
                table_name: generator.summary()
                for table_name, generator in self.table_generators.items()
            },
        }

    def _build_dimension_generators(self) -> None:
        seed = self.config.get("random_seed")
        self.table_generators["users"] = self._adapt(
            "users",
            UsersGenerator(
                n_users=self.config.get("n_users", 1_000),
                seed=seed,
            ),
        )
        self.table_generators["videos"] = self._adapt(
            "videos",
            VideoGenerator(
                n_videos=self.config.get("n_videos", 1_000),
                genre_skew=self._genre_skew(),
                seed=seed,
            ),
        )

    def _build_fact_generators(self, user_ids: list[str], video_ids: list[str]) -> None:
        seed = self.config.get("random_seed")
        n_users = len(user_ids)
        n_sessions = self.config.get("n_playback_sessions", n_users * 5)
        n_interactions = self.config.get("n_interactions", n_users * 3)
        n_impressions = self.config.get("n_ad_impressions", n_users * 2)
        duplicate_rate = self.config.get("duplicate_rate_offline", 0.0)

        self.table_generators["playback_history"] = self._adapt(
            "playback_history",
            PlaybackHistoryGenerator(
                days_history=self.config.get("days_history", 30),
                skew_ratio=self._popularity_skew(),
                duplicate_rate=duplicate_rate,
                user_ids=user_ids,
                video_ids=video_ids,
                n_sessions=n_sessions,
                playback_end=self._playback_end_date(),
                seed=seed,
            ),
        )

    def _build_dependent_fact_generators(
        self,
        user_ids: list[str],
        video_ids: list[str],
        playback_history_df: pd.DataFrame,
    ) -> None:
        seed = self.config.get("random_seed")
        n_users = len(user_ids)
        n_interactions = self.config.get("n_interactions", n_users * 3)
        n_impressions = self.config.get("n_ad_impressions", n_users * 2)
        duplicate_rate = self.config.get("duplicate_rate_offline", 0.0)

        self.table_generators["interactions"] = self._adapt(
            "interactions",
            InteractionGenerator(
                duplicate_rate=duplicate_rate,
                user_ids=user_ids,
                video_ids=video_ids,
                playback_history_df=playback_history_df,
                n_interactions=n_interactions,
                seed=seed,
            ),
        )
        self.table_generators["ad_impressions"] = self._adapt(
            "ad_impressions",
            AdImpressionGenerator(
                advertiser_id_pool=self.config.get("advertiser_ids_pool", 75),
                cost_nanos_range=tuple(
                    self.config.get("cost_nanos_range", [500_000_000, 5_000_000_000])
                ),
                schema_change_date=self._schema_change_date(),
                user_ids=user_ids,
                video_ids=video_ids,
                playback_history_df=playback_history_df,
                n_impressions=n_impressions,
                ad_click_rate=self.config.get("ad_click_rate", 0.03),
                playback_start=self._playback_start_date(),
                playback_end=self._playback_end_date(),
                seed=seed,
            ),
        )

    def _adapt(self, table_name: str, generator) -> BaseTableGenerator:
        return _GeneratorAdapter(
            config=self.config,
            schema=self.schema.to_pyarrow_schema(table_name),
            utils=self.utils,
            generator=generator,
        )

    def _save_tables(self) -> Dict[str, str]:
        output_paths: Dict[str, str] = {}
        for table_name, generator in self.table_generators.items():
            table_path = Path(self.output_path) / table_name
            output_paths[table_name] = generator.save(table_path)
        return output_paths

    def _schema_change_date(self) -> date:
        value = self.config.get("schema_change_date", "2026-04-01")
        return self._parse_date(value)

    def _playback_start_date(self) -> date:
        value = self.config.get(
            "playback_start",
            self.config.get("start_date", "2025-04-01"),
        )
        return self._parse_date(value)

    def _playback_end_date(self) -> date:
        value = self.config.get(
            "playback_end",
            self.config.get("end_date", "2026-01-31"),
        )
        return self._parse_date(value)

    def _parse_date(self, value) -> date:
        if isinstance(value, date):
            return value
        return date.fromisoformat(value)

    def _popularity_skew(self) -> float:
        return self.config.get(
            "skew_ratio_popularity",
            self.config.get("popular_catalog_ratio", 1.0),
        )

    def _genre_skew(self) -> float:
        return self.config.get(
            "skew_ratio_genre",
            self.config.get("popular_genre_ratio", 1.0),
        )
