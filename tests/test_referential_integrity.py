def test_referential_integrity_in_fact_tables(
    sample_users_df,
    sample_videos_df,
    sample_playback_history_df,
    sample_interactions_df,
    sample_ad_impressions_df,
):
    """
    Verify that all user_id and video_id references in fact/streaming
    tables exist in the respective dimension tables.
    """
    valid_users = set(sample_users_df["user_id"])
    valid_videos = set(sample_videos_df["video_id"])
    fact_tables = {
        "playback_history": sample_playback_history_df,
        "interactions": sample_interactions_df,
        "ad_impressions": sample_ad_impressions_df,
    }

    for table_name, fact_df in fact_tables.items():
        invalid_users = fact_df[~fact_df["user_id"].isin(valid_users)]
        assert invalid_users.empty, (
            f"{table_name} contains orphaned user_ids: "
            f"{invalid_users['user_id'].tolist()}"
        )

        invalid_videos = fact_df[~fact_df["video_id"].isin(valid_videos)]
        assert invalid_videos.empty, (
            f"{table_name} contains orphaned video_ids: "
            f"{invalid_videos['video_id'].tolist()}"
        )


def test_fact_tables_use_user_video_pairs_from_playback_sessions(
    sample_playback_history_df,
    sample_interactions_df,
    sample_ad_impressions_df,
):
    playback_pairs = set(
        zip(
            sample_playback_history_df["user_id"],
            sample_playback_history_df["video_id"],
        )
    )

    interaction_pairs = set(
        zip(sample_interactions_df["user_id"], sample_interactions_df["video_id"])
    )
    impression_pairs = set(
        zip(sample_ad_impressions_df["user_id"], sample_ad_impressions_df["video_id"])
    )

    assert interaction_pairs.issubset(playback_pairs)
    assert impression_pairs.issubset(playback_pairs)


def test_playback_history_session_id_links_multiple_video_watches(
    sample_playback_history_df,
):
    watches_per_session = sample_playback_history_df.groupby("session_id").size()
    users_per_session = sample_playback_history_df.groupby("session_id")["user_id"].nunique()
    dates_per_session = sample_playback_history_df.groupby("session_id")[
        "playback_date"
    ].nunique()

    assert watches_per_session.max() > 1
    assert users_per_session.max() == 1
    assert dates_per_session.max() == 1
