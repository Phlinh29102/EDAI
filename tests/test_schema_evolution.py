import pandas as pd


def test_schema_evolution_fields_are_null_before_change_date(
    dummy_config,
    sample_ad_impressions_df,
):
    """
    Verify that ad tracking fields (midpoint, third_quartile) introduced
    in the schema change are strictly NULL before the schema_change_date.
    """
    schema_change_date_str = dummy_config.get("schema_change_date", "2026-04-01")
    schema_change_date = pd.to_datetime(schema_change_date_str).date()
    df = sample_ad_impressions_df

    # Validate pre-schema change (legacy data)
    before_change = df[df["playback_date"] < schema_change_date]
    assert not before_change.empty, "Test must include data before schema change date"
    assert before_change["midpoint"].isnull().all(), "midpoint must be NULL in legacy partitions"
    assert before_change["third_quartile"].isnull().all(), "third_quartile must be NULL in legacy partitions"

    # Validate post-schema change (updated VAST spec)
    after_change = df[df["playback_date"] >= schema_change_date]
    assert not after_change.empty, "Test must include data after schema change date"
    assert after_change["midpoint"].notnull().all(), "midpoint must be populated after schema change"
    assert after_change["third_quartile"].notnull().all(), "third_quartile must be populated after schema change"
