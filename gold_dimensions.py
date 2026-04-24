from pyspark import pipelines as dp
from pyspark.sql.functions import *


# =============================================================================
# GOLD GAMES
# =============================================================================

@dp.table()
def Dim_Games_Stage():
    return spark.readStream.table("silver_games")


@dp.temporary_view()
def Dim_Games_view():
    return spark.readStream.table("Dim_Games_Stage")


dp.create_streaming_table("Dim_Games")

dp.create_auto_cdc_flow(
    target="Dim_Games",
    source="Dim_Games_view",
    keys=["game_id"],
    sequence_by="game_date",
    stored_as_scd_type=2
)


# =============================================================================
# GOLD PLAYERS
# =============================================================================

@dp.temporary_view()
def Dim_Players_view():
    df = spark.read.table("silver_players")

    # Add age and age group
    df = df.withColumn("age", year(current_date()) - year(to_date(col("birth_date"))))
    df = df.withColumn("age_group",
        when(col("age") < 25, "Young")
        .when(col("age").between(25, 40), "Middle")
        .otherwise("Seniors"))

    # Add salary category
    df = df.withColumn("player_salary",
        when(col("salary_usd").cast("int") >= 5000000, "High")
        .when(col("salary_usd").cast("int").between(2000000, 4999999), "Mid")
        .otherwise("Low"))

    # Join with teams
    df_teams = spark.read.table("silver_teams").select("team_id", "season", "wins")
    df = df.join(df_teams, on="team_id", how="left")

    # Join with regions
    df_regions = spark.read.table("silver_regions").select(
        col("region_id").cast("string").alias("region_id"), "region_name"
    )
    df = df.join(df_regions, "region_id", "left")

    # Compute BMI
    df = df.withColumn("BMI_BodyMassIndex",
        col("weight_kg").cast("double") / ((col("height_cm").cast("double") / 100) ** 2))

    # Deduplicate by player_id
    df = df.dropDuplicates(subset=["player_id"])

    return df


@dp.materialized_view(name="Dim_Players_v2")
def Dim_Players():
    return spark.read.table("Dim_Players_view")


# =============================================================================
# GOLD TEAMS
# =============================================================================

@dp.table()
def Dim_Teams_Stage():
    return spark.readStream.table("sports_catalog.silver.teams_silver")


@dp.temporary_view()
def Dim_Teams_view():
    return spark.readStream.table("Dim_Teams_Stage")


dp.create_streaming_table("Dim_Teams")

dp.create_auto_cdc_flow(
    target="Dim_Teams",
    source="Dim_Teams_view",
    keys=["stat_id"],
    sequence_by="stat_id",
    stored_as_scd_type=2
)


# =============================================================================
# GOLD REGIONS
# =============================================================================

@dp.table()
def Dim_Regions_Stage():
    return spark.readStream.table("sports_catalog.silver.regions_silver")


@dp.temporary_view()
def Dim_Regions_view():
    return spark.readStream.table("Dim_Regions_Stage")


dp.create_streaming_table("Dim_Regions")

dp.create_auto_cdc_flow(
    target="Dim_Regions",
    source="Dim_Regions_view",
    keys=["region_id"],
    sequence_by="region_id",
    stored_as_scd_type=2
)
