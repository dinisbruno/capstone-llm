import argparse
import json
import logging
import os

import boto3
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)


def _read_items(spark: SparkSession, path: str) -> DataFrame:
    """Read a StackExchange API JSON dump and return one row per item.

    The API wraps results as ``{"items": [ ... ], "has_more": ...}``, so the
    file is a single (multi-line) JSON object. We read it with multiLine and,
    if there is a top-level ``items`` array, explode it into one row per item.
    If the file is already a bare array of items, we use it as-is.
    """
    df = spark.read.option("multiLine", True).json(path)
    if "items" in df.columns:
        df = df.select(F.explode("items").alias("item")).select("item.*")
    return df


def clean(spark: SparkSession, environment: str, tag: str):
    # The output path must match the `s3_path` fixture in tests/test_clean.py,
    # e.g. "cleaned/<user>/<tag>". Set CAPSTONE_USER to your name.
    user = os.environ.get("CAPSTONE_USER", "changeme")

    input_prefix = f"s3a://{llm_bucket}/input/{tag}"
    logger.info(f"Reading questions and answers for tag '{tag}' from {input_prefix}")

    # Keep only the fields we need, renamed to the target output schema.
    questions = _read_items(spark, f"{input_prefix}/questions.json").select(
        F.col("question_id"),
        F.col("title"),
        F.col("body").alias("question"),
        F.col("link"),
    )

    # One document per question: keep a single answer per question, preferring
    # the accepted one, then the highest score. Change this if you'd rather
    # keep every answer (one row per answer instead of per question).
    best_answer = F.row_number().over(
        Window.partitionBy("question_id").orderBy(
            F.col("is_accepted").desc_nulls_last(),
            F.col("score").desc_nulls_last(),
        )
    )
    answers = (
        _read_items(spark, f"{input_prefix}/answers.json")
        .withColumn("rank", best_answer)
        .filter(F.col("rank") == 1)
        .select(
            F.col("question_id"),
            F.col("answer_id"),
            F.col("body").alias("answer"),
        )
    )

    cleaned = questions.join(answers, on="question_id", how="inner")

    # Write one JSON document per question. Spark's df.write.json() emits
    # JSON-Lines part files (many records per file); the test loads a whole
    # file with json.loads() and expects a single object, so we write each
    # record as its own S3 object via boto3.
    output_prefix = f"cleaned/{user}/{tag}"
    s3 = boto3.client("s3")

    records = [json.loads(r) for r in cleaned.toJSON().collect()]
    logger.info(f"Writing {len(records)} cleaned documents to s3://{llm_bucket}/{output_prefix}")

    for record in records:
        key = f"{output_prefix}/{record['question_id']}.json"
        s3.put_object(
            Bucket=llm_bucket,
            Key=key,
            Body=json.dumps(record),
            ContentType="application/json",
        )

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        builder = SparkSession.builder.appName("Spark S3 Integration").config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        for key, value in common_spark_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
