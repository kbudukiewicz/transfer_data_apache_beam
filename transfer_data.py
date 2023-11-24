"""Download data from url with comments and posts, then transfer to SQL data."""
import logging
from typing import Any

import apache_beam as beam
import requests
from apache_beam.io.jdbc import WriteToJdbc


def download_data(pipeline: beam.Pipeline, url: str) -> beam.Pipeline:
    """Download data from url.

    Args:
        pipeline: pipeline for process
        url: url to download data.

    Returns:
        Apache Beam collection with data from url.

    Raises:
        If wrong call to url.
    """
    try:
        response = requests.get(url=url, timeout=60).json()
        return pipeline | f"Create collection for {url} data" >> beam.Create(response)
    except ValueError as error:
        raise f"Error with download data from {url} - {error}."


def _parse_data(coll: beam.PCollection, data_name: str) -> beam.PCollection:
    """Parse data.

    Args:
        coll: collection data to parse.
        data_name: name for the parse data.

    Returns:
        Parsed Collection, f.e (123, {'id': 123, 'count_comments': 100}).
    """
    return coll | f"Parse data {data_name}" >> beam.Map(lambda x: (x["id"], x))


def data_to_mysql(
    data: beam.PCollection,
    table_name: str,
    database_name: str,
    host_name: str,
    username: str,
    password: str,
    driver_class: str,
) -> None:
    """Send data to SQL table.

    Args:
        data: data to SQL
        table_name: name of the table.
        database_name: name of the database.
        host_name: name of the host.
        username: name of the user.
        password: password.
        driver_class: name class for the driver.
    """
    try:
        data | "Write to SQL" >> WriteToJdbc(
            table_name=table_name,
            jdbc_url=f"{host_name}/{database_name}",
            username=username,
            password=password,
            driver_class_name=driver_class,
        )
        logging.info(f"Transfer data to {table_name} successfully.")
    except ValueError as error:
        raise f"Error with transfer data to SQL - {error}."


def run(url_posts: str, url_comments: str, database_config: dict[str, Any]) -> None:
    """Run process for download posts and comments data. Then transfer data to SQL database.

    Args:
        url_posts: url for posts data.
        url_comments: url for comments data.
        database_config: SQL database config.
    """
    with beam.Pipeline() as pipeline:
        comments = download_data(pipeline=pipeline, url=url_comments)
        posts = download_data(pipeline=pipeline, url=url_posts)

        count_comments = (
            comments
            | "Count comments for posts" >> beam.Map(lambda x: (x["postId"], 1))
            | beam.CombinePerKey(sum)
            | beam.Map(lambda x: {"id": x[0], "count_comments": x[1]})
        )

        final_data = (
            {
                "count_comments": _parse_data(
                    count_comments, data_name="count_comments"
                ),
                "posts": _parse_data(posts, data_name="posts"),
            }
            | beam.CoGroupByKey()
            | beam.Map(
                lambda x: {"id": x[0], **x[1]["count_comments"][0], **x[1]["posts"][0]}
            )
        )

        data_to_mysql(data=final_data, table_name=TABLE_NAME, **database_config)


if __name__ == "__main__":
    TABLE_NAME = "final"
    DATABASE_CONFIG = {
        "database_name": "database",
        "driver_class": "org.mysql.Driver",
        "host_name": "jdbc:mysql://localhost:1234",
        "username": "user",
        "password": "password",
    }

    URL_POSTS = "https://jsonplaceholder.typicode.com/posts"
    URL_COMMENTS = "https://jsonplaceholder.typicode.com/comments"
    run(url_posts=URL_POSTS, url_comments=URL_COMMENTS, database_config=DATABASE_CONFIG)
