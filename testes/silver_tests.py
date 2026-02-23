import pytest
from unittest.mock import patch, MagicMock, call
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from transform_openbrewery_silver import transform_openbrewery, write_silver, run


# ─────────────────────────────────────────────
# Spark Session para testes
# ─────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Cria uma SparkSession local para os testes."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_bronze_data(spark):
    """DataFrame simulando dados da camada bronze."""
    data = [
        ("abc-1", "Brewery One", "micro", "Denver", "Colorado", "United States", "39.73", "-104.99"),
        ("abc-2", "Brewery Two", "BREWPUB", "Austin", "Texas", "United States", "30.26", "-97.74"),
        ("abc-3", "Brewery Three", "regional", "Portland", "Oregon", "United States", "45.52", "-122.67"),
        ("abc-1", "Brewery One", "micro", "Denver", "Colorado", "United States", "39.73", "-104.99"),  # duplicata
        (None,   "Brewery Null", "micro", "Seattle", "Washington", "United States", "47.60", "-122.33"),  # id nulo
    ]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────
# transform_openbrewery tests
# ─────────────────────────────────────────────

class TestTransformOpenbrewery:

    @patch("transform_openbrewery_silver.spark")
    def test_removes_duplicates(self, mock_spark, sample_bronze_data):
        """Deve remover linhas duplicadas pelo campo id."""
        mock_spark.table.return_value = sample_bronze_data

        result = transform_openbrewery()
        ids = [row["id"] for row in result.collect() if row["id"] is not None]

        assert len(ids) == len(set(ids)), "Existem IDs duplicados no resultado"

    @patch("transform_openbrewery_silver.spark")
    def test_removes_null_ids(self, mock_spark, sample_bronze_data):
        """Deve filtrar registros com id nulo."""
        mock_spark.table.return_value = sample_bronze_data

        result = transform_openbrewery()
        null_ids = [row for row in result.collect() if row["id"] is None]

        assert len(null_ids) == 0

    @patch("transform_openbrewery_silver.spark")
    def test_brewery_type_is_lowercase(self, mock_spark, sample_bronze_data):
        """Deve converter brewery_type para lowercase."""
        mock_spark.table.return_value = sample_bronze_data

        result = transform_openbrewery()
        types = [row["brewery_type"] for row in result.collect()]

        for t in types:
            assert t == t.lower(), f"brewery_type '{t}' não está em lowercase"

    @patch("transform_openbrewery_silver.spark")
    def test_latitude_longitude_are_double(self, mock_spark, sample_bronze_data):
        """Deve converter latitude e longitude para double."""
        mock_spark.table.return_value = sample_bronze_data

        result = transform_openbrewery()
        schema_fields = {f.name: f.dataType for f in result.schema.fields}

        assert isinstance(schema_fields["latitude"], DoubleType)
        assert isinstance(schema_fields["longitude"], DoubleType)

    @patch("transform_openbrewery_silver.spark")
    def test_ingestion_date_column_exists(self, mock_spark, sample_bronze_data):
        """Deve adicionar a coluna ingestion_date."""
        mock_spark.table.return_value = sample_bronze_data

        result = transform_openbrewery()
        columns = result.columns

        assert "ingestion_date" in columns

    @patch("transform_openbrewery_silver.spark")
    def test_selected_columns_are_present(self, mock_spark, sample_bronze_data):
        """Deve retornar apenas as colunas esperadas."""
        mock_spark.table.return_value = sample_bronze_data

        result = transform_openbrewery()
        expected_columns = {"id", "name", "brewery_type", "city", "state", "country", "latitude", "longitude", "ingestion_date"}

        assert expected_columns == set(result.columns)

    @patch("transform_openbrewery_silver.spark")
    def test_reads_from_bronze_table(self, mock_spark, sample_bronze_data):
        """Deve ler os dados da tabela bronze.openbrewery_raw."""
        mock_spark.table.return_value = sample_bronze_data

        transform_openbrewery()

        mock_spark.table.assert_called_once_with("bronze.openbrewery_raw")


# ─────────────────────────────────────────────
# write_silver tests
# ─────────────────────────────────────────────

class TestWriteSilver:

    @patch("transform_openbrewery_silver.spark")
    def test_creates_silver_schema(self, mock_spark, sample_bronze_data):
        """Deve criar o schema silver se não existir."""
        mock_df = MagicMock()

        write_silver(mock_df)

        mock_spark.sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS silver")

    @patch("transform_openbrewery_silver.spark")
    def test_saves_with_overwrite_mode(self, mock_spark, sample_bronze_data):
        """Deve salvar com modo overwrite para garantir idempotência."""
        mock_df = MagicMock()

        write_silver(mock_df)

        mock_df.write.mode.assert_called_once_with("overwrite")

    @patch("transform_openbrewery_silver.spark")
    def test_partitions_by_country_and_state(self, mock_spark, sample_bronze_data):
        """Deve particionar por country e state."""
        mock_df = MagicMock()

        write_silver(mock_df)

        mock_df.write.mode.return_value.partitionBy.assert_called_once_with("country", "state")

    @patch("transform_openbrewery_silver.spark")
    def test_saves_to_silver_table(self, mock_spark, sample_bronze_data):
        """Deve salvar na tabela silver.openbrewery."""
        mock_df = MagicMock()

        write_silver(mock_df)

        mock_df.write.mode.return_value.partitionBy.return_value.saveAsTable.assert_called_once_with("silver.openbrewery")


# ─────────────────────────────────────────────
# run tests
# ─────────────────────────────────────────────

class TestRun:

    @patch("transform_openbrewery_silver.write_silver")
    @patch("transform_openbrewery_silver.transform_openbrewery")
    def test_run_calls_transform_and_write(self, mock_transform, mock_write):
        """Deve chamar transform_openbrewery e write_silver em sequência."""
        mock_df = MagicMock()
        mock_transform.return_value = mock_df

        run()

        mock_transform.assert_called_once()
        mock_write.assert_called_once_with(mock_df)

    @patch("transform_openbrewery_silver.write_silver")
    @patch("transform_openbrewery_silver.transform_openbrewery")
    def test_run_passes_transformed_data_to_write(self, mock_transform, mock_write):
        """Deve passar o DataFrame transformado corretamente para o write."""
        mock_df = MagicMock()
        mock_transform.return_value = mock_df

        run()

        mock_write.assert_called_once_with(mock_df)