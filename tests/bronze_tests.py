import pytest
from unittest.mock import patch, MagicMock
from requests.exceptions import Timeout, HTTPError
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingest_openbrewery_bronze import fetch_breweries, write_bronze_layer, run


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

SAMPLE_BREWERY = {
    "id": "abc-123",
    "name": "Sample Brewery",
    "brewery_type": "micro",
    "city": "Denver",
    "state": "Colorado",
    "country": "United States",
    "latitude": "39.7392",
    "longitude": "-104.9903"
}


# ─────────────────────────────────────────────
# fetch_breweries tests
# ─────────────────────────────────────────────

class TestFetchBreweries:

    @patch("ingest_openbrewery_bronze.requests.get")
    def test_returns_list_of_breweries(self, mock_get):
        """Deve retornar uma lista com dados das cervejarias."""
        mock_response = MagicMock()
        mock_response.json.side_effect = [[SAMPLE_BREWERY] * 5, []]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_breweries(page_size=5, max_pages=2)

        assert isinstance(result, list)
        assert len(result) == 5
        assert result[0]["id"] == "abc-123"

    @patch("ingest_openbrewery_bronze.requests.get")
    def test_stops_on_empty_page(self, mock_get):
        """Deve parar de paginar quando a API retornar página vazia."""
        mock_response = MagicMock()
        mock_response.json.side_effect = [[SAMPLE_BREWERY], []]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_breweries(page_size=200, max_pages=5)

        assert len(result) == 1
        assert mock_get.call_count == 2

    @patch("ingest_openbrewery_bronze.requests.get")
    def test_raises_on_http_error(self, mock_get):
        """Deve propagar erro HTTP quando a API retornar status de erro."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(HTTPError):
            fetch_breweries()

    @patch("ingest_openbrewery_bronze.requests.get")
    def test_raises_on_timeout(self, mock_get):
        """Deve propagar erro de timeout quando a API demorar demais."""
        mock_get.side_effect = Timeout("Connection timed out")

        with pytest.raises(Timeout):
            fetch_breweries()

    @patch("ingest_openbrewery_bronze.requests.get")
    def test_respects_max_pages(self, mock_get):
        """Deve respeitar o limite máximo de páginas."""
        mock_response = MagicMock()
        mock_response.json.return_value = [SAMPLE_BREWERY]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        fetch_breweries(page_size=1, max_pages=3)

        assert mock_get.call_count == 3

    @patch("ingest_openbrewery_bronze.requests.get")
    def test_returns_empty_list_when_first_page_empty(self, mock_get):
        """Deve retornar lista vazia se a primeira página já vier vazia."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_breweries()

        assert result == []


# ─────────────────────────────────────────────
# write_bronze_layer tests
# ─────────────────────────────────────────────

class TestWriteBronzeLayer:

    @patch("ingest_openbrewery_bronze.spark")
    def test_creates_dataframe_and_saves(self, mock_spark):
        """Deve criar um DataFrame e salvar como tabela bronze."""
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.write.mode.return_value.saveAsTable = MagicMock()

        write_bronze_layer([SAMPLE_BREWERY])

        mock_spark.createDataFrame.assert_called_once_with([SAMPLE_BREWERY])
        mock_df.write.mode.assert_called_once_with("overwrite")
        mock_df.write.mode.return_value.saveAsTable.assert_called_once_with("bronze.openbrewery_raw")

    @patch("ingest_openbrewery_bronze.spark")
    def test_uses_overwrite_mode(self, mock_spark):
        """Deve usar modo overwrite para garantir idempotência."""
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        write_bronze_layer([SAMPLE_BREWERY])

        mock_df.write.mode.assert_called_with("overwrite")


# ─────────────────────────────────────────────
# run tests
# ─────────────────────────────────────────────

class TestRun:

    @patch("ingest_openbrewery_bronze.write_bronze_layer")
    @patch("ingest_openbrewery_bronze.fetch_breweries")
    def test_run_calls_fetch_and_write(self, mock_fetch, mock_write):
        """Deve chamar fetch_breweries e write_bronze_layer em sequência."""
        mock_fetch.return_value = [SAMPLE_BREWERY]

        run()

        mock_fetch.assert_called_once()
        mock_write.assert_called_once_with([SAMPLE_BREWERY])

    @patch("ingest_openbrewery_bronze.write_bronze_layer")
    @patch("ingest_openbrewery_bronze.fetch_breweries")
    def test_run_passes_fetched_data_to_write(self, mock_fetch, mock_write):
        """Deve passar os dados buscados corretamente para o write."""
        data = [SAMPLE_BREWERY, SAMPLE_BREWERY]
        mock_fetch.return_value = data

        run()

        mock_write.assert_called_once_with(data)