from unittest.mock import patch, MagicMock

from weather import get_coords_from_zipcode, get_current_weather


class TestGetCoordsFromZipcode:
    @patch("weather.session")
    def test_success(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"lat": 42.3, "lon": -71.8}
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        lat, lon = get_coords_from_zipcode("01701", "US", "fake-key")

        assert lat == 42.3
        assert lon == -71.8
        mock_session.get.assert_called_once()
        assert "01701" in mock_session.get.call_args[0][0]

    @patch("weather.session")
    def test_http_error_raises(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("404 Not Found")
        mock_session.get.return_value = mock_resp

        try:
            get_coords_from_zipcode("00000", "US", "fake-key")
            assert False, "Should have raised"
        except Exception as e:
            assert "404" in str(e)


class TestGetCurrentWeather:
    @patch("weather.session")
    def test_success(self, mock_session):
        weather_payload = {
            "main": {"temp": 72.5, "humidity": 45},
            "weather": [{"main": "Clear", "description": "clear sky"}],
            "name": "Framingham",
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = weather_payload
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        result = get_current_weather(42.3, -71.8, "fake-key")

        assert result == weather_payload
        assert result["main"]["temp"] == 72.5

    @patch("weather.session")
    def test_failure_returns_none(self, mock_session):
        mock_session.get.side_effect = Exception("Connection error")

        result = get_current_weather(42.3, -71.8, "fake-key")

        assert result is None
