from unittest.mock import patch, MagicMock, call

import main


FAKE_WEATHER = {
    "main": {"temp": 72.5},
    "weather": [{"description": "clear sky"}],
    "name": "TestCity",
}


class TestResolveZipcodes:
    @patch("main.get_coords_from_zipcode")
    @patch.object(main, "ZIPCODE", "01701")
    @patch.object(main, "ZIPCODE_2", None)
    def test_single_zipcode(self, mock_coords):
        mock_coords.return_value = (42.3, -71.8)

        result = main.resolve_zipcodes()

        assert result == [("01701", 42.3, -71.8)]
        mock_coords.assert_called_once()

    @patch("main.get_coords_from_zipcode")
    @patch.object(main, "ZIPCODE", "01701")
    @patch.object(main, "ZIPCODE_2", "10001")
    def test_two_zipcodes(self, mock_coords):
        mock_coords.side_effect = [(42.3, -71.8), (40.7, -74.0)]

        result = main.resolve_zipcodes()

        assert len(result) == 2
        assert result[0] == ("01701", 42.3, -71.8)
        assert result[1] == ("10001", 40.7, -74.0)
        assert mock_coords.call_count == 2


class TestStreamWeatherData:
    def _run_stream(self, locations, weather_returns, max_sends=4):
        """Helper: run stream_weather_data, stop after max_sends calls to producer.send."""
        mock_producer = MagicMock()
        send_count = 0

        def counting_send(*args, **kwargs):
            nonlocal send_count
            send_count += 1
            if send_count >= max_sends:
                raise KeyboardInterrupt

        mock_producer.send.side_effect = counting_send

        def fresh_weather(*args, **kwargs):
            return dict(FAKE_WEATHER)

        with (
            patch("main.resolve_zipcodes", return_value=locations),
            patch("main.create_kafka_producer", return_value=mock_producer),
            patch("main.get_current_weather", side_effect=fresh_weather),
            patch("main.time.sleep"),
            patch.object(main, "KAFKA_TOPIC", "test-topic"),
        ):
            try:
                main.stream_weather_data()
            except KeyboardInterrupt:
                pass

        return mock_producer

    def test_round_robin_order(self):
        locations = [("01701", 42.3, -71.8), ("10001", 40.7, -74.0)]
        mock_producer = self._run_stream(locations, None, max_sends=4)

        keys = [c.kwargs["key"] for c in mock_producer.send.call_args_list]
        assert keys == ["01701", "10001", "01701", "10001"]

    def test_attaches_zipcode_to_payload(self):
        locations = [("01701", 42.3, -71.8), ("10001", 40.7, -74.0)]
        mock_producer = self._run_stream(locations, None, max_sends=2)

        for i, loc in enumerate(locations):
            sent_value = mock_producer.send.call_args_list[i].kwargs["value"]
            assert sent_value["zipcode"] == loc[0]

    @patch("main.time.sleep")
    @patch("main.create_kafka_producer")
    @patch("main.get_current_weather", return_value=None)
    @patch("main.resolve_zipcodes", return_value=[("01701", 42.3, -71.8)])
    @patch.object(main, "KAFKA_TOPIC", "test-topic")
    def test_stops_after_max_errors(self, mock_resolve, mock_weather, mock_producer_fn, mock_sleep):
        mock_producer = MagicMock()
        mock_producer_fn.return_value = mock_producer

        main.stream_weather_data()

        # Should never have sent anything
        mock_producer.send.assert_not_called()
        # Should have tried 5 times then stopped
        assert mock_weather.call_count == 5
