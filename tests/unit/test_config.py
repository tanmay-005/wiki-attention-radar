from spark.common import config


def test_settings_load():
    cfg = config.load()
    assert cfg["languages"]
    assert cfg["spikes"]["global_hourly"]["baseline_hours"] == 168


def test_dev_days():
    assert len(config.hourly_days(dev=True)) == 2
    assert len(config.country_days(dev=True)) == 3


def test_full_period_lengths():
    assert len(config.hourly_days()) == 21
    assert len(config.country_days()) == 31


def test_connections_have_both_envs():
    cfg = config.load()
    assert {"in_docker", "on_host"} <= set(cfg["connections"])
