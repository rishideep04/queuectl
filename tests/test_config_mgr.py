from config_mgr import ensure_defaults,get_config,set_config,list_config

def test_ensure_defaults():
    ensure_defaults()
    configs = list_config()
    assert "max_retries" in configs
    assert "backoff_base" in configs

def test_set_and_get_config():
    set_config("max_retries","5")
    val = get_config("max_retries")
    assert val == 5

def test_default_values():
    val = get_config("nonexistent_key",default=42)
    assert val == 42
