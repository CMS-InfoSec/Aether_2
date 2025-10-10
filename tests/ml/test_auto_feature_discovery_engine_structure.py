import inspect

from auto_feature_discovery import FeatureDiscoveryEngine


def test_feature_discovery_engine_exposes_production_methods():
    required_methods = {
        "_load_market_data",
        "_generate_candidates",
        "_compute_rsi",
        "_compute_cross_correlation",
        "_derive_target",
        "_score_candidates",
        "_time_series_splits",
        "_sharpe_ratio",
        "_persist_candidates",
        "_promote_features",
        "_log_promotions",
    }

    engine_members = FeatureDiscoveryEngine.__dict__
    missing = [name for name in required_methods if name not in engine_members]
    assert not missing, f"FeatureDiscoveryEngine is missing methods: {missing}"

    for name in required_methods:
        member = engine_members[name]
        assert inspect.isfunction(member), f"{name} should be a function, found {type(member)!r}"
        assert member.__qualname__.startswith("FeatureDiscoveryEngine."), (
            f"{name} should be bound to FeatureDiscoveryEngine, got {member.__qualname__!r}"
        )
