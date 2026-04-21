from tools.label_synthesis.synthesize_fragmented_labels import _load_domain_prompt_module


def test_domain_prompt_loader_supports_single_word_domains():
    mod = _load_domain_prompt_module("arrowheads")
    assert mod is not None
    assert mod.__name__.endswith(".arrowheads")


def test_domain_prompt_loader_falls_back_to_base_for_multi_segment_domains():
    mod = _load_domain_prompt_module("dimension_types_linear")
    assert mod is not None
    assert mod.__name__.endswith(".dimension_types")
