from agentmart_ecosystem import OpenRouterHermesClient

def test_dry_run_complete_with_metrics_shape():
    c = OpenRouterHermesClient(dry_run=True)
    text, metrics = c.complete_with_metrics("shopping_agent", "sys", "find earbuds")
    assert text.startswith("[dry-run:shopping_agent]")
    assert set(metrics) == {"prompt_tokens", "completion_tokens", "total_tokens", "elapsed_ms"}
    assert metrics["elapsed_ms"] >= 0
    assert metrics["total_tokens"] == 0  # dry-run has no real usage
