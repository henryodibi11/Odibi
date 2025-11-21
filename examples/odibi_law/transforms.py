from odibi.registry import transform


@transform
def summarize_bill(context, current):
    """Mock LLM summarization."""
    df = current.copy()

    def summarize(text):
        if not text:
            return ""
        # Simulate API latency?
        # import time; time.sleep(0.1)
        return text.split(".")[0] + "."

    df["summary"] = df["text"].apply(summarize)
    return df
