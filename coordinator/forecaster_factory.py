import os

def create_forecaster():
    """Create a forecaster instance dynamically based on the environment variables.
    Imports are done lazily inside each branch to prevent ModuleNotFoundError or
    startup deadlocks if unused forecaster packages are not installed.
    """
    forecaster_type = os.getenv("FORECASTER_TYPE", "chronos").lower()

    # Read optional max context length parameter
    max_context_length = os.getenv("FORECASTER_MAX_CONTEXT_LENGTH")
    if max_context_length is not None:
        max_context_length = int(max_context_length)
    downsample_rate = max(1, int(os.getenv("DOWNSAMPLE_RATE", "1")))
    print(f"USING FORECASTER: {forecaster_type}")
    if forecaster_type == "chronos":
        try:
            from coordinator.chronos_forecaster import ChronosForecaster
        except ImportError:
            from chronos_forecaster import ChronosForecaster
        return ChronosForecaster()

    elif forecaster_type == "custom_chronos":
        try:
            from coordinator.custom_chronos_forecaster import CustomChronosForecaster
        except ImportError:
            from custom_chronos_forecaster import CustomChronosForecaster
        model_path = os.getenv("CHRONOS_MODEL_PATH", "models/chronos-2")
        return CustomChronosForecaster(
            model_path=model_path,
            downsample_rate=downsample_rate,
            max_context_length=max_context_length,
        )

    elif forecaster_type in ("lstm", "gru"):
        try:
            from coordinator.rnn_forecaster import RNNForecaster
        except ImportError:
            from rnn_forecaster import RNNForecaster
        hidden_size = int(os.getenv("RNN_HIDDEN_SIZE", "32"))
        num_layers = int(os.getenv("RNN_NUM_LAYERS", "1"))
        learning_rate = float(os.getenv("RNN_LEARNING_RATE", "0.01"))
        return RNNForecaster(
            rnn_type=forecaster_type,
            hidden_size=hidden_size,
            num_layers=num_layers,
            learning_rate=learning_rate,
            max_context_length=max_context_length,
            downsample_rate=downsample_rate,
        )

    elif forecaster_type == "river":
        try:
            from coordinator.river_forecaster import RiverForecaster
        except ImportError:
            from river_forecaster import RiverForecaster
        p = int(os.getenv("RIVER_P", "1"))
        d = int(os.getenv("RIVER_D", "0"))
        q = int(os.getenv("RIVER_Q", "0"))
        m = int(os.getenv("RIVER_M", "30"))
        return RiverForecaster(p=p, d=d, q=q, m=m, downsample_rate=downsample_rate)

    else:
        raise ValueError(f"Unknown forecaster type: {forecaster_type}")