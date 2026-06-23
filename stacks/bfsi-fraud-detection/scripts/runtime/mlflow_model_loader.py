"""
MLflow Model Loader and Drift Detection Module

For demonstration purposes only.

This module provides:
1. Model loading from MLflow Registry (replacing pkl files)
2. Inference performance tracking
3. Model drift detection and alerting

Used by all inference services: Kafka, PGAA, ClickHouse, RisingWave
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import deque

import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "fraud-detection-model")
MODEL_STAGE = os.getenv("MLFLOW_MODEL_STAGE", "Production")
DRIFT_WINDOW_SIZE = int(os.getenv("DRIFT_WINDOW_SIZE", "1000"))
DRIFT_ALERT_THRESHOLD = float(os.getenv("DRIFT_ALERT_THRESHOLD", "0.1"))


@dataclass
class InferenceMetrics:
    """Tracks inference metrics for drift detection."""
    predictions: deque = field(default_factory=lambda: deque(maxlen=DRIFT_WINDOW_SIZE))
    probabilities: deque = field(default_factory=lambda: deque(maxlen=DRIFT_WINDOW_SIZE))
    latencies_ms: deque = field(default_factory=lambda: deque(maxlen=DRIFT_WINDOW_SIZE))
    actual_labels: deque = field(default_factory=lambda: deque(maxlen=DRIFT_WINDOW_SIZE))
    total_predictions: int = 0
    total_frauds_predicted: int = 0
    start_time: datetime = field(default_factory=datetime.now)

    def add_prediction(self, prediction: int, probability: float, latency_ms: float, actual: Optional[int] = None):
        self.predictions.append(prediction)
        self.probabilities.append(probability)
        self.latencies_ms.append(latency_ms)
        self.total_predictions += 1
        if prediction == 1:
            self.total_frauds_predicted += 1
        if actual is not None:
            self.actual_labels.append(actual)

    def get_current_metrics(self) -> Dict[str, float]:
        """Calculate current inference metrics."""
        if len(self.predictions) == 0:
            return {}

        predictions = np.array(self.predictions)
        probabilities = np.array(self.probabilities)

        metrics = {
            "inference_fraud_rate": predictions.mean(),
            "avg_fraud_probability": probabilities.mean(),
            "avg_latency_ms": np.mean(self.latencies_ms),
            "p95_latency_ms": np.percentile(self.latencies_ms, 95) if len(self.latencies_ms) > 10 else 0,
            "total_predictions": self.total_predictions,
            "window_size": len(self.predictions)
        }

        # If we have actual labels, calculate accuracy metrics
        if len(self.actual_labels) > 0:
            actuals = np.array(self.actual_labels)
            preds = np.array(list(self.predictions)[-len(actuals):])

            if len(actuals) == len(preds):
                tp = ((preds == 1) & (actuals == 1)).sum()
                fp = ((preds == 1) & (actuals == 0)).sum()
                tn = ((preds == 0) & (actuals == 0)).sum()
                fn = ((preds == 0) & (actuals == 1)).sum()

                metrics["accuracy"] = (tp + tn) / max(len(actuals), 1)
                metrics["precision"] = tp / max(tp + fp, 1)
                metrics["recall"] = tp / max(tp + fn, 1)
                metrics["f1"] = 2 * metrics["precision"] * metrics["recall"] / max(metrics["precision"] + metrics["recall"], 1e-6)

        return metrics


class MLflowModelLoader:
    """
    Loads models from MLflow Registry and provides drift detection.

    Usage:
        loader = MLflowModelLoader()
        model = loader.load_model()

        # Make predictions
        prediction, probability = loader.predict(features)

        # Check for drift
        drift_status = loader.check_drift()
    """

    def __init__(
        self,
        tracking_uri: str = MLFLOW_TRACKING_URI,
        model_name: str = MODEL_NAME,
        model_stage: str = MODEL_STAGE,
        fallback_pkl_path: Optional[str] = None
    ):
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.model_stage = model_stage
        self.fallback_pkl_path = fallback_pkl_path

        self.model = None
        self.model_version = None
        self.model_source = None
        self.training_metrics = {}
        self.feature_columns = []
        self.inference_metrics = InferenceMetrics()

        self._mlflow_available = False
        self._load_config()

    def _load_config(self):
        """Load inference configuration if available."""
        config_path = "/models/inference_config.json"
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                self.training_metrics = config.get("training_metrics", {})
                self.feature_columns = config.get("feature_columns", [])
                logger.info(f"Loaded inference config: {len(self.feature_columns)} features")
            except Exception as e:
                logger.warning(f"Could not load inference config: {e}")

    def load_model(self) -> Any:
        """
        Load model from MLflow Registry, with fallback to pkl file.

        Returns:
            Loaded model (XGBoost classifier)
        """
        # Try MLflow Registry first
        try:
            import mlflow
            mlflow.set_tracking_uri(self.tracking_uri)

            # Try loading from Production stage
            model_uri = f"models:/{self.model_name}/{self.model_stage}"
            logger.info(f"Loading model from MLflow: {model_uri}")

            self.model = mlflow.xgboost.load_model(model_uri)
            self.model_source = "mlflow_registry"
            self._mlflow_available = True

            # Get model version info
            client = mlflow.tracking.MlflowClient()
            versions = client.search_model_versions(f"name='{self.model_name}'")
            for v in versions:
                if v.current_stage == self.model_stage:
                    self.model_version = v.version
                    break

            logger.info(f"Loaded model from MLflow Registry: {self.model_name} v{self.model_version}")
            return self.model

        except Exception as e:
            logger.warning(f"MLflow Registry not available: {e}")
            self._mlflow_available = False

        # Fallback to pkl file
        if self.fallback_pkl_path and os.path.exists(self.fallback_pkl_path):
            import joblib
            logger.info(f"Loading fallback model from: {self.fallback_pkl_path}")
            self.model = joblib.load(self.fallback_pkl_path)
            self.model_source = "pkl_file"
            self.model_version = "local"
            return self.model

        raise RuntimeError("No model available from MLflow Registry or fallback pkl file")

    def predict(self, features: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Make predictions and track metrics.

        Args:
            features: Feature array (n_samples, n_features)

        Returns:
            Tuple of (predictions, probabilities)
        """
        if self.model is None:
            raise RuntimeError("Model not loaded. Call load_model() first.")

        start_time = time.time()

        predictions = self.model.predict(features)
        probabilities = self.model.predict_proba(features)[:, 1]

        latency_ms = (time.time() - start_time) * 1000

        # Track metrics for each prediction
        for pred, prob in zip(predictions, probabilities):
            self.inference_metrics.add_prediction(
                prediction=int(pred),
                probability=float(prob),
                latency_ms=latency_ms / len(predictions)
            )

        return predictions, probabilities

    def check_drift(self) -> Dict[str, Any]:
        """
        Check for model drift by comparing inference metrics to training metrics.

        Returns:
            Dictionary with drift status and details
        """
        current_metrics = self.inference_metrics.get_current_metrics()

        if not current_metrics or not self.training_metrics:
            return {
                "status": "insufficient_data",
                "message": "Not enough data for drift detection",
                "current_metrics": current_metrics,
                "training_metrics": self.training_metrics
            }

        drift_detected = False
        drift_details = []

        # Check fraud rate drift
        training_fraud_rate = self.training_metrics.get("fraud_rate", 0)
        inference_fraud_rate = current_metrics.get("inference_fraud_rate", 0)

        if training_fraud_rate > 0:
            fraud_rate_change = abs(inference_fraud_rate - training_fraud_rate) / training_fraud_rate
            if fraud_rate_change > DRIFT_ALERT_THRESHOLD:
                drift_detected = True
                drift_details.append({
                    "metric": "fraud_rate",
                    "training": training_fraud_rate,
                    "inference": inference_fraud_rate,
                    "change_pct": fraud_rate_change * 100
                })

        # Check accuracy drift (if we have actual labels)
        if "f1" in current_metrics and "f1" in self.training_metrics:
            training_f1 = self.training_metrics["f1"]
            inference_f1 = current_metrics["f1"]

            f1_change = abs(inference_f1 - training_f1) / max(training_f1, 0.01)
            if f1_change > DRIFT_ALERT_THRESHOLD:
                drift_detected = True
                drift_details.append({
                    "metric": "f1_score",
                    "training": training_f1,
                    "inference": inference_f1,
                    "change_pct": f1_change * 100
                })

        return {
            "status": "drift_detected" if drift_detected else "healthy",
            "drift_detected": drift_detected,
            "drift_threshold": DRIFT_ALERT_THRESHOLD,
            "drift_details": drift_details,
            "current_metrics": current_metrics,
            "training_metrics": self.training_metrics,
            "model_source": self.model_source,
            "model_version": self.model_version,
            "checked_at": datetime.now().isoformat()
        }

    def log_to_mlflow(self, metrics: Dict[str, float], step: Optional[int] = None):
        """Log inference metrics to MLflow for dashboard tracking."""
        if not self._mlflow_available:
            return

        try:
            import mlflow

            # Log metrics to a dedicated inference tracking run
            with mlflow.start_run(run_name=f"inference_{self.model_source}", nested=True):
                mlflow.log_metrics(metrics, step=step)
                mlflow.set_tags({
                    "type": "inference_tracking",
                    "model_version": str(self.model_version),
                    "model_source": self.model_source
                })
        except Exception as e:
            logger.debug(f"Could not log to MLflow: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get current loader and model status."""
        return {
            "model_loaded": self.model is not None,
            "model_source": self.model_source,
            "model_version": self.model_version,
            "model_name": self.model_name,
            "mlflow_available": self._mlflow_available,
            "tracking_uri": self.tracking_uri,
            "total_predictions": self.inference_metrics.total_predictions,
            "total_frauds_predicted": self.inference_metrics.total_frauds_predicted,
            "uptime_seconds": (datetime.now() - self.inference_metrics.start_time).total_seconds()
        }


# Convenience function for simple usage
def load_fraud_model(fallback_pkl: Optional[str] = None) -> Tuple[Any, MLflowModelLoader]:
    """
    Load the fraud detection model.

    Args:
        fallback_pkl: Path to fallback pkl file if MLflow unavailable

    Returns:
        Tuple of (model, loader)
    """
    loader = MLflowModelLoader(fallback_pkl_path=fallback_pkl)
    model = loader.load_model()
    return model, loader


if __name__ == "__main__":
    # Test the loader
    print("Testing MLflow Model Loader...")

    try:
        model, loader = load_fraud_model(fallback_pkl="/models/fraud_model_pgaa.pkl")
        print(f"Model loaded: {loader.get_status()}")

        # Test prediction
        test_features = np.random.randn(5, 12)
        predictions, probabilities = loader.predict(test_features)
        print(f"Test predictions: {predictions}")
        print(f"Test probabilities: {probabilities}")

        # Check drift
        drift_status = loader.check_drift()
        print(f"Drift status: {drift_status['status']}")

    except Exception as e:
        print(f"Error: {e}")
