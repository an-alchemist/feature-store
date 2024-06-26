import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_feature_retrieval(feature_view: str, entity_value: Any, features: Dict[str, Any]):
    logger.info(f"Retrieved features for {feature_view}, entity value: {entity_value}")
    logger.info(f"Features: {features}")