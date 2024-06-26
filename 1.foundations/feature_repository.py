from pydantic import BaseModel
from typing import List, Dict, Any

class Feature(BaseModel):
    name: str
    dtype: str

class FeatureView(BaseModel):
    name: str
    features: List[Feature]
    entities: List[str]
    ttl: int  # Time to live in seconds

class FeatureRepository:
    def __init__(self):
        self.feature_views: Dict[str, FeatureView] = {}

    def create_feature_view(self, feature_view: FeatureView):
        self.feature_views[feature_view.name] = feature_view

    def get_feature_view(self, name: str) -> FeatureView:
        return self.feature_views.get(name)

    def list_feature_views(self) -> List[str]:
        return list(self.feature_views.keys())