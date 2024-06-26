from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime

class Feature(BaseModel):
    name: str
    dtype: str

class FeatureView(BaseModel):
    name: str
    features: List[Feature]
    entities: List[str]
    ttl: int  # Time to live in seconds
    version: int
    created_at: datetime = None

class FeatureRepository:
    def __init__(self):
        self.feature_views: Dict[str, List[FeatureView]] = {}

    def create_feature_view(self, feature_view: FeatureView):
        name = feature_view.name
        if name not in self.feature_views:
            self.feature_views[name] = []
        
        # Set the version and creation timestamp
        feature_view.version = len(self.feature_views[name]) + 1
        feature_view.created_at = datetime.now()
        
        self.feature_views[name].append(feature_view)

    def get_feature_view(self, name: str, version: int = None) -> FeatureView:
        if name not in self.feature_views:
            return None
        
        if version is None:
            # Return the latest version if no specific version is requested
            return self.feature_views[name][-1]
        
        # Find the specific version
        for fv in reversed(self.feature_views[name]):
            if fv.version == version:
                return fv
        
        return None

    def list_feature_views(self) -> List[str]:
        return list(self.feature_views.keys())

    def get_feature_view_versions(self, name: str) -> List[int]:
        if name not in self.feature_views:
            return []
        return [fv.version for fv in self.feature_views[name]]