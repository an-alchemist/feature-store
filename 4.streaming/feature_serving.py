from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from feature_repository import FeatureRepository
from online_store import OnlineStore
from monitoring import log_feature_retrieval

app = FastAPI()

class FeatureRequest(BaseModel):
    feature_view: str
    entity_column: str
    entity_value: Any
    version: Optional[int] = None

class FeatureResponse(BaseModel):
    features: Dict[str, Any]
    version: int

class FeatureStoreService:
    def __init__(self, feature_repo: FeatureRepository, online_store: OnlineStore):
        self.feature_repo = feature_repo
        self.online_store = online_store

    async def get_online_features(self, request: FeatureRequest) -> FeatureResponse:
        feature_view = self.feature_repo.get_feature_view(request.feature_view, request.version)
        if not feature_view:
            raise HTTPException(status_code=404, detail="Feature view not found")
        
        features = self.online_store.get_online_features(
            request.feature_view,
            request.entity_column,
            request.entity_value
        )
        
        if not features:
            raise HTTPException(status_code=404, detail="Features not found")
        
        log_feature_retrieval(request.feature_view, request.entity_value, features)
        return FeatureResponse(features=features, version=feature_view.version)

def get_feature_store_service():
    # This will be initialized in main.py and passed here
    return app.state.feature_store_service

@app.post("/get_online_features", response_model=FeatureResponse)
async def get_online_features(
    request: FeatureRequest,
    feature_store_service: FeatureStoreService = Depends(get_feature_store_service)
):
    return await feature_store_service.get_online_features(request)

@app.get("/list_feature_view_versions/{feature_view_name}")
async def list_feature_view_versions(
    feature_view_name: str,
    feature_store_service: FeatureStoreService = Depends(get_feature_store_service)
):
    versions = feature_store_service.feature_repo.get_feature_view_versions(feature_view_name)
    if not versions:
        raise HTTPException(status_code=404, detail="Feature view not found")
    return {"feature_view": feature_view_name, "versions": versions}