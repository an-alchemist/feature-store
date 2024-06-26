from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any
from feature_repository import FeatureRepository
from online_store import OnlineStore
from monitoring import log_feature_retrieval

app = FastAPI()

class FeatureRequest(BaseModel):
    feature_view: str
    entity_column: str
    entity_value: Any

class FeatureResponse(BaseModel):
    features: Dict[str, Any]

class FeatureStoreService:
    def __init__(self, feature_repo: FeatureRepository, online_store: OnlineStore):
        self.feature_repo = feature_repo
        self.online_store = online_store

    async def get_online_features(self, request: FeatureRequest) -> FeatureResponse:
        feature_view = self.feature_repo.get_feature_view(request.feature_view)
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
        return FeatureResponse(features=features)

def get_feature_store_service():
    # This will be initialized in main.py and passed here
    return app.state.feature_store_service

@app.post("/get_online_features", response_model=FeatureResponse)
async def get_online_features(
    request: FeatureRequest,
    feature_store_service: FeatureStoreService = Depends(get_feature_store_service)
):
    return await feature_store_service.get_online_features(request)