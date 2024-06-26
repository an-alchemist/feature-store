import time
from typing import Dict, Any, List
from feature_repository import FeatureRepository, FeatureView
from online_store import OnlineStore
from offline_store import OfflineStore

class StreamingProcessor:
    def __init__(self, feature_repo: FeatureRepository, online_store: OnlineStore, offline_store: OfflineStore):
        self.feature_repo = feature_repo
        self.online_store = online_store
        self.offline_store = offline_store

    def process_event(self, event: Dict[str, Any]):
        """Process a single event and update features."""
        for feature_view_name in self.feature_repo.list_feature_views():
            feature_view = self.feature_repo.get_feature_view(feature_view_name)
            if self._event_applies_to_feature_view(event, feature_view):
                self._update_features(event, feature_view)

    def _event_applies_to_feature_view(self, event: Dict[str, Any], feature_view: FeatureView) -> bool:
        """Check if the event is relevant to the given feature view."""
        return all(entity in event for entity in feature_view.entities)

    def _update_features(self, event: Dict[str, Any], feature_view: FeatureView):
        """Update features based on the event."""
        # Compute new feature values
        new_features = self._compute_features(event, feature_view)
        
        # Add entity values to the new_features dictionary
        for entity in feature_view.entities:
            new_features[entity] = event[entity]
        
        # Update online store
        self.online_store.insert_data(feature_view.name, new_features)
        
        # Update offline store
        self.offline_store.insert_data(feature_view.name, new_features)

    def _compute_features(self, event: Dict[str, Any], feature_view: FeatureView) -> Dict[str, Any]:
        """Compute feature values based on the event and feature view definition."""
        computed_features = {}
        for feature in feature_view.features:
            if feature.name in event:
                computed_features[feature.name] = event[feature.name]
            else:
                # Here you would implement more complex feature computations
                # For simplicity, we're just using a placeholder value
                computed_features[feature.name] = 0
        return computed_features

    def run(self, event_stream):
        """Run the streaming processor on an event stream."""
        for event in event_stream:
            self.process_event(event)
            # In a real system, you might want to add some rate limiting or batching here
            time.sleep(0.1)  # Simulate some processing time