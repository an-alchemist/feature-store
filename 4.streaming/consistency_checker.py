import random
from offline_store import OfflineStore
from online_store import OnlineStore
from feature_repository import FeatureRepository

def check_consistency(offline_store: OfflineStore, online_store: OnlineStore, feature_repo: FeatureRepository, sample_size: int = 100):
    inconsistencies = []

    # Get all feature views
    feature_views = feature_repo.list_feature_views()

    for feature_view_name in feature_views:
        feature_view = feature_repo.get_feature_view(feature_view_name)
        
        # Sample entities from the offline store
        all_entities = offline_store.get_all_entity_ids(feature_view_name)
        sampled_entities = random.sample(all_entities, min(sample_size, len(all_entities)))

        for entity_id in sampled_entities:
            offline_features = offline_store.get_batch_features(feature_view_name, feature_view.entities[0], [entity_id])
            online_features = online_store.get_online_features(feature_view_name, feature_view.entities[0], entity_id)

            # Compare offline and online features
            for feature in feature_view.features:
                offline_value = offline_features[feature.name].iloc[0]
                online_value = online_features[feature.name]

                if not is_consistent(offline_value, online_value):
                    inconsistencies.append({
                        'feature_view': feature_view_name,
                        'entity_id': entity_id,
                        'feature': feature.name,
                        'offline_value': offline_value,
                        'online_value': online_value
                    })

    return inconsistencies

def is_consistent(offline_value, online_value, tolerance=1e-6):
    if isinstance(offline_value, (int, float)) and isinstance(online_value, (int, float)):
        return abs(offline_value - online_value) < tolerance
    else:
        return offline_value == online_value

def report_inconsistencies(inconsistencies):
    if not inconsistencies:
        print("No inconsistencies found. Offline and online stores are in sync.")
    else:
        print(f"Found {len(inconsistencies)} inconsistencies:")
        for inc in inconsistencies:
            print(f"  Feature View: {inc['feature_view']}")
            print(f"  Entity ID: {inc['entity_id']}")
            print(f"  Feature: {inc['feature']}")
            print(f"  Offline Value: {inc['offline_value']}")
            print(f"  Online Value: {inc['online_value']}")
            print()

if __name__ == "__main__":
    # Initialize your stores and feature repository here
    offline_store = OfflineStore("offline_store.db")
    online_store = OnlineStore("online_store.db")
    feature_repo = FeatureRepository()

    inconsistencies = check_consistency(offline_store, online_store, feature_repo)
    report_inconsistencies(inconsistencies)