#XingSzen

class MongoDBQuerier:
    # Initializes with the database connection
    def __init__(self, db_connector):
        self.db_connector = db_connector

    # Complex Query 1: Aggregation Pipeline for Repeat Offenders
    # Instead of just finding single records, this groups records by sender account
    # to find accounts that have triggered multiple alerts and sums their total suspicious amounts.
    def get_aggregated_repeat_offenders(self, collection_name, min_total_amount=5000):
        collection = self.db_connector.get_collection(collection_name)
        
        pipeline = [
            # Stage 1: Group by the sender account to aggregate their activity
            {
                "$group": {
                    "_id": "$sender_account",
                    "total_suspicious_amount": {"$sum": {"$toDouble": "$amount"}},
                    "incident_count": {"$sum": 1},
                    "avg_velocity_score": {"$avg": {"$toDouble": "$velocity_score"}},
                    "max_geo_anomaly": {"$max": {"$toDouble": "$geo_anomaly_score"}}
                }
            },
            # Stage 2: Filter out accounts that haven't reached the monetary threshold
            {
                "$match": {
                    "total_suspicious_amount": {"$gte": min_total_amount}
                }
            },
            # Stage 3: Sort by the highest total suspicious amount descending
            {
                "$sort": {"total_suspicious_amount": -1}
            },
            # Stage 4: Rename the _id field to sender_account for better readability in the output
            {
                "$project": {
                    "_id": 0,
                    "sender_account": "$_id",
                    "total_suspicious_amount": 1,
                    "incident_count": 1,
                    "avg_velocity_score": 1,
                    "max_geo_anomaly": 1
                }
            }
        ]
        
        return list(collection.aggregate(pipeline))

    # Complex Query 2: Dynamic Risk Categorization
    # This evaluates the batch data, filters for statistical significance, and dynamically
    # assigns a text-based "Risk Category" based on numerical thresholds using a conditional switch.
    def get_dynamic_risk_profiles(self, collection_name, min_volume=1000):
        collection = self.db_connector.get_collection(collection_name)
        
        pipeline = [
            # Stage 1: Only analyze transaction types with enough total volume to be statistically significant
            {
                "$match": {
                    "total_count": {"$gte": min_volume}
                }
            },
            # Stage 2: Create a new calculated field called 'risk_category' using a Switch statement
            {
                "$addFields": {
                    "risk_category": {
                        "$switch": {
                            "branches": [
                                {"case": {"$gte": ["$fraud_percentage", 4.0]}, "then": "CRITICAL"},
                                {"case": {"$gte": ["$fraud_percentage", 3.6]}, "then": "HIGH"}
                            ],
                            "default": "MODERATE"
                        }
                    }
                }
            },
            # Stage 3: Filter the results to only show actionable (Critical or High) risk levels
            {
                "$match": {
                    "risk_category": {"$in": ["CRITICAL", "HIGH"]}
                }
            },
            # Stage 4: Sort the results to show the absolute worst fraud percentages first
            {
                "$sort": {"fraud_percentage": -1}
            },
            # Stage 5: Clean up the output projection
            {
                "$project": {
                    "_id": 0,
                    "transaction_type": 1,
                    "total_count": 1,
                    "fraud_percentage": 1,
                    "risk_category": 1
                }
            }
        ]
        
        return list(collection.aggregate(pipeline))

