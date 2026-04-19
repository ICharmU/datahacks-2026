from rest_framework import serializers
from .models import UserAlert


class UserAlertSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserAlert
        fields = [
            "id",
            "user",
            "wrapper",
            "location_type",
            "location_id",
            "min_risk_score",
            "is_active",
            "created_at",
        ]
        read_only_fields = ["id", "created_at"]