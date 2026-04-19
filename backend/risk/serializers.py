from rest_framework import serializers


class FactorContributionSerializer(serializers.Serializer):
    name = serializers.CharField()
    direction = serializers.ChoiceField(choices=["up", "down"])
    magnitude = serializers.FloatField()


class ForecastPointSerializer(serializers.Serializer):
    t = serializers.DateTimeField()
    risk_score = serializers.FloatField()


class MapRiskPointSerializer(serializers.Serializer):
    location_id = serializers.CharField()
    name = serializers.CharField()
    lat = serializers.FloatField()
    lon = serializers.FloatField()
    risk_score = serializers.FloatField()
    risk_bucket = serializers.CharField()
    uncertainty_score = serializers.FloatField(required=False, allow_null=True)


class MapRiskResponseSerializer(serializers.Serializer):
    generated_at_utc = serializers.DateTimeField()
    wrapper = serializers.CharField()
    horizon = serializers.CharField()
    locations = MapRiskPointSerializer(many=True)


class BeachRiskDetailSerializer(serializers.Serializer):
    location_id = serializers.CharField()
    name = serializers.CharField()
    wrapper = serializers.CharField()
    forecast = ForecastPointSerializer(many=True)
    risk_bucket = serializers.CharField()
    uncertainty_score = serializers.FloatField(required=False, allow_null=True)
    top_factors = FactorContributionSerializer(many=True)
    recommended_action = serializers.CharField()
    evidence = serializers.ListField(child=serializers.CharField())


class RiskQueryRequestSerializer(serializers.Serializer):
    wrapper = serializers.ChoiceField(choices=["beach", "fishing", "surf", "ecosystem"])
    location_id = serializers.CharField()
    horizon = serializers.CharField(required=False, default="24h")