from rest_framework import serializers


class HealthSerializer(serializers.Serializer):
    ok = serializers.BooleanField()
    service = serializers.CharField()
    env = serializers.CharField()


class WrapperSerializer(serializers.Serializer):
    key = serializers.CharField()
    label = serializers.CharField()
    description = serializers.CharField()