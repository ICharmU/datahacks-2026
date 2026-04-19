from django.contrib.auth import get_user_model
from django.db import models

User = get_user_model()


class UserAlert(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    wrapper = models.CharField(max_length=20)
    location_type = models.CharField(max_length=50)
    location_id = models.CharField(max_length=100)
    min_risk_score = models.FloatField(default=0.7)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)