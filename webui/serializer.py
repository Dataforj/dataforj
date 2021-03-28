from rest_framework import serializers


class DataflowProject(object):
    def __init__(self, name, path):
        self.name = name
        self.path = path



class DataflowProjectSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=256)
    path = serializers.CharField()

    def create(self, validated_data):
        return DataflowProject(id=None, **validated_data)

    def update(self, instance, validated_data):
        for field, value in validated_data.items():
            setattr(instance, field, value)
        return instance