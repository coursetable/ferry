import os

"""
Contains configurations and settings used by the rest of the project.
"""

_PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

DATA_DIR = os.path.join(_PROJECT_DIR, "api_output")
RESOURCE_DIR = os.path.join(_PROJECT_DIR, "resources")
