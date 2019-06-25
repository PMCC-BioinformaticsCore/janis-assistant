import argparse
from dotenv import load_dotenv

# from shepherd.management.configmanager import ConfigManager

load_dotenv()

from shepherd.main import fromjanis
from shepherd.data.models.filescheme import FileScheme, LocalFileScheme, SSHFileScheme, GCSFileScheme, S3FileScheme
from shepherd.engines import Cromwell, CWLTool, Toil, CromwellConfiguration
from shepherd.management.taskmanager import TaskManager, TaskStatus
from shepherd.environments.environment import Environment
from shepherd.validation import ValidationRequirements

__version__ = "v0.0.2"
