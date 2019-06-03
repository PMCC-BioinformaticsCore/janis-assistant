import argparse
from dotenv import load_dotenv

# from shepherd.management.configmanager import ConfigManager

load_dotenv()

from shepherd.engines import Cromwell, CWLTool, CromwellConfiguration
from shepherd.management.taskmanager import TaskManager, TaskStatus
from shepherd.environments.environment import Environment
from shepherd.validation import ValidationRequirements

__version__ = "v0.0.1"