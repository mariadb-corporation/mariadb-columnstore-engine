import json
from pathlib import Path
import subprocess
from integration_tests.utils import change_directory


def cluster_exists(terraform_dir) -> bool:
    """
    Check if the cluster exists by running terraform output command.
    """
    with change_directory(terraform_dir):
        try:
            outputs = json.loads(subprocess.check_output(
                ["terraform", "output", "-json"],
                text=True,
            ))
            return bool(outputs)
        except subprocess.CalledProcessError as e:
            print(f"Error checking cluster existence: {e}")
            return False


def create_cluster(terraform_dir: Path) -> None:
    """
    Create the cluster by running terraform and ansible
    """
    with change_directory(terraform_dir):
        subprocess.check_call(["terraform", "init"])
        subprocess.check_call(["terraform", "plan"])
        subprocess.check_call(["terraform", "apply", "-auto-approve"])
        subprocess.check_call(["ansible-playbook", "provision.yml"])

        # Run our own provisioning playbook to prepare the cluster for the tests
        our_playbook = Path(__file__).parent / "integration_tests.yml"
        subprocess.check_call(["ansible-playbook", str(our_playbook.absolute())])


def destroy_cluster(terraform_dir: Path) -> None:
    assert cluster_exists(terraform_dir), "Cluster does not exist"
    with change_directory(terraform_dir):
        subprocess.check_call(["terraform", "destroy", "-auto-approve"])
