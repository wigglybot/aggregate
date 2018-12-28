from subprocess import run
import os

run("cd %s && pipenv run pipenv_to_requirements -f" % os.path.join(os.getcwd(), "app"), shell=True, check=True)
