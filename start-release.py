from subprocess import run
import os
from configparser import ConfigParser

CONFIG_PATH = os.path.join(os.getcwd(), "app", "component", "config.ini")

CONFIG = ConfigParser()
CONFIG.optionxform = str
CONFIG.read(CONFIG_PATH)
CONFIG["version"]["REVISION"] = str(int(CONFIG["version"]["REVISION"]) + 1)

VERSION = ''.join([
    CONFIG["version"]["MAJOR"],
    ".",
    CONFIG["version"]["MINOR"],
    ".",
    CONFIG["version"]["REVISION"]
])


run(
    "git flow release start %s" % VERSION,
    shell=True,
    check=True
)


with open(CONFIG_PATH, 'w') as configfile:
    CONFIG.write(configfile)

print(VERSION)
