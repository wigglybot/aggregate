from subprocess import run
import os
from configparser import ConfigParser
import datetime

the_date = datetime.datetime.now()
CONFIG_PATH = os.path.join(os.getcwd(), "app", "component", "config.ini")

CONFIG = ConfigParser()
CONFIG.optionxform = str
CONFIG.read(CONFIG_PATH)
CONFIG["version"]["REVISION"] = str(int(CONFIG["version"]["REVISION"]) + 1)
CONFIG["version"]["DATE"] = "%s-%s-%s" % (the_date.year, str(the_date.month).rjust(2, "0"), str(the_date.day).rjust(2, "0"))

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
