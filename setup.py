from setuptools import setup
from setuptools.command.install import install


class InstallCommand(install):

    def run(self):
        super().run()
        try:
            from yadet._sources import warn_if_no_source_driver

            warn_if_no_source_driver()
        except ImportError:
            pass


setup(cmdclass={"install": InstallCommand})
