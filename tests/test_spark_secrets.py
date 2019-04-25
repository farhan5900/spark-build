import logging
import pytest
import os

import sdk_cmd
import sdk_utils

import spark_utils as utils

log = logging.getLogger(__name__)

@pytest.fixture(scope='module', autouse=True)
def setup_spark(configure_security_spark, configure_universe):
    try:
        utils.upload_dcos_test_jar()
        utils.require_spark()
        yield
    finally:
        utils.teardown_spark()


@pytest.mark.sanity
def test_env_based_ref_secret():
    secret_path = "/spark/secret-name"
    secret_value = "secret-value"
    sdk_cmd.run_cli("security secrets delete " + secret_path)
    sdk_cmd.run_cli("security secrets create -v " + secret_value + " " + secret_path)
    try:
        utils.run_tests(
            app_url=utils.dcos_test_jar_url(),
            app_args="",
            expected_output=secret_value,
            args=["--conf=spark.mesos.driver.secret.names='"+secret_path+"'",
                  "--conf=spark.mesos.driver.secret.envkeys='SECRET_ENV_KEY'",
                  "--class SecretConfs"])
    finally:
        sdk_cmd.run_cli("security secrets delete " + secret_path)


@pytest.mark.sanity
def test_value_secret():
    secret_value = "secret-value"
    utils.run_tests(
        app_url=utils.dcos_test_jar_url(),
        app_args="",
        expected_output=secret_value,
        args=["--conf=spark.mesos.driver.secret.values='"+secret_value+"'",
              "--conf=spark.mesos.driver.secret.envkeys='SECRET_ENV_KEY'",
              "--class SecretConfs"])


@pytest.mark.sanity
def test_file_based_ref_secret():
    secret_path = "/spark/secret-name"
    secret_file_name = "secret.file"
    secret_value = "secret-value"
    with open(secret_file_name, 'w') as secret_file:
        secret_file.write(secret_value)
    sdk_cmd.run_cli("security secrets delete " + secret_path)
    sdk_cmd.run_cli("security secrets create -f " + secret_file_name + " " + secret_path)
    try:
        utils.run_tests(
            app_url=utils.dcos_test_jar_url(),
            app_args="",
            expected_output=secret_value,
            args=["--conf=spark.mesos.driver.secret.names='"+secret_path+"'",
                  "--conf=spark.mesos.driver.secret.filenames='"+secret_file_name+"'",
                  "--class SecretConfs"])
    finally:
        sdk_cmd.run_cli("security secrets delete " + secret_path)
        if os.path.exists(secret_file_name):
            os.remove(secret_file_name)
