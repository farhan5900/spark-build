# Env:
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   S3_BUCKET
#   S3_PREFIX

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import re
import os
import subprocess
import shakedown

def upload_jar(jar):
    conn = S3Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
    bucket = conn.get_bucket(os.environ['S3_BUCKET'])

    key = Key(bucket, 'S3_PREFIX')
    key.metadata = {'Content-Type': 'application/java-archive'}
    key.set_contents_from_filename(jar)
    key.make_public()

    basename = os.path.basename(jar)

    jar_url = "http://{0}.s3.amazonaws.com/{1}{2}".format(
        os.environ['S3_BUCKET'],
        os.environ['S3_PREFIX'],
        basename)

    return jar_url


def submit_job(jar_url):
    spark_job_runner_args = 'http://leader.mesos:5050 dcos \\"*\\" spark:only 2'
    submit_args = "-Dspark.driver.memory=2g --class com.typesafe.spark.test.mesos.framework.runners.SparkJobRunner {0} {1}".format(
        jar_url, spark_job_runner_args)
    cmd = 'dcos --log-level=DEBUG spark --verbose run --submit-args="{0}"'.format(submit_args)
    print('Running {}'.format(cmd))
    stdout = subprocess.check_output(cmd, shell=True).decode('utf-8')
    print(stdout)

    regex = r"Submission id: (\S+)"
    match = re.search(regex, stdout)
    return match.group(1)

def task_log(task_id):
    cmd = "dcos task log --completed --lines=1000 {}".format(task_id)
    print('Running {}'.format(cmd))
    stdout = subprocess.check_output(cmd, shell=True).decode('utf-8')
    return stdout


def main():
    jar_url = upload_jar(os.getenv('TEST_JAR_PATH'))
    task_id = submit_job(jar_url)
    print('Waiting for task id={} to complete'.format(task_id))
    shakedown.wait_for_task_completion(task_id)
    log = task_log(task_id)
    print(log)
    assert "All tests passed" in log

main()
