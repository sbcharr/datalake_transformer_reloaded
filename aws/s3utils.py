import boto3
from botocore.exceptions import ClientError
import logging as log
from commons import commons as c


class AwsS3Service:
    def __init__(self):
        self.s3_resource = boto3.resource('s3', region_name=c.REGION_NAME,
                                          aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                                          aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY)

        self.s3_client = boto3.client('s3', region_name=c.REGION_NAME,
                                      aws_access_key_id=c.AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=c.AWS_SECRET_ACCESS_KEY)

    def download_file(self, bucket, key, local_file_path):
        print(bucket, key, local_file_path)
        try:
            self.s3_resource.meta.client.download_file(bucket, key, local_file_path + '/' + key)
            # self.s3_resource.Bucket(bucket).download_file(key, local_file_path + '/' + key)
        except ClientError as e:
            print("debug2")
            log.error(e.response['Error']['Message'])
            raise
        log.info("download complete for file {}".format(key))

    def upload_file(self, local_file_path, bucket, key):
        try:
            self.s3_resource.meta.client.upload_file(local_file_path + '/' + key, bucket, key)
        except ClientError as e:
            log.error(e.response['Error']['Message'])
            raise

    def get_matching_objects(self, bucket, prefix=None, suffix=None):
        # print("debug1")
        kwargs = {'Bucket': bucket, 'Prefix': prefix + '/'}

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
        except ClientError as e:
            log.error(e.response['Error']['Message'])
            raise

        for page in paginator.paginate(**kwargs):
            # print(page["Contents"])
            try:
                contents = page["Contents"]
            except ClientError as e:
                log.error(e.response['Error']['Message'])
                raise
            for obj in contents:
                key = obj['Key']
                if key.endswith(suffix):
                    yield obj['Key']

    def load_object(self, bucket, prefix, key):
        try:
            self.s3_resource.Object(bucket, prefix + '/' + key).load()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                log.error("The object {} does not exist".format(key))
                return 0    # object doesn't exist
            else:
                log.error(e.response['Error']['Message'])

        return 1    # object exists










    # def retrieve_sqs_message(self, sqs_queue_url, wait_time=0, visibility_time=0):
    #     """Retrieve messages from an SQS queue
    #     The retrieved messages are not deleted from the queue.
    #     :param sqs_queue_url: String URL of existing SQS queue
    #     :param wait_time: Number of seconds to wait if no messages in queue
    #     :param visibility_time: Number of seconds to make retrieved messages
    #         hidden from subsequent retrieval requests
    #     :return: List of retrieved messages. If no messages are available, returned
    #         list is empty. If error, returns None.
    #     """

    #     # Validate number of messages to retrieve
    #     # if num_msgs < 1:
    #     #     num_msgs = 1
    #     # elif num_msgs > 10:
    #     #     num_msgs = 10
    #     num_msgs = 1     # this is to make sure only 1 message is read per thread

    #     # Retrieve messages from an SQS queue
    #     try:
    #         msgs = self.sqs_client.receive_message(QueueUrl=sqs_queue_url,
    #                                                MaxNumberOfMessages=num_msgs,
    #                                                WaitTimeSeconds=wait_time,
    #                                                VisibilityTimeout=visibility_time)
    #     except ClientError as e:
    #         log.error(e)
    #         raise

    #     # Return the list of retrieved messages
    #     return msgs['Messages']

    # def delete_fifo_queue(self, queue_url):
    #     try:
    #         _ = self.sqs_client.delete_queue(
    #             QueueUrl=queue_url
    #         )
    #     except ClientError as e:
    #         log.error(e.response['Error']['Message'])
    #         return
    #     log.info("deleting queue {}, wait for 60 seconds".format(queue_url))
    #     time.sleep(61)    # as per aws queue deletion may take up to 60 secs

    # def delete_sqs_message(self, sqs_queue_url, msg_receipt_handle):
    #     """Delete a message from an SQS queue
    #     :param sqs_queue_url: String URL of existing SQS queue
    #     :param msg_receipt_handle: Receipt handle value of retrieved message
    #     """

    #     # Delete the message from the SQS queue
    #     self.sqs_client.delete_message(QueueUrl=sqs_queue_url,
    #                                    ReceiptHandle=msg_receipt_handle)

    # def get_queue_url(self, queue_name):
    #     try:
    #         queue_url = self.sqs_client.get_queue_url(
    #             QueueName=queue_name,
    #         )
    #     except ClientError as e:
    #         if e.response['Error']['Code'] == "AWS.SimpleQueueService.NonExistentQueue":
    #             log.info("fifo queue {} does not exist".format(queue_name))
    #             return
    #         else:
    #             log.error(e)
    #             raise

    #     return queue_url