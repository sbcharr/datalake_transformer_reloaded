import logging as log
from commons import commons as c
from datetime import datetime
from aws import s3utils
# import threading
import time
import sys, os, shutil
import multiprocessing as mp


s3 = s3utils.AwsS3Service()

# class DownloadFileFromS3(threading.Thread):
#     def __init__(self, file):
#         threading.Thread.__init__(self)
#         self.file = file
#
#     def run(self):
#         download_file_from_s3(self.file)
#
#
# def download_file_from_s3(file):
#     print("start processing for file", file)
#     s3_service_instance = s3utils.AwsS3Service()
#     s3_service_instance.download_file(file)
#     print("processing over for file", file)


# def main():
#     # TODO: input validation
#     c.setup()
#     t1 = datetime.now()
#     # s3_service_instance = s3_service.AwsS3Service()
#     # key_and_filename = ["trip data/yellow_tripdata_2019-01.csv|/home/ubuntu/data/yellow_tripdata_2019-01.csv",
#     #                    "trip data/yellow_tripdata_2019-02.csv|/home/ubuntu/data/yellow_tripdata_2019-02.csv"]
#     key_and_filename = ["tables/delta01/part-00000-d8a9092f-4f12-440f-bb2c-c947c50d1f0f-c000.snappy.parquet|/tmp/part-00000-d8a9092f-4f12-440f-bb2c-c947c50d1f0f-c000.snappy.parquet",
#                         "tables/delta01/part-00000-816f0829-2c20-472d-bc53-236e4c3719ad-c000.snappy.parquet|/tmp/part-00000-816f0829-2c20-472d-bc53-236e4c3719ad-c000.snappy.parquet"]
#     # filename = "/tmp/yellow_tripdata_2019-01.csv"
#     # with ThreadPool(2) as p: # multiprocessing.cpu_count()
#     #     p.map(s3_service_instance.download_file, key_and_filename)
#     # p.close()
#     # p.join()
#
#     for f in key_and_filename:
#         thread = DownloadFileFromS3(f)
#         thread.start()
#
#     while threading.active_count() > 1:
#         print(threading.active_count())
#         time.sleep(20)
#         continue
#
#     # print("cpu count: {}".format(mp.cpu_count()))
#     # #with mp.Pool(2) as pool:
#     # print("debug1")
#     # pool = mp.Pool(mp.cpu_count())
#     # _ = pool.map(download_file_from_s3, key_and_filename)
#     # print("debug2")
#     # pool.join()
#     # pool.close()
#     # print("debug3")
#     t2 = datetime.now()
#     print("total time taken: {} seconds".format((t2-t1).seconds))


def check_done_file(table, run_date):
    done_file = "mint_" + table + "_" + run_date + ".done"
    prefix = table + '/' + run_date
    max_retries_done_file = 5
    sleep_time = 5
    # print(c.SOURCE_BUCKET_NAME, prefix, done_file)

    for i in range(0, max_retries_done_file):
        if bool(s3.load_object(c.SOURCE_BUCKET_NAME, prefix, done_file)):

            log.info("'done' file {} available for table {} at {}".format(done_file, table, time.ctime()))
            return 1
        else:
            log.info("'done' file {} for table {} is not available in S3 at {}".format(done_file, table, time.ctime()))
            time.sleep(sleep_time)

    return 0


def get_matching_s3_keys(bucket, prefix=None, suffix=None):
    keys = []
    for obj_key in s3.get_matching_objects(bucket, prefix, suffix):
        keys.append(obj_key.strip())

    return keys


def download_file_from_s3(w):
    # print("debug3")
    # print(w)
    # sys.exit(0)
    time.sleep(1)
    s3.download_file(w[0], w[1], w[2])


def validate_keys_after_download(path, suffix):
    keys = [file for file in os.listdir(path) if os.isfile(os.path.join(path, file)) and
            os.isfile(os.path.join(path, file)).endswith(suffix)]
    return keys


def transform(table, run_date, put_date):
    print(put_date)
    done_file_check = check_done_file(table, run_date)

    if not bool(done_file_check):
        log.error("'done', file for table {} not available, exiting".format(table))
        sys.exit(1)

    prefix = table + '/' + run_date
    suffix = '.csv'
    data_path = c.SOURCE_BUCKET_NAME + '/' + prefix

    keys_gpg = get_matching_s3_keys(c.SOURCE_BUCKET_NAME, prefix, suffix)
    if len(keys_gpg) == 0:
        log.error("no files in source s3 path {}".format(data_path))
        sys.exit(-1)

    work = []
    bucket = c.SOURCE_BUCKET_NAME
    base_dir = "/home/ubuntu/data"
    if not os.path.isdir(base_dir):
        log.error("path to local directory {} doesn't exist, exiting...".format(base_dir))

    if os.path.isdir(base_dir + '/' + prefix):
        os.chdir(base_dir)
        try:
            shutil.rmtree(prefix)
        except OSError:
            print("Deletion of the directory {} failed".format(prefix))
        else:
            print("Successfully deleted the directory {}".format(prefix))

    os.chdir(base_dir)
    try:
        os.makedirs(prefix)
    except OSError:
        print("Creation of the directory {} failed".format(prefix))
    else:
        print("Successfully created the directory {}".format(prefix))

    for key in keys_gpg:
        # print("key: ", key)
        w = [bucket, key, base_dir]
        # print(type(w))
        work.append(w)
    # print(work)

    # download files from S3
    with mp.Pool(mp.cpu_count()) as pool:
        # pool = mp.Pool(mp.cpu_count())
        _ = pool.map(download_file_from_s3, work)
        # time.sleep(5)
        pool.close()
        pool.join()
    # log.info("downloaded files from s3 file path {}".format(data_path))
    downloaded_keys = validate_keys_after_download(base_dir + '/' + prefix, suffix)
    if len(downloaded_keys) != len(keys_gpg):
        log.error("number of files downloaded is {} and doesn't match with the actual file list of {}, exiting..."
                  .format(downloaded_keys, keys_gpg))
        sys.exit(-1)


def main():
    c.setup()
    t1 = datetime.now()
    # aws_s3_service = s3utils.AwsS3Service()
    tables = c.get_list_of_tables()
    # print(tables)
    run_date = c.get_run_date()
    put_date = c.get_put_date()

    for table in tables:
        transform(table, run_date, put_date)
        log.info("transformation completed for table {}".format(table))
    t2 = datetime.now()
    print("total time taken: {} seconds".format((t2 - t1).seconds))


if __name__ == "__main__":
    main()
