# # import logging as log
# from commons import commons as c
# from datetime import datetime
# from aws import s3utils
# import multiprocessing as mp
#
#
# def download_file_from_s3(file):
#     print("start processing for file", file)
#     s3_service_instance = s3utils.AwsS3Service()
#     s3_service_instance.download_file(file)
#     print("processing over for file", file)
#
#
# def main():
#     # TODO: input validation
#     c.setup()
#     t1 = datetime.now()
#     # s3_service_instance = s3_service.AwsS3Service()
#     # key_and_filename = ["trip data/yellow_tripdata_2019-01.csv|/home/ubuntu/data/yellow_tripdata_2019-01.csv",
#     #                    "trip data/yellow_tripdata_2019-02.csv|/home/ubuntu/data/yellow_tripdata_2019-02.csv"]
#     key_and_filename = ["tables/delta01/part-00000-d8a9092f-4f12-440f-bb2c-c947c50d1f0f-c000.snappy.parquet|/home/ubuntu/data/part-00000-d8a9092f-4f12-440f-bb2c-c947c50d1f0f-c000.snappy.parquet",
#                         "tables/delta01/part-00000-816f0829-2c20-472d-bc53-236e4c3719ad-c000.snappy.parquet|/home/ubuntu/data/part-00000-816f0829-2c20-472d-bc53-236e4c3719ad-c000.snappy.parquet"]
#     # filename = "/tmp/yellow_tripdata_2019-01.csv"
#     # with ThreadPool(2) as p: # multiprocessing.cpu_count()
#     #     p.map(s3_service_instance.download_file, key_and_filename)
#     # p.close()
#     # p.join()
#
#     print("cpu count: {}".format(mp.cpu_count()))
#     #with mp.Pool(2) as pool:
#     print("debug1")
#     pool = mp.Pool(mp.cpu_count())
#     _ = pool.map(download_file_from_s3, key_and_filename)
#     print("debug2")
#     pool.join()
#     pool.close()
#     print("debug3")
#     t2 = datetime.now()
#     print("total time taken: {} seconds".format((t2-t1).seconds))
#
#
# if __name__ == "__main__":
#     main()
