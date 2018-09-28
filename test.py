import json
import time
import datetime

date_st = datetime.date(2018,  9,  1)
date_ed = datetime.date(2018,  9, 11)


if __name__ == '__main__':

    sc = SparkContext(appName="Courtesy costs nothing.")
    sqlContext = HiveContext(sc)

    city_code = '1'
    date_i    = date_st
    while date_i < date_ed:

        order_info_name_lst = ["order_id",
                               "strive_time", "arrive_time",
                               "est_pick_dur", "eta_second_pick_dur"]
        sql_order = ("select %s from gulfstream_dwd.dwd_order_make_d "
                     "where concat(year,month,day)=%s "
                     "and product_category in (3,7,20,99,314) "
                     "and driver_id>0 "
                     "and is_td_arrive=1 "
                     "and type=0") % (", ".join(order_info_name_lst),
                                      date_i.strftime("%Y%m%d"))

        print sql_order
        order_rdd = sqlContext.sql(sql_order).repartition(200).rdd

        # order_rdd = order_rdd.map( lambda row: ",".join([str(row.order_id), str(row.est_pick_dur), str(row.eta_second_pick_dur)]) )
        #                      .filter( lambda row: row[3] != "0000-00-00 00:00:00" and row[4] != "0000-00-00 00:00:00" ) \
        order_rdd = order_rdd.map( lambda row: (str(row.order_id), int(row.est_pick_dur), int(row.eta_second_pick_dur), row.strive_time, row.arrive_time) ) \
                             .filter( lambda row: row[1] != -9999 and row[2] != -9999 and row[3] != "0000-00-00 00:00:00" and row[4] != "0000-00-00 00:00:00" ) \
                             .map( lambda row: (row[0], row[1], row[2],
                                                int(time.mktime(datetime.datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S").timetuple())),
                                                int(time.mktime(datetime.datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S").timetuple()))) ) \
                             .filter( lambda row: row[4] >= row[3] )

        order_rdd.cache()


        eta_error0 = order_rdd.map( lambda row: abs(row[4] - row[3] - row[1] * 60.0) / (row[4] - row[3]) ).sum()
        eta_error1 = order_rdd.map( lambda row: abs(row[4] - row[3] - row[2] * 1.0) / (row[4] - row[3]) ).sum()
        order_count = order_rdd.count()
        print "{}: {:>16.3f} {:>10.5f} {:>16.3f} {:>10.5f} {:>10d}".format(date_i.strftime("%Y-%m-%d"),
                                                                    eta_error0,
                                                                    eta_error0 / order_count,
                                                                    eta_error1,
                                                                    eta_error1 / order_count,
                                                                    order_count)

        order_rdd.unpersist()

        date_i += datetime.timedelta(1)

    sc.stop()
