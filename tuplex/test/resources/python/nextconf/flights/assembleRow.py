def assemble_row(row):
    x = row['features']

    return {"f_quarter": x[0],
            "f_month": x[1],
            "f_day_of_month": x[2],
            "f_day_of_week": x[3],
            "f_carrier": x[4],
            "f_origin_state": x[5],
            "f_dest_state": x[6],
            "f_dep_delay": x[7],
            "f_arr_delay": x[8],
            "f_crs_arr_hour": x[9],
            "f_crs_dep_hour": x[10],
            "f_crs_arr_5min": x[11],
            "f_crs_dep_5min": x[12],
            "t_carrier_delay": row['CARRIER_DELAY'],
            "t_weather_delay": row['WEATHER_DELAY'],
            "t_nas_delay": row['NAS_DELAY'],
            "t_security_delay": row['SECURITY_DELAY'],
            "t_late_aircraft_delay": row['LATE_AIRCRAFT_DELAY']}