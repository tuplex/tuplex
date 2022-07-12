#!/usr/bin/env python3
# (c) L.Spiegelberg 2020
# use this script to extract downloaded flight data in data/ folder and rename it for the benchmarks

import os
import glob
import zipfile

zipfiles = sorted(glob.glob('./data/*.zip'))[::-1]

for path in zipfiles:

    year = os.path.basename(path)
    year = int(year[:year.find('.')])
    month = os.path.basename(path)
    month = month[month.find('.')+1:]
    month = int(month[:month.find('.')])

    zf = zipfile.ZipFile(path)

    assert len(zf.infolist()) == 1, 'zipfile is supposed to contain only one entry'
    zf_filename = zf.infolist()[0].filename

    zf.extract(zf.infolist()[0])

    os.rename(zf_filename, 'data/flights_on_time_performance_{}_{:02d}.csv'.format(year, month))
    print('extracted {:02d}/{}'.format(month, year))
print('done.')
