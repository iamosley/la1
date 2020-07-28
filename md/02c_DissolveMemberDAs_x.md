## Calculate DA Membership for each LA
Principal computation is determining which DAs belong to each individual LA. The spatial operation 'within' was onerous and the calculations were split across multiple sessions. Only one session is presented here for clarity. 


```python
import pickle
import geopandas as gp
import datetime
```


```python
# reload required data
with open(r'.\Load\da_single_max.zip', 'rb') as f:
    da_single_max = pickle.load(f)
with open(r'.\Dissolve\single_member_dissolve.zip', 'rb') as f:
    single_member_dissolve = pickle.load(f)
    
```


```python
single_member_dissolve.info()
```

    <class 'geopandas.geodataframe.GeoDataFrame'>
    MultiIndex: 27222 entries, (382, 0) to (644, 9)
    Data columns (total 3 columns):
    total       27222 non-null int64
    area        27222 non-null float64
    geometry    27222 non-null geometry
    dtypes: float64(1), geometry(1), int64(1)
    memory usage: 843.5 KB
    


```python
# manually split the processing into four 'slices'. Slice '0' is presented here.
MAX = 27222
##---------xxxxxx----------------xxxxxxxxxx----------------
slices = [[0, 250], [250, 500], [500, 10000], [10000, MAX]]
SLICE = 0
```


```python
# these tuples (member_id, LA) were particularly troublesome, they were skipped and processed later
bad_cases = [(382, 211), (384, 457), (382, 219)]
```


```python
# create an R-tree spatial index for the DA boundaries
da_sindex = da_single_max.sindex
```


```python
# calculate DAs within LAs (which were buffered by one meter) using a spatial index, very helpful post:
# from https://automating-gis-processes.github.io/site/notebooks/L3/spatial_index.html
# 'itertuples()' function is a faster version of 'iterrows()'
i = 0
single_member_das = []
for single in single_member_dissolve[slices[SLICE][0]:slices[SLICE][1]].itertuples():
    if single.Index in bad_cases:
        print('Skipping')
        continue
    bounds = single.geometry.bounds
    candidates = list(da_sindex.intersection(bounds))
    possible_matches = da_single_max.iloc[candidates]
    result = possible_matches.loc[possible_matches.within(single.geometry)].index.tolist()
    single_member_das += [[single.Index, result]]
    if i % 100 == 0:
        print(f'{single.Index}, {len(candidates)}, {len(result)}, {datetime.datetime.now().time()}')
    i += 1
```

    (382, 0), 1, 1, 09:52:43.599615
    (382, 100), 39, 37, 09:52:58.423145
    (382, 200), 2, 1, 09:55:23.403673
    Skipping
    Skipping
    (384, 75), 3, 1, 09:55:25.240870
    (384, 175), 43, 2, 09:55:26.821191
    (384, 275), 2, 1, 09:55:31.440438
    (384, 375), 4, 1, 09:58:31.546163
    Skipping
    (385, 0), 8, 1, 10:06:40.658230
    (385, 100), 8, 1, 10:06:42.131495
    (385, 200), 19, 2, 10:06:42.881784
    (385, 300), 49, 5, 10:06:44.040206
    (385, 400), 8, 1, 10:06:45.018199
    (385, 500), 9, 1, 10:06:45.800152
    (385, 600), 12, 1, 10:06:47.396424
    (385, 700), 8, 1, 10:06:48.856026
    (385, 800), 41, 7, 10:06:49.996811
    (385, 900), 11, 1, 10:06:51.428400
    (385, 1000), 9, 1, 10:06:52.721987
    (385, 1100), 35, 4, 10:06:59.788080
    (385, 1200), 6, 1, 10:07:00.659880
    (385, 1300), 14, 1, 10:07:01.495731
    (385, 1400), 6, 1, 10:07:02.377350
    (385, 1500), 4, 1, 10:07:09.777131
    (385, 1600), 59, 23, 10:07:25.164006
    (400, 7), 10, 1, 10:46:35.646936
    (400, 107), 5, 1, 10:46:37.773023
    (406, 59), 3, 1, 10:46:38.471601
    (423, 6), 3, 2, 10:46:40.341737
    (473, 18), 7, 1, 10:46:44.977921
    (474, 19), 8, 1, 10:46:45.661708
    (477, 64), 39, 9, 10:46:46.345789
    (478, 86), 6, 1, 10:46:47.123575
    (478, 186), 39, 5, 10:46:47.940369
    (478, 286), 166, 5, 10:46:48.664000
    (481, 64), 9, 1, 10:46:49.520919
    (481, 164), 9, 1, 10:46:50.218066
    (482, 35), 12, 1, 10:46:51.012542
    (482, 135), 8, 1, 10:46:52.452880
    (482, 235), 23, 2, 10:46:53.826370
    (482, 335), 892, 128, 10:46:58.568794
    (482, 435), 6, 1, 10:46:59.512353
    (482, 535), 23, 3, 10:47:05.945392
    (482, 635), 19, 1, 10:47:06.650393
    (482, 735), 12, 2, 10:47:07.541110
    (482, 835), 10, 1, 10:47:08.425345
    (482, 935), 19, 2, 10:47:09.586329
    (482, 1035), 51, 3, 10:47:10.508404
    (482, 1135), 24, 3, 10:47:11.645213
    (482, 1235), 504, 139, 10:47:49.192979
    (483, 87), 18, 3, 10:47:50.070922
    (486, 6), 9, 1, 10:47:50.862073
    (488, 16), 67, 10, 10:47:52.145819
    (488, 116), 14, 2, 10:47:52.852480
    (492, 0), 10, 1, 10:47:53.570068
    (492, 100), 20, 2, 10:47:54.334374
    (493, 44), 11, 1, 10:47:55.133151
    (493, 144), 8, 1, 10:47:55.934044
    (493, 244), 12, 1, 10:47:57.811672
    (493, 344), 9, 1, 10:47:58.541178
    (493, 444), 9, 1, 10:47:59.915779
    (493, 544), 7, 1, 10:48:07.197588
    (493, 644), 17, 1, 10:48:08.010453
    (493, 744), 15, 1, 10:48:08.782567
    (493, 844), 9, 1, 10:48:11.691208
    (497, 64), 12, 1, 10:48:15.625493
    (499, 21), 12, 1, 10:48:16.438908
    (500, 84), 11, 1, 10:48:17.192852
    (502, 29), 8, 1, 10:48:18.102689
    (505, 69), 14, 1, 10:48:18.953872
    (505, 169), 6, 1, 10:48:19.994865
    (505, 269), 18, 1, 10:48:20.778973
    (505, 369), 13, 2, 10:48:21.625813
    (505, 469), 45, 11, 10:48:22.522508
    (505, 569), 11, 2, 10:49:29.158007
    (505, 669), 7, 2, 10:49:30.015317
    (505, 769), 32, 1, 10:49:30.758270
    (505, 869), 54, 20, 10:49:47.208507
    (505, 969), 9, 1, 10:49:54.737932
    (510, 4), 26, 2, 10:50:00.312306
    (510, 104), 19, 4, 10:50:01.041496
    (510, 204), 21, 2, 10:50:01.714329
    (514, 21), 8, 1, 10:50:03.178390
    (514, 121), 13, 1, 10:50:04.068637
    (514, 221), 17, 2, 10:50:04.859203
    (515, 53), 8, 1, 10:50:05.659129
    (515, 153), 12, 1, 10:50:08.600087
    (515, 253), 40, 10, 10:50:09.538555
    (515, 353), 7, 1, 10:50:10.332325
    (516, 2), 16, 1, 10:50:27.604139
    (516, 102), 7, 1, 10:50:28.385599
    (520, 59), 12, 1, 10:50:29.184957
    (520, 159), 8, 1, 10:50:30.017161
    (520, 259), 6, 1, 10:50:30.726322
    (521, 79), 10, 1, 10:50:31.709011
    (528, 11), 7, 1, 10:50:33.098985
    (529, 12), 8, 1, 10:50:33.859077
    (529, 112), 11, 1, 10:50:34.548284
    (530, 6), 8, 1, 10:50:35.250155
    (530, 106), 10, 1, 10:50:36.215396
    (531, 22), 16, 1, 10:50:36.932194
    


```python
def pickled (data, file_path):
    # don't want to overwrite data, so "x"b
    with open(file_path, 'xb') as f:
        pickle.dump(data, f)
```


```python
pickled(single_member_das, r'.\Dissolve\single_member_das_1.zip')
```
