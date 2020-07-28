## Calculate population, area and density values based on LA-DA intersections.


```python
import pickle
import numpy as np
import geopandas as gp
```


```python
# reload required data
with open(r'.\01_Load\da_df.zip', 'rb') as f:
    da_df = pickle.load(f)
with open(r'.\01_Load\da_single_max.zip', 'rb') as f:
    da_single_max = pickle.load(f)
with open(r'.\02_Dissolve\single_member_dissolve.zip', 'rb') as f:
    single_member_dissolve = pickle.load(f)
with open(r'.\02_Dissolve\single_member_das_1.zip', 'rb') as f:
    single_member_das_1 = pickle.load(f)
with open(r'.\02_Dissolve\single_member_das_2_Bad_1.zip', 'rb') as f:
    single_member_das_2_Bad_1 = pickle.load(f)
with open(r'.\02_Dissolve\single_member_das_2_Bad_2.zip', 'rb') as f:
    single_member_das_2_Bad_2 = pickle.load(f)
with open(r'.\02_Dissolve\single_member_das_2_Bad_3.zip', 'rb') as f:
    single_member_das_2_Bad_3 = pickle.load(f)
with open(r'.\02_Dissolve\single_member_das_4.zip', 'rb') as f:
    single_member_das_4 = pickle.load(f)
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
# need to change this from uint16 since numpy fails on the key lookup
da_df['member_id'] = da_df['member_id'].astype(np.int16)
```

### Take note of the number of combinations for languages (member_id) and DAs: 11,997,080!


```python
da_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 11997080 entries, 0 to 11997079
    Data columns (total 3 columns):
    da           object
    member_id    int16
    total        Int16
    dtypes: Int16(1), int16(1), object(1)
    memory usage: 148.7+ MB
    


```python
# need to increase the speed of the DA and Member_ID lookups, so make them indexes
da_df_indexed = da_df.set_index(['da', 'member_id'], verify_integrity=True)
da_df_indexed.info()
```

    <class 'pandas.core.frame.DataFrame'>
    MultiIndex: 11997080 entries, (10010732, 382) to (62080015, 644)
    Data columns (total 1 columns):
    total    Int16
    dtypes: Int16(1)
    memory usage: 103.4+ MB
    


```python
# append all of the LA-DA matches into a single list
la_das = single_member_das_2_Bad_1 + \
    single_member_das_2_Bad_2 + \
    single_member_das_2_Bad_3 + \
    single_member_das_1 + \
    single_member_das_4
```


```python
print(f'Total LA-DA combinations (total i): {len(la_das)}')
```

    Total LA-DA combinations (total i): 27222
    

## Recalculate the total language population and area for each LA
Using the LA area is not valid since is a spatial union of buffered DA polygons. Also, the 'total' for each LA is an artifact of the original member_id, before it was exploded into single polygons.
### Important to understand the composition of the LA-DA list.
- An LA is a language ID (member_id) along with a specific LA. Each language can have mutiple LAs. In this case 27,222. 
- e.g. (384, 23): 'English', languare are '23'
- The DA list comprises of the DA's row index: the internal DA id and its 'exploded' id from step 1. (the latter id is of no use, but included since I didn't want to rebuild the row index key). 
- e.g. [(8995, 0), ...]) DA internal id '8995' and exploded polygon id of '0'.


```python
# calculate the LA's total population based on the la_da[]
total_pop = 0
total_area = 0
last_la = None
i = 0
la_da_df_lookup = []
# iterate the list of LAs and DAs ('language Area's and Dissemination Areas)
for la, das in la_das: # [(384, 23), [(8995, 0), ...]] [la, das] 
    if la != last_la and last_la is not None:
        # update the LA stats
        single_member_dissolve.loc[last_la, ['area', 'total']] = [total_area, total_pop]
        total_area = 0
        total_pop = 0
    # get a list of the actual DA names, and its area
    da_areas = da_single_max.loc[das][['DAUID', 'area']].values.tolist()
    total_area += sum([i[1] for i in da_areas])
    j = 0
    for da_area in da_areas:
        dauid = da_area[0]
        # build the lookup key using the DA name and member_id (first part of the LA)
        loc_key = (dauid, la[0])
        # save the LA to DA-Member_ID lookup
        la_da_df_lookup += [[la, (dauid, la[0])]]
        # accumulate the pop
        total_pop += da_df_indexed.loc[loc_key]
        if (i + j) % 10_000 == 0:
            print(f'i: {i}, {j} of {len(da_areas)}')
        j += 1
    last_la = la
    i += 1
```

    i: 0, 0 of 40559
    i: 0, 10000 of 40559
    i: 0, 20000 of 40559
    i: 0, 30000 of 40559
    i: 0, 40000 of 40559
    i: 1, 9999 of 37215
    i: 1, 19999 of 37215
    i: 1, 29999 of 37215
    i: 10000, 0 of 1
    i: 19095, 905 of 976
    i: 19923, 77 of 718
    i: 19924, 76 of 180
    i: 19998, 2 of 3
    i: 19999, 1 of 2
    i: 20000, 0 of 2
    


```python
def pickled (data, file_path):
    # don't want to overwrite data, so "x"b
    with open(file_path, 'xb') as f:
        pickle.dump(data, f)
```


```python
# with correct totals, calc the desnity per square km
single_member_dissolve['density_km2'] = (single_member_dissolve['total'] /
                                        (single_member_dissolve['area'] / 1_000_000))
```


```python
single_member_dissolve.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>total</th>
      <th>area</th>
      <th>geometry</th>
      <th>density_km2</th>
    </tr>
    <tr>
      <th>member_id</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">382</th>
      <th>0</th>
      <td>235</td>
      <td>4.418451e+07</td>
      <td>POLYGON ((6989942.345 676901.328, 6989942.475 ...</td>
      <td>5.318606</td>
    </tr>
    <tr>
      <th>1</th>
      <td>560</td>
      <td>5.133985e+06</td>
      <td>POLYGON ((6942839.136 718082.230, 6942839.543 ...</td>
      <td>109.077063</td>
    </tr>
    <tr>
      <th>2</th>
      <td>595</td>
      <td>8.835379e+04</td>
      <td>POLYGON ((7194899.915 921123.174, 7194960.339 ...</td>
      <td>6734.289422</td>
    </tr>
    <tr>
      <th>3</th>
      <td>460</td>
      <td>5.235135e+04</td>
      <td>POLYGON ((7195136.558 921590.961, 7195136.873 ...</td>
      <td>8786.784274</td>
    </tr>
    <tr>
      <th>4</th>
      <td>615</td>
      <td>2.668304e+06</td>
      <td>POLYGON ((7227689.708 927052.952, 7227689.937 ...</td>
      <td>230.483519</td>
    </tr>
  </tbody>
</table>
</div>




```python
pickled(single_member_dissolve, r'.\03_Stats\single_member_dissolve.zip')
pickled(la_da_df_lookup, r'.\03_Stats\la_da_df_lookup.zip')
```
