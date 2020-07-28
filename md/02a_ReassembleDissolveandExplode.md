## Combine Multiprocessing Outputs Into a Single DataFrame
Multiprocessing shortened processing from ~20 hours to ~5 hours. However, multiple data frames need to be recombined and then geometrically exploded to provide similar output to the original serial approach.


```python
import pickle
import geopandas as gp
import pandas as pd
```


```python
def pickled (data, file_path):
    # don't want to overwrite data, so "xb"
    with open(file_path, 'xb') as f:
        pickle.dump(data, f)
```


```python
# load the dissolve parts
with open(r'.\02_Dissolve\dissolves.zip', 'rb') as f:
    dissolves = pickle.load(f)
with open(r'.\02_Dissolve\dissolves_1.zip', 'rb') as f:
    dissolves_1 = pickle.load(f)
with open(r'.\02_Dissolve\dissolves_2.zip', 'rb') as f:
    dissolves_2 = pickle.load(f)
```


```python
# combine the output
all_dissolves = dissolves + dissolves_1 + dissolves_2
all_dissolves_df = pd.concat(all_dissolves)
all_dissolves_df.info()
all_dissolves_df.head()
```

    <class 'geopandas.geodataframe.GeoDataFrame'>
    Int64Index: 174 entries, 389 to 385
    Data columns (total 1 columns):
     #   Column    Non-Null Count  Dtype   
    ---  ------    --------------  -----   
     0   geometry  174 non-null    geometry
    dtypes: geometry(1)
    memory usage: 2.7 KB
    




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
      <th>geometry</th>
    </tr>
    <tr>
      <th>member_id</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>389</th>
      <td>MULTIPOLYGON (((4719683.978 1755992.394, 47196...</td>
    </tr>
    <tr>
      <th>391</th>
      <td>MULTIPOLYGON (((7624593.359 1303102.643, 76245...</td>
    </tr>
    <tr>
      <th>392</th>
      <td>MULTIPOLYGON (((7752029.150 1443229.511, 77522...</td>
    </tr>
    <tr>
      <th>394</th>
      <td>MULTIPOLYGON (((7961765.294 1923011.337, 79617...</td>
    </tr>
    <tr>
      <th>395</th>
      <td>MULTIPOLYGON (((7462890.994 1187796.506, 74628...</td>
    </tr>
  </tbody>
</table>
</div>




```python
# dissolving will have created multipolygons which needs to be broken down into single polygons per record
# each single polygon is a single, cohesive, "language area"
all_dissolves_gdf = all_dissolves[0].explode()
for i in all_dissolves[1:]:
    all_dissolves_gdf = all_dissolves_gdf.append(i.explode())
```


```python
len(all_dissolves_gdf)
```




    26995




```python
pickled(all_dissolves_gdf, r'.\02_Dissolve\single_member_dissolve.zip')
```
