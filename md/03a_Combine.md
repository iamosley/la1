## The Goal: Determine the Densest LA for each DA
Which LA best represents the DA? Calculate the population density per LA and rank them across each constituent DA (there can be many LAs for a single DA). LA with rank '1' for a specific DA is the most representative.


```python
import pickle
import pandas as pd
import numpy as np
```


```python
# percent rank of language per DA
with open(r'.\03_Stats\da_df_language_rank.zip', 'rb') as f:
    da_rank = pickle.load(f)
# density of language per LA
with open(r'.\03_Stats\single_member_dissolve.zip', 'rb') as f:
    la_density = pickle.load(f)
# lookup keys between LAs (member_id and area) and DA and member_id
with open(r'.\03_Stats\la_da_df_lookup.zip', 'rb') as f:
    la_da_lookup = pickle.load(f)
```


```python
display(da_rank.info())
display(da_rank.head())
display(la_density.head())
display(la_da_lookup[:10])
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 369028 entries, 0 to 369027
    Data columns (total 7 columns):
    da             369028 non-null object
    member_id_x    369028 non-null uint16
    total_x        369028 non-null Int16
    member_id_y    369028 non-null uint16
    total_y        369028 non-null Int16
    p_total        369028 non-null float32
    rank           369028 non-null int16
    dtypes: Int16(2), float32(1), int16(1), object(1), uint16(2)
    memory usage: 11.3+ MB

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
      <th>da</th>
      <th>member_id_x</th>
      <th>total_x</th>
      <th>member_id_y</th>
      <th>total_y</th>
      <th>p_total</th>
      <th>rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10010734</td>
      <td>384</td>
      <td>150</td>
      <td>382</td>
      <td>150</td>
      <td>100.000000</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10010735</td>
      <td>384</td>
      <td>350</td>
      <td>382</td>
      <td>350</td>
      <td>100.000000</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10010736</td>
      <td>384</td>
      <td>125</td>
      <td>382</td>
      <td>130</td>
      <td>96.153847</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010733</td>
      <td>384</td>
      <td>65</td>
      <td>382</td>
      <td>65</td>
      <td>100.000000</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10010737</td>
      <td>384</td>
      <td>365</td>
      <td>382</td>
      <td>365</td>
      <td>100.000000</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



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



    [[(382, 211), ('24150028', 382)],
     [(382, 211), ('24150029', 382)],
     [(382, 211), ('24150030', 382)],
     [(382, 211), ('24150031', 382)],
     [(382, 211), ('24150032', 382)],
     [(382, 211), ('24150033', 382)],
     [(382, 211), ('24150034', 382)],
     [(382, 211), ('24150035', 382)],
     [(382, 211), ('24150036', 382)],
     [(382, 211), ('24150037', 382)]]



```python
# create a better lookup: don't need the DA's polygon id
la_da_index = [[i[0][0], i[0][1], i[1][0]] for i in la_da_lookup]
la_da_index[:10]
```




    [[382, 211, '24150028'],
     [382, 211, '24150029'],
     [382, 211, '24150030'],
     [382, 211, '24150031'],
     [382, 211, '24150032'],
     [382, 211, '24150033'],
     [382, 211, '24150034'],
     [382, 211, '24150035'],
     [382, 211, '24150036'],
     [382, 211, '24150037']]




```python
# convert the list into a dataframe
la_da = pd.DataFrame(la_da_index, columns=['member_id', 'la_area', 'da'])
la_da.head()
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
      <th>member_id</th>
      <th>la_area</th>
      <th>da</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>382</td>
      <td>211</td>
      <td>24150028</td>
    </tr>
    <tr>
      <th>1</th>
      <td>382</td>
      <td>211</td>
      <td>24150029</td>
    </tr>
    <tr>
      <th>2</th>
      <td>382</td>
      <td>211</td>
      <td>24150030</td>
    </tr>
    <tr>
      <th>3</th>
      <td>382</td>
      <td>211</td>
      <td>24150031</td>
    </tr>
    <tr>
      <th>4</th>
      <td>382</td>
      <td>211</td>
      <td>24150032</td>
    </tr>
  </tbody>
</table>
</div>




```python
# restore the row keys as columns for use in a later merge
la_density_reset = la_density[['total', 'area', 'density_km2']].reset_index()
la_density_reset.rename(columns={"level_1": "la_area"}, inplace=True)
la_density_reset.info()
la_density_reset.head()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 27222 entries, 0 to 27221
    Data columns (total 5 columns):
    member_id      27222 non-null int64
    la_area        27222 non-null int64
    total          27222 non-null int64
    area           27222 non-null float64
    density_km2    27222 non-null float64
    dtypes: float64(2), int64(3)
    memory usage: 1.0 MB
    




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
      <th>member_id</th>
      <th>la_area</th>
      <th>total</th>
      <th>area</th>
      <th>density_km2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>382</td>
      <td>0</td>
      <td>235</td>
      <td>4.418451e+07</td>
      <td>5.318606</td>
    </tr>
    <tr>
      <th>1</th>
      <td>382</td>
      <td>1</td>
      <td>560</td>
      <td>5.133985e+06</td>
      <td>109.077063</td>
    </tr>
    <tr>
      <th>2</th>
      <td>382</td>
      <td>2</td>
      <td>595</td>
      <td>8.835379e+04</td>
      <td>6734.289422</td>
    </tr>
    <tr>
      <th>3</th>
      <td>382</td>
      <td>3</td>
      <td>460</td>
      <td>5.235135e+04</td>
      <td>8786.784274</td>
    </tr>
    <tr>
      <th>4</th>
      <td>382</td>
      <td>4</td>
      <td>615</td>
      <td>2.668304e+06</td>
      <td>230.483519</td>
    </tr>
  </tbody>
</table>
</div>




```python
# merge the LA density values and the DA ids
la_density_da = la_density_reset.loc[la_density_reset['member_id'] > 382].merge(la_da, on=['member_id', 'la_area'])
la_density_da.info()
la_density_da.sort_values('da').head()
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 173888 entries, 0 to 173887
    Data columns (total 6 columns):
    member_id      173888 non-null int64
    la_area        173888 non-null int64
    total          173888 non-null int64
    area           173888 non-null float64
    density_km2    173888 non-null float64
    da             173888 non-null object
    dtypes: float64(2), int64(3), object(1)
    memory usage: 9.3+ MB
    




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
      <th>member_id</th>
      <th>la_area</th>
      <th>total</th>
      <th>area</th>
      <th>density_km2</th>
      <th>da</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2061</th>
      <td>384</td>
      <td>336</td>
      <td>455705</td>
      <td>9.838252e+10</td>
      <td>4.631971</td>
      <td>10010165</td>
    </tr>
    <tr>
      <th>2063</th>
      <td>384</td>
      <td>336</td>
      <td>455705</td>
      <td>9.838252e+10</td>
      <td>4.631971</td>
      <td>10010166</td>
    </tr>
    <tr>
      <th>2068</th>
      <td>384</td>
      <td>336</td>
      <td>455705</td>
      <td>9.838252e+10</td>
      <td>4.631971</td>
      <td>10010167</td>
    </tr>
    <tr>
      <th>2073</th>
      <td>384</td>
      <td>336</td>
      <td>455705</td>
      <td>9.838252e+10</td>
      <td>4.631971</td>
      <td>10010168</td>
    </tr>
    <tr>
      <th>2076</th>
      <td>384</td>
      <td>336</td>
      <td>455705</td>
      <td>9.838252e+10</td>
      <td>4.631971</td>
      <td>10010169</td>
    </tr>
  </tbody>
</table>
</div>



## Rank LA language densities for each DA
LA with the highest density for a specific DA is the most representative.  This 'flattens' the LA overlapping areas into a single language id (member_id) for each DA neighbourhood. This achieves the desired result of creating a spatial assignment of single response languages that are not defined by the maximum percentage for each DA, but a competitive spatial maximum based on density.


```python
la_density_da_rank = la_density_da
la_density_da_rank["rank"] = la_density_da_rank.groupby("da")["density_km2"].rank("first", ascending=False).astype(np.int16)
la_density_da_rank.head()
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
      <th>member_id</th>
      <th>la_area</th>
      <th>total</th>
      <th>area</th>
      <th>density_km2</th>
      <th>da</th>
      <th>rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>384</td>
      <td>0</td>
      <td>205</td>
      <td>4.418451e+07</td>
      <td>4.639635</td>
      <td>35370839</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>384</td>
      <td>1</td>
      <td>550</td>
      <td>5.133985e+06</td>
      <td>107.129258</td>
      <td>35370692</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>384</td>
      <td>2</td>
      <td>295</td>
      <td>8.835379e+04</td>
      <td>3338.849377</td>
      <td>35211874</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>384</td>
      <td>3</td>
      <td>200</td>
      <td>5.235135e+04</td>
      <td>3820.340989</td>
      <td>35211873</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>384</td>
      <td>4</td>
      <td>590</td>
      <td>2.668304e+06</td>
      <td>221.114270</td>
      <td>35203174</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




## Select top ranked LA (language area) for the DA (dissemination area)
```python
la_density_da_rank_1 = la_density_da_rank.loc[la_density_da_rank['rank']==1, ['member_id', 'la_area', 'density_km2', 'da']]
```

Total count is 54,999


```python
la_density_da_rank_1.head()
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
      <th>member_id</th>
      <th>la_area</th>
      <th>density_km2</th>
      <th>da</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>384</td>
      <td>0</td>
      <td>4.639635</td>
      <td>35370839</td>
    </tr>
    <tr>
      <th>1</th>
      <td>384</td>
      <td>1</td>
      <td>107.129258</td>
      <td>35370692</td>
    </tr>
    <tr>
      <th>2</th>
      <td>384</td>
      <td>2</td>
      <td>3338.849377</td>
      <td>35211874</td>
    </tr>
    <tr>
      <th>3</th>
      <td>384</td>
      <td>3</td>
      <td>3820.340989</td>
      <td>35211873</td>
    </tr>
    <tr>
      <th>4</th>
      <td>384</td>
      <td>4</td>
      <td>221.114270</td>
      <td>35203174</td>
    </tr>
  </tbody>
</table>
</div>



# All done!
