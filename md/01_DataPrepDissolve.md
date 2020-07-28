## ETL for Demographic and Geographic data from StatCan
Since the base profile data from StatCan is large (~14GB) and I only need a small portion of that, some ETL is required for efficient processing. The ETL also considers the detailed metadata that StatsCan uses for statistical significance and suppression.

### Sources:
Statistics Canada. No date. Census Profile - Age, Sex, Type of Dwelling, Families, Households, Marital Status, Language, Income, Immigration and Ethnocultural Diversity, Housing, Aboriginal Peoples, Education, Labour, Journey to Work, Mobility and Migration, and Language of Work for Canada, Provinces and Territories, Census Divisions, Census Subdivisions and Dissemination Areas, 2016 Census (Database). Release Date November 29, 2017
https://www150.statcan.gc.ca/n1/en/catalogue/98-401-X2016044

Statistics Canada. No date. 2016 Census - Boundary files, English, ArcGis&reg;, Dissemination areas (Cartographic Boundary File).
https://www12.statcan.gc.ca/census-recensement/alternative_alternatif.cfm?l=eng&dispext=zip&teng=lda_000b16a_e.zip&k=%20%20%20%2090414&loc=http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lda_000b16a_e.zip


```python
import csv
import pickle
```

#### Process the raw Census profile data and save to a much smaller text file.


```python
# file names
raw_input = r'E:\CensusData\2016\source\98-401-X2016044_eng_CSV\98-401-X2016044_English_CSV_data.csv'
etl_output = r'.\01_Load\HomeLanguage.txt'

# setup constants for N/A data and suppression based on StatCan notes
NA_SRP = '..'
NA = '...'
UNRELIABLE = 'F'
SUPPRESSED = 'x'

# classify them into zero or null values
ZERO = (UNRELIABLE, SUPPRESSED)
NULL = (NA_SRP, NA)

# specify the geographic level required, 4, Dissemination Area
GEO_LEVEL = ('4')

# create a dict of the columns or interest
COLUMNS = {
    'geo_code': 1,
    'geo_level': 2,
    'member_id': 10,
    'total': 12,
    'male': 13,
    'female': 14
}

# create a dict for the required census datum (member_id in StatCan terminology)
# just so I remember how I did this (from my SQL Server version of the data)
sql = '''
SELECT
    cast([Member_ID] as varchar(6)) + ': "'
    + [TrimDescription] + '",'
FROM 
    [Census2020].[dbo].[profileDictionary]
WHERE
    [bottomLevel] = 1 -- ungrouped data
    or [Member_ID] = 382 -- special case, single response denominator'''

DATUM = {
    382: "Single responses",
    384: "English",
    385: "French",
    389: "Blackfoot",
    391: "Atikamekw",
    392: "Montagnais (Innu)",
    393: "Moose Cree",
    394: "Naskapi",
    395: "Northern East Cree",
    396: "Plains Cree",
    397: "Southern East Cree",
    398: "Swampy Cree",
    399: "Woods Cree",
    400: "Cree, n.o.s.",
    402: "Malecite",
    403: "Mi'kmaq",
    405: "Algonquin",
    406: "Ojibway",
    407: "Oji-Cree",
    408: "Ottawa (Odawa)",
    409: "Algonquian languages, n.i.e.",
    412: "Babine (Wetsuwet'en)",
    413: "Beaver",
    414: "Carrier",
    415: "Chilcotin",
    416: "Dene",
    417: "Dogrib (Tlicho)",
    418: "Gwich'in",
    419: "Sarsi (Sarcee)",
    420: "Sekani",
    422: "North Slavey (Hare)",
    423: "South Slavey",
    424: "Slavey, n.o.s.",
    426: "Kaska (Nahani)",
    427: "Tahltan",
    429: "Northern Tutchone",
    430: "Southern Tutchone",
    431: "Athabaskan languages, n.i.e.",
    432: "Haida",
    434: "Inuinnaqtun (Inuvialuktun)",
    435: "Inuktitut",
    436: "Inuit languages, n.i.e.",
    438: "Cayuga",
    439: "Mohawk",
    440: "Oneida",
    441: "Iroquoian languages, n.i.e.",
    442: "Kutenai",
    443: "Michif",
    445: "Comox",
    446: "Halkomelem",
    447: "Lillooet",
    448: "Okanagan",
    449: "Shuswap (Secwepemctsin)",
    450: "Squamish",
    451: "Straits",
    452: "Thompson (Ntlakapamux)",
    453: "Salish languages, n.i.e.",
    455: "Dakota",
    456: "Stoney",
    457: "Siouan languages, n.i.e.",
    458: "Tlingit",
    460: "Gitxsan (Gitksan)",
    461: "Nisga'a",
    462: "Tsimshian",
    464: "Haisla",
    465: "Heiltsuk",
    466: "Kwakiutl (Kwak'wala)",
    467: "Nuu-chah-nulth (Nootka)",
    468: "Wakashan languages, n.i.e.",
    469: "Aboriginal languages, n.o.s.",
    473: "Kabyle",
    474: "Berber languages, n.i.e.",
    476: "Bilen",
    477: "Oromo",
    478: "Somali",
    479: "Cushitic languages, n.i.e.",
    481: "Amharic",
    482: "Arabic",
    483: "Assyrian Neo-Aramaic",
    484: "Chaldean Neo-Aramaic",
    485: "Harari",
    486: "Hebrew",
    487: "Maltese",
    488: "Tigrigna",
    489: "Semitic languages, n.i.e.",
    490: "Afro-Asiatic languages, n.i.e.",
    492: "Khmer (Cambodian)",
    493: "Vietnamese",
    494: "Austro-Asiatic languages, n.i.e",
    496: "Bikol",
    497: "Cebuano",
    498: "Fijian",
    499: "Hiligaynon",
    500: "Ilocano",
    501: "Malagasy",
    502: "Malay",
    503: "Pampangan (Kapampangan, Pampango)",
    504: "Pangasinan",
    505: "Tagalog (Pilipino, Filipino)",
    506: "Waray-Waray",
    507: "Austronesian languages, n.i.e.",
    509: "Haitian Creole",
    510: "Creole, n.o.s.",
    511: "Creole languages, n.i.e.",
    513: "Kannada",
    514: "Malayalam",
    515: "Tamil",
    516: "Telugu",
    517: "Dravidian languages, n.i.e.",
    518: "Hmong-Mien languages",
    520: "Albanian",
    521: "Armenian",
    524: "Latvian",
    525: "Lithuanian",
    527: "Belarusan",
    528: "Bosnian",
    529: "Bulgarian",
    530: "Croatian",
    531: "Czech",
    532: "Macedonian",
    533: "Polish",
    534: "Russian",
    535: "Serbian",
    536: "Serbo-Croatian",
    537: "Slovak",
    538: "Slovene (Slovenian)",
    539: "Ukrainian",
    540: "Slavic languages, n.i.e.",
    542: "Scottish Gaelic",
    543: "Welsh",
    544: "Celtic languages, n.i.e.",
    546: "Afrikaans",
    547: "Danish",
    548: "Dutch",
    549: "Frisian",
    550: "German",
    551: "Icelandic",
    552: "Norwegian",
    553: "Swedish",
    554: "Vlaams (Flemish)",
    555: "Yiddish",
    556: "Germanic languages, n.i.e.",
    557: "Greek",
    560: "Bengali",
    561: "Gujarati",
    562: "Hindi",
    563: "Kashmiri",
    564: "Konkani",
    565: "Marathi",
    566: "Nepali",
    567: "Oriya (Odia)",
    568: "Punjabi (Panjabi)",
    569: "Sindhi",
    570: "Sinhala (Sinhalese)",
    571: "Urdu",
    573: "Kurdish",
    574: "Pashto",
    575: "Persian (Farsi)",
    576: "Indo-Iranian languages, n.i.e.",
    578: "Catalan",
    579: "Italian",
    580: "Portuguese",
    581: "Romanian",
    582: "Spanish",
    583: "Italic (Romance) languages, n.i.e.",
    584: "Japanese",
    586: "Georgian",
    587: "Korean",
    589: "Mongolian",
    591: "Akan (Twi)",
    592: "Bamanankan",
    593: "Edo",
    594: "Ewe",
    595: "Fulah (Pular, Pulaar, Fulfulde)",
    596: "Ga",
    597: "Ganda",
    598: "Igbo",
    599: "Lingala",
    600: "Rundi (Kirundi)",
    601: "Kinyarwanda (Rwanda)",
    602: "Shona",
    603: "Swahili",
    604: "Wolof",
    605: "Yoruba",
    606: "Niger-Congo languages, n.i.e.",
    608: "Dinka",
    609: "Nilo-Saharan languages, n.i.e.",
    616: "Cantonese",
    617: "Hakka",
    618: "Mandarin",
    619: "Min Dong",
    620: "Min Nan (Chaochow, Teochow, Fukien, Taiwanese)",
    621: "Wu (Shanghainese)",
    622: "Chinese, n.o.s.",
    623: "Chinese languages, n.i.e.",
    625: "Burmese",
    626: "Karenic languages",
    627: "Tibetan",
    628: "Tibeto-Burman languages, n.i.e.",
    630: "Lao",
    631: "Thai",
    632: "Tai-Kadai languages, n.i.e",
    634: "Azerbaijani",
    635: "Turkish",
    636: "Uyghur",
    637: "Uzbek",
    638: "Turkic languages, n.i.e.",
    640: "Estonian",
    641: "Finnish",
    642: "Hungarian",
    643: "Uralic languages, n.i.e.",
    644: "Other languages, n.i.e."
}
```


```python
def pickled (data, file_path):
    # don't want to overwrite data, so "xb"
    with open(file_path, 'xb') as f:
        pickle.dump(data, f)
```


```python
pickled(DATUM, r'.\01_Load\DATUM.zip')
```


```python
# read the base file, filter for the required DATUM and take care of NULLs etc.
# keep track of how many rows are written
i = 0

with open(raw_input, 'r') as f:
    reader = csv.reader(f)
    with open(etl_output, 'w') as w:
        for row in reader:
            # principal datum for the row
            member_id = row[COLUMNS['member_id']]
            geo_level = row[COLUMNS['geo_level']]
            # processing the required geographic level and datum
            # (relying on short-circuit boolean test so the int() works!)
            if geo_level in GEO_LEVEL and int(member_id) in DATUM:
                geo_code = row[COLUMNS['geo_code']]
                if row[COLUMNS['total']] in ZERO:
                    total = 0
                elif row[COLUMNS['total']] in NULL:
                    total = ''
                else:
                    total = row[COLUMNS['total']]
                if row[COLUMNS['male']] in ZERO:
                    male = 0
                elif row[COLUMNS['male']] in NULL:
                    male = ''
                else:
                    male = row[COLUMNS['male']]
                if row[COLUMNS['female']] in ZERO:
                    female = 0
                elif row[COLUMNS['female']] in NULL:
                    female = ''
                else:
                    female = row[COLUMNS['female']]
                
                new_row = f'{geo_code},{member_id},{total},{male},{female}\n'
                w.write(new_row)
                i += 1
                # print status every-so-ofen
                if i % 1_000_000 == 0:
                    print(i)
```

    1000000
    2000000
    3000000
    4000000
    5000000
    6000000
    7000000
    8000000
    9000000
    10000000
    11000000
    


```python
print(f'Total records written: {i}')
```

    Total records written: 11997080
    

## Load into Pandas


```python
import pandas as pd
import numpy as np
```


```python
# prepare dataframe metadata
header = None
names = ['da', 'member_id', 'total', 'male', 'female']
index_col = False
usecols = [0, 1, 2]
# using pd.Int32Dtype to support integer NULLs
dtypes = {0: object, 1: np.uint16, 2: pd.Int16Dtype()}
na_values = ['']
```


```python
da_df = pd.read_csv(etl_output, header=header, names=names, usecols=usecols, na_values=na_values, index_col=index_col,
                 dtype=dtypes)
da_df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 11997080 entries, 0 to 11997079
    Data columns (total 3 columns):
    da           object
    member_id    uint16
    total        Int16
    dtypes: Int16(1), object(1), uint16(1)
    memory usage: 148.7+ MB
    


```python
# save it for future processing
pickled(da_df, r'.\01_Load\da_df.zip')
```

## Load Spatial Data
The core part of "Language Area" (LA) analysis is spatal processing of StatCan Dissemination Area (DA) polygons.


```python
import geopandas as gp
```


```python
# NOTE: this has been validated using QGIS, keeping self-intersecting rings
da_poly_file = r'.\01_Load\lda_000b16a_e_valid_rings.shp'
da_gdf = gp.read_file(da_poly_file)
# a LOT of columns in this file, let's get the bare essentials
da_gdf = da_gdf[['DAUID', 'geometry']]
da_gdf.info()
da_gdf.head()
```

    <class 'geopandas.geodataframe.GeoDataFrame'>
    RangeIndex: 56589 entries, 0 to 56588
    Data columns (total 2 columns):
    DAUID       56589 non-null object
    geometry    56589 non-null geometry
    dtypes: geometry(1), object(1)
    memory usage: 884.3+ KB
    




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
      <th>DAUID</th>
      <th>geometry</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10010244</td>
      <td>POLYGON ((8976851.149 2149576.543, 8976818.149...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10010245</td>
      <td>POLYGON ((8977202.180 2150836.794, 8977136.277...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10010246</td>
      <td>POLYGON ((8977549.383 2150892.566, 8977492.269...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010247</td>
      <td>POLYGON ((8977682.314 2151083.183, 8977689.440...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>10010248</td>
      <td>POLYGON ((8978152.474 2151142.586, 8978040.654...</td>
    </tr>
  </tbody>
</table>
</div>



### Select the largest polygon in a multipolygon
Later processing will use the spatial operator 'within' which proved challenging for multipolygons, so extracting the largest polygon within those multipolgons for analysis.


```python
# split up multipolygons
da_single = da_gdf.explode()
# explicitly calculate the polygon area
da_single['area'] = da_single['geometry'].area
# find the largest polygon for a DA
da_single_max = da_single.loc[da_single.groupby(["DAUID"])["area"].idxmax()] 
da_single_max.info()
```

    <class 'geopandas.geodataframe.GeoDataFrame'>
    MultiIndex: 56589 entries, (33, 0) to (55486, 0)
    Data columns (total 3 columns):
    DAUID       56589 non-null object
    geometry    56589 non-null geometry
    area        56589 non-null float64
    dtypes: float64(1), geometry(1), object(1)
    memory usage: 2.1+ MB
    


```python
# save for later, the 'area' is especially important
pickled(da_single_max, r'.\01_Load\da_single_max.zip')
```

## Create a master data set with Geometry and Language Data


```python
# merge (join) DA profile data with boundaries
da_df_max = da_single_max.merge(da_df, left_on='DAUID', right_on='da')
da_df_max.info()
```

    <class 'geopandas.geodataframe.GeoDataFrame'>
    Int64Index: 11996868 entries, 0 to 11996867
    Data columns (total 6 columns):
    DAUID        object
    geometry     geometry
    area         float64
    da           object
    member_id    uint16
    total        Int16
    dtypes: Int16(1), float64(1), geometry(1), object(2), uint16(1)
    memory usage: 514.8+ MB
    

## Calculate Language Area (LA) Polygons


```python
# select necessary columns and only those where total is > 5 (avoid random rounding problems),
# and exclude the single response denominator (382)
da_df_for_buffer = da_df_max[['member_id', 'geometry']].loc[(da_df_max['total'] > 5) & (da_df_max['member_id'] > 382)]
# buffer the polygons
da_df_for_buffer['geometry'] = da_df_for_buffer['geometry'].buffer(1, resolution=5)
```


```python
pickled(da_df_for_buffer, r'.\01_Load\da_df_for_buffer.zip')
```
## Dissolve polygons (spatial union) by their language ID (aka member_id)

### Use multiprocessing to save time
Worked well for smaller LAs, but blocked for the two largest LAs: English and French


```python
from multiprocessing import Pool
```


```python
### THIS NEEDS TO BE RUN OUTSIDE OF JUPYTER NOTEBOOKS, WITH THE INTERPRETER DIRECTLY
def dissolve_single(member_id):
    single_member = da_df_for_buffer.loc[da_df_for_buffer['member_id']==member_id]
    return single_member.dissolve(by='member_id', aggfunc='sum')

candidate_member_ids = list(DATUM)[1:] # don't want to dissolve the "single response" data

with Pool(4) as p:
    dissolves = p.map(dissolve_single, candidate_member_ids)
```
