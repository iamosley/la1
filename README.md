## Discovering Dominant Language Areas (LA) by Neighbourhood

### Which Single Response Languages from the 2016 Census have the highest population density by LA per Dissemination Area (aka neighbourhood) 

Language distribution using choropleth maps can be difficult. Within any specific geography many languages exist and are typically overshadowed by English and French. This methodology demonstrates a way to reliably identify principal "language areas" and which ones are the most dense (population per km<sup>2</sup> in a neighbourhood).

## An Example

All analysis for this project was done at the [Dissemination Area (DA)](https://www150.statcan.gc.ca/n1/pub/92-195-x/2011001/geo/da-ad/da-ad-eng.htm) level. In downtown Toronto, DA '35202296' has a combination of English, Italian, Portuguese and Spanish speakers. Let's look at this DA's language profile:

#### Table 1.
| Language   | Count | % of Total |
|------------|------:|-----------:|
| English    |   295 |      71.08 |
| Italian    |    60 |      14.46 |
| Portuguese |    35 |       8.43 |
| Spanish    |    25 |       6.03 |

When creating a choropleth map, English would typically be the identified language for this DA since it has an overwhelming 71.08% representation. This is not surprising. But what I wanted to achieve is a way to identify other languages that were significant but not well represented by a simple percentage calculation. Thinking (a lot) about this, I used a measure of population density per square kilometre to see how a language would 'rank'.

#### Table 2.
| Language   | Population per km<sup>2</sup> |
|------------|-------:|
| English    |   3.98 |
| Italian    |  39.25 |
| Portuguese | 143.11 |
| Spanish    |  73.97 |

**Interesting**. English is now the lowest ranked with a population density of 3.98 persons per km<sup>2</sup>. How can this be?

Well, I used an *unusual* calculation for density. This is not simply the DA's language counts divided by the area of the DA, but, the total count of all adjoining DAs that have a non-zero count &mdash; for that specific language &mdash; divided by the sum of their areas. These contiguous polygons of language I call **Language Areas** (LA). 

Let's have a look at DA 35202296 and its two LA polygons representing Portuguese and Spanish.

#### Figure 1.
![alt text](https://iamosley.github.io/la/img/da_la_overlap.png "Figure 1.")

The red outline is DA 35202296, and the '///' hatched areas are Portuguese speaking areas, and '\\\\\\' hatched areas are Spanish. Many of the areas are both, including the DA in question. Given that Portuguese has the highest density, this DA would be hatched '///' in a single-value map.

Here is the map re-presented with dominant language based on LA density: green showing Portuguese and blue for Spanish. (Uncoloured areas have been assigned a language other than Portuguese or Spanish in the complete analysis.)

#### Figure 2.
![alt text](https://iamosley.github.io/la/img/da_la_overlap_single.png "Figure 2.")

As is seen in **Figure 2**, the target DA is Portuguese confirming the calculations from **Table 2**. Interesting to note that when DAs comprise both Portuguese and Spanish, Portuguese is the more representative from an LA perspective. 
