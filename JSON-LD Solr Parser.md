- **id:** Root level ID item
- **record_id:** No longer needed. This was earlier used as a construct for the URL.
- **title** No longer needed as data is pulled in from the data store.
- **data:** No longer needed as data is pulled in from the data store.
- **record_metadata_arrived_in_LUX_dtsi:** This should be a timestamp created programatically at time of ingest.
- **record_metadata_rights_label:** Unneccsary.
- **record_metadata_identifier:** No longer used. Now append to the end value of the ID.
- **identifiers_identifier_value:** Look in the root for the key ```identified_by```. Traverse the elements and match on items where ```type: "Identifier"```. The value is in the ```content``` key.
  ```json
  {
    ...
    "identified_by":
    [
      ...
      {
        "type": "Identifier",
        "content": "abc.123",
      },
      ...
    ],
    ...
  }
  ```
- **identifiers_identifier_display:** Combine values from ```identifiers_identifier_value``` and traverse the same array looking for the ```identified_by``` array. The value is in the ```content``` key.
```json
  {
    ...
    "identified_by":
    [
      ...
      {
        "type": "Identifier",
        "content": "abc.123",
        "identified_by": [
          {
            "content": "ABC 123"
          }
        ]
        ...
      },
      ...
    ],
    ...
  }
```
  The two arrays should be combined as one for the Solr value array.
- **identifiers_identifier_type:** Look in the root for the key ```identified_by```. Traverse the elements and match on items where ```type: "Identifier"```. Iterate over the ```classified_as``` array and look  for the ```id``` key to equal "http://vocab.getty.edu/aat/300435704". Grab the content from the ```_label``` key.
```json
  {
    ...
    "identified_by":
    [
      ...
      {
        "type": "Identifier",
        "content": "abc.123",
        "classsified_by": [
          {
            ...
           "id": "http://vocab.getty.edu/aat/300435704",
            "label": "System-Assigned Number"          
          }
        ]
      },
      ...
    ],
    ...
  }
```
- **identifiers_identifier_label:** No longer used. Duplicate of ```identifiers_identifier_type_tim```.
- **basic_descriptors_supertypes_level:** Combines four level of supertypes into one field. This will contain the lowest level item (including specific type).  The level 1-4 hierarchy should be created outside of the fulltext search.  To get the supertypes, traverse the ```classified_as``` array, match ```type: "Type"``` and find the ```_label``` key.
```json
  {
    ...
    "classified_by":
    [
      ...
      {
        "type": "Type",
        "_label": "Tools and Equipment"
      },
      ...
    ],
    ...
  }
  ```
- **format:** No longer used. This is a calculated value based on supertypes. This can be derived outside of the fulltext search.
- **basic_descriptors_specific_type:** No longer used. This is now represented in ```basic_descriptors_supertypes_level_tim```. The specific type will override the supertype when present.
- **basic_descriptors_edition_display:** Look in the root for the array ```referred_to_by```. Traverse the array ```classified_as``` and match the key ```id: "http://vocab.getty.edu/aat/300435435"``` or ```_label: "Edition Statement```. Go up one level (in ```classified_as```) and grab the ```content``` key. 
```json
  {
    ...
   "referred_to_by": [
    {
      "type": "LinguisticObject",
      "content": "Score and parts",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300435435",
          "type": "Type",
          "_label": "Edition Statement",
          ...
        }
      ]
    },
    ...
  }
```
- **basic_descriptors_imprint_display:** Look in the root for the array ```referred_to_by```. Traverse the array ```classified_as``` and match the key ```id: "http://vocab.getty.edu/aat/300435436"``` or ```_label: "Production Statement```. Go up one level (from  ```classified_as```) and grab the ```content``` key. 
```json
  {
    ...
    "referred_to_by":
    [
      ...
      {
        "type": "LinguisticObject",
        "content": "Imprint text",
        "classified_as": 
        [
          {
            "id": "http://vocab.getty.edu/aat/300435436",
            "type": "Type",
            "_label": "Production Statement"
          }
        ]
      },
      ...
    ],
    ...
  }
```
- **basic_descriptors_materials_type:**  Look in the root for the array ```made_of```. Traverse the array and grab the ```_label``` key.
```json
{
  ...
   "made_of": [
    {
      "id": "http://vocab.getty.edu/page/aat/300011098",
      "type": "Material",
      "_label": "graphite"
    },
    {
      "id": "http://vocab.getty.edu/page/aat/300014974",
      "type": "Material",
      "_label": "varnish"
    },
    ...
}
```
- **basic_descriptors_inscription_display:** Look in the root for the array ```referred_to_by```. Traverse the array ```classified_as``` and match the key ```"id": "http://vocab.getty.edu/aat/300435414"``` or ```"_label": "Inscription"```. Go up one level (in ```classified_as```) and grab the ```content``` key. 
```json
{
  ...
  "referred_to_by": [
    ...
    {
      "type": "LinguisticObject",
      "content": "Inscribed in graphite: \"F.S.\"",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300435414",
          "type": "Type",
          "_label": "Inscription",
          ...
        }
      ]
    },
    ...
  ]
  ...
}
```
- **basic_descriptors_inscription_type:** No longer used.
- **basic_descriptors_provenance_display:** Look in the root for the array ```referred_to_by```. Traverse the array ```classified_as``` and match the key ```"id": "http://vocab.getty.edu/aat/300435438"``` or ```"_label": "Provenance Statement"```. Go up one level (from ```classified_as```) and grab the ```content``` key. 
```json
{
  ...
  "referred_to_by": [
    ...
    {
      "type": "LinguisticObject",
      "content": "Strawberry Hill Sale, Day 8, 50 to H.G. Bohn; (London, 733).",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300435438",
          "type": "Type",
          "_label": "Provenance Statement",
          ...
        }
      ]
    },
    ...
  ]
  ...
}
```
- **basic_descriptors_acquisition_source_display:** Look in the root for the array ```referred_to_by```. Traverse the array ```classified_as``` and match the key ```"id": "http://vocab.getty.edu/aat/300435439"``` or ```"_label": "Acquisition Statement"```. Go up one level (from ```classified_as```) and grab the ```content``` key. 
```json
{
  ...
  "referred_to_by": [
    ...
    {
      "type": "LinguisticObject",
      "content": "<p>Purchased from R. A. Gekoski Booksellers on the Edwin J. Beinecke Book Fund, 1994-2005, 2010; purchased from R. A. Gekoski Booksellers on the James Marshall and Marie-Louise Osborn Collection Fund, 2015; purchased from Peter Grogan on the Edwin J. Beinecke Book Fund, 2016; gift of Peter Ackroyd, 2005-2011, 2016, 2019; gift of Random House, 2012; gift of Talese Editorial, 2015-2016. For further information see Collections Contents list.</p>",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300435439",
          "type": "Type",
          "_label": "Acquisition Statement",
          ...
        }
      ]
    },
    ...
  ]
  ...
}
```
- **titles_title_display** Look in the root for the array ```identified_by```. The ```type``` key should match to ```Name```. Use the ```content``` label.
```json
  ...
  "identified_by": [
    {
      "type": "Name",
      "content": "Commercial Paper Sample Book Collection, circa 1930-2000",
      ...
    },
    ...
```
- **titles_title_type** Look in the root for the array ```identified_by```. The ```type``` key should match to ```Name```. Traverse the ```classified_as``` as array and use the ```content``` label.
```json
  ...
  "identified_by": [
    {
      "type": "Name",
      "content": "Commercial Paper Sample Book Collection, circa 1930-2000",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300404670",
          "type": "Type",
          "_label": "Primary Name"
        }
      ]
    },
    ...
```
- **measurements_measurement_label**  Look in the root for the array ```dimension```. Traverse the array and look for the key ```type``` and it should match ```Dimension```. Inside of that, look in the ```identified_by``` object. Grab ```_label``` key.
```json
...
"dimension": [
    {
      "type": "Dimension",
      "value": 12.5,
      "identified_by": [
        {
          "type": "Type",
          "_label": "extent"
        }
      ]
  ...
```
- **measurements_measurement_form_measurement_element** Unknown. Still in progress.
- **measurements_measurement_form_measurement_display_measurement_value** Look in the root for the array ```dimension```. Traverse the array and look for the key ```type``` and it should match ```Dimension```. Inside of that, grab ```value``` key.
```json
...
"dimension": [
    {
      "type": "Dimension",
      "value": 12.5,
    ...
  ...
```
- **measurements_measurement_form_measurement_aspect_measurement_unit** Look in the root for the array ```dimension```. Traverse the array and look for the key ```type``` and it should match ```Dimension```. Inside of that, look in the ```unit``` object. Grab ```_label``` key.
```json
...
"dimension": [
  {
    "type": "Dimension",
    "value": 30.6,
    "unit": {
      "id": "http://id.loc.gov/authorities/subjects/sh2008006746",
      "type": "MeasurementUnit",
      "_label": "cm"
    },
  ...
```
- **measurements_measurement_form_measurement_aspect_measurement_type** Look in the root for the array ```dimension```. Traverse the array and look for the key ```type``` and it should match ```Dimension```. Inside of that, look in the ```unit``` object. Grab ```id``` key.
```json
...
"dimension": [
  {
    "type": "Dimension",
    "value": 30.6,
    "unit": {
      "id": "http://id.loc.gov/authorities/subjects/sh2008006746",
      "type": "MeasurementUnit",
      "_label": "cm"
    },
  ...
```
- **notes_note_type** Look in the root for the ```referred_to_by``` array. Traverse the array and find the ```classified_as``` array. Traverse the ```classified_as``` array and grab the ```_label``` key.
```json
"referred_to_by": [
    {
      "type": "LinguisticObject",
      "content": "stone",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300435429",
          "type": "Type",
          "_label": "Material Statement",
          ...
        }
      ]
      ...
...
```
- **notes_note_display** Look in the root for the ```referred_to_by``` array. Traverse the array and grab the ```content``` key.
```json
"referred_to_by": [
    {
      "type": "LinguisticObject",
      "content": "Oil on canvas",
      ...
...
```
- **notes_note_label** Look in the root for the ```referred_to_by``` array. Traverse the array and find the ```identified_by``` array. Traverse the ```identified_by``` array and grab the ```content``` key.
```json
"referred_to_by": [
    {
      "type": "LinguisticObject",
      "content": "Oil on canvas",
      "identified_by": [
        {
          "type": "Name",
          "content": "Preferred Citation",
          ...
        }
      ]
      ...
...
```
- **languages_language_display** Look in the root for the ```language``` array. Traverse the array and grab the ```label``` key.
```json
"language": [
  {
    "label": "English"
  }
]
...
```
- **languages_language_code** Look in the root for the ```language``` array. Traverse the array and find the ```identified_by``` array. Traverse the array and grab the ```content``` key.
```json
"language": [
  {
    "label": "English",
    "identified_by": [
      {
        "content": "eng"
      }
    ]
  }
]
...
```
- **agents_agent_display** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, traverse the inner array of objects. Match the ```type``` key equal to "Person" or "Group". Once you find that value, look for the ```identified_by``` array and iterate over the items. Check if the ```type``` key equals "Name". If it does, grab the value of the ```content``` key.
```json
  "produced_by": {
    "type": "Production",
    "part": [
      {
       "type": "Production",
        "carried_out_by": [
          {
          "id": "urn:uuid:68878b8c-e08d-47cb-84e7-aecb9a4c79b4",
          "type": "Group",
          "identified_by": [
            {
              "type": "Name",
              "content": "Rome"
            }
          ],
...
```
- **agents_production** Not implemented.
- **agents_agent_sortname** Follow the values for **agents_agent_display**. Check if there is a array key called ```classified_as```. Traverse the array and look for an ```id ```of "http://vocab.getty.edu/aat/300404672", which has an equivalent value of "Sorting Name" in the ```_label ``` key.
```json
  "produced_by": {
    "type": "Production",
    "part": [
      {
       "type": "Production",
        "carried_out_by": [
          {
            "id": "urn:uuid:ae4da937-80f3-4ee1-9a93-fc1408983952",
            "type": "Person",
            "identified_by": [
              {
                "type": "Name",
                "content": "zzzzzzzzzUnknown",
                "classified_as": [
                  {
                    "id": "http://vocab.getty.edu/aat/300404672",
                    "type": "Type",
                    "_label": "Sorting Name"
                  }
                ]
              }
          ],
...
``` 
- **agents_agent_role_label** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, traverse the inner object and then through the arrays.  Match the ```type``` key equal to "Person" or "Group". Once you find that value, go back up on level and look for the ```classified_as``` array. Traverse the array and grab the  ```_label``` key.
```json
"produced_by": {
  "type": "Production",
  "part": [
    {
      "type": "Production",
       "carried_out_by": [
        {
          "id": "urn:uuid:68878b8c-e08d-47cb-84e7-aecb9a4c79b4",
          "type": "Group",
          "identified_by": [
            {
              "type": "Name",
              "content": "Rome"
            }
          ],
          ...
        }
      ],
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300205362",
          "type": "Type",
          "_label": "mint"
        }
      ]
...
```
- **agents_agent_role_code** Same as ***agents_agent_role_label***.
- **agents_agent_type_display** Same as ***agents_agent_role_label***.
- **agents_agent_culture_display** Look through the object where the ```type``` key equals "Group". Check the ```classified_as``` arraay to verify that it has an object with either an ```id``` of "http://vocab.getty.edu/aat/300387171" or a ```_label``` of "Culture". At the same level as ```classified_as```, there should be a ```identified_by``` key. Traverse that array and find the object with ```type``` equal to the "Name" value. Pull the value from the ```content``` key.
```json
  "shows": [
    {
      "id": "http://lux.yale.edu/visual/yuag:99389",
      "type": "VisualItem",
      "about": [
        {
          "type": "Group",
          "classified_as": [
            {
              "id": "http://vocab.getty.edu/aat/300387171",
              "type": "Type",
              "_label": "Culture"
            }
          ],
          "identified_by": [
            {
              "type": "Name",
              "content": "Roman"
            }
          ]
        },
...
```
- **agents_agent_context_display** Not implemented.
- **places_place_display** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, look for the ```took_place_at``` or ```used_for``` key. Traverse the array and find the ```identified_by``` key.  Traverse that array and grab the ```content``` key. 
```json
    "produced_by": {
        "took_place_at": [{
            "id": "urn:uuid:uid:0fca0d36-6ffa-4e64-b461-cec17fd3f20f",
            "type": "Place",
            "identified_by": [{
                "type": "Name",
                "content": "Rome"
            }],
...
```
- **places_place_role_label** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, look for the ```took_place_at``` or ```used_for``` key. Traverse the array  and look for the ```classified_as``` array. Traverse the array and grab the  ```_label``` key.
- **places_place_role_code** Not implemented.
- **places_place_type_display** Not implemented.
- **places_place_coordinates_type** Not implemented.
- **dates_date_display** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, looks for the key ```timespan```.  Look in that key for the ```identified_by``` array. Traverse the array and grab the ```content``` label.
```json
    "produced_by": {
        "type": "Production",
        "timespan": {
            "type": "TimeSpan",
            "begin_of_the_begin": "-45-01-01T00:00:00Z",
            "end_of_the_end": "-45-01-01T00:00:00Z",
            "identified_by": [{
                "type": "Name",
                "content": "45 B.C.",
                ...
            }]
...
```
- **dates_date_earliest** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, look for the key ```timespan```.  Look in that key for the ```begin_of_the_begin``` key.
```json
    "produced_by": {
        "type": "Production",
        "timespan": {
            "type": "TimeSpan",
            "begin_of_the_begin": "-45-01-01T00:00:00Z",
            "end_of_the_end": "-45-01-01T00:00:00Z",
            ...
        }
...
```
- **dates_date_latest** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, look for the key ```timespan```.  Look in that key for the ```end_of_the_end``` key.
```json
    "produced_by": {
        "type": "Production",
        "timespan": {
            "type": "TimeSpan",
            "begin_of_the_begin": "-45-01-01T00:00:00Z",
            "end_of_the_end": "-45-01-01T00:00:00Z",
            ...
        }
...
```
- **dates_date_role_label** This will be in a number of different places. Look through the whole object for the following keys:
  - ```created_by```
  - ```produced_by```
  - ```encountered_by```
  - ```removed_by```
  - ```destroyed_by```
  - ```removed_by```
Once you have matched on one of those keys, looks for the key ```timespan```.  Inside of that key, find the ```identified_by``` key array. Traverse the array and grab the ```content``` value. 
```json
  "timespan": {
    "type": "TimeSpan",
    "begin_of_the_begin": "-42-01-01T00:00:00Z",
    "end_of_the_end": "-42-01-01T00:00:00Z",
    "identified_by": [{
      "type": "Name",
      "content": "42 B.C.",
      ...
    }]
  ...
  }
```
- **dates_date_role_code** Not implemented.
- **dates_year_earliest** Derive from ***dates_date_earliest***.
- **dates_year_latest** Derive from ***dates_date_latest***.
- **subjects_subject_heading_sortname** Not implemented.
- **subjects_subject_heading_display** Look for the ```about``` array key. Loop through the array and look for the ```identified_by``` key.  Loop through the array and grab the ```content``` key.
```json
 "about": [
    {
      "type": "Type",
      "identified_by": [
        {
          "type": "Name",
          "content": "Education--Colombia"
        }
      ],
      ...
```
- **subjects_subject_facets_facet_display** Look for the ```about``` array key. Loop through the array and look for the ```created_by``` key.  Loop through the array and grab the ```influenced_by``` array. Loop through the ```influenced_by``` and grab the ```identified_by``` array.  Loop through the ```identified_by``` array and grab the ```content``` key.
```json
"about": [
    {
      "type": "Type",
      ...
      "created_by": {
        "type": "Creation",
        "influenced_by": [
          {
            "id": "urn:uuid:413ede13-db2c-484f-b778-d21dcea3c2ab",
            "type": "Type",
            "identified_by": [
              {
                "type": "Name",
                "content": "Education"
              }
            ]
          },
```
- **subjects_subject_facets_facet_type** This is now combined with subjects_subject_facets_facet_type_label. It is no longer a separate field.
- **subjects_subject_facets_facet_type_label**  Look for the ```about``` array key. Loop through the array and look for the ```created_by``` key.  Loop through the array and grab the ```influenced_by``` array. Loop through the ```influenced_by``` and grab the ```type``` key.
```json
"about": [
    {
      "type": "Type",
      ...
      "created_by": {
        "type": "Creation",
        "influenced_by": [
          {
            "id": "urn:uuid:413ede13-db2c-484f-b778-d21dcea3c2ab",
            "type": "Type",
           ...
          },
```
- **subjects_subject_facets_facet_role_code** Not implemented.
- **subjects_subject_facets_facet_coordinates_display** Not implemented.
- **subjects_subject_facets_facet_coordinates_type** Not implemented.
- **locations_campus_division**
- **locations_yul_holding_institution**
- **locations_collections**
- **locations_access_in_repository** Not implemented.
- **locations_access_in_repository_display** Look for the ```referred_to_by``` key. Traverse the array and look in each object's ```classified_as``` key. Check that the ```id``` key is equal to "http://vocab.getty.edu/aat/300133046" or the ```_label``` key is equal to the "Access Statement" value. If it matches, go up one level and grab the ```content``` key.
```json
    "referred_to_by": [ 
      {
        "type": "LinguisticObject",
        "content": "by appointment, Pratt Study Room",
        "classified_as": [
          {
            "id": "http://vocab.getty.edu/aat/300133046",
            "type": "Type",
            "_label": "Access Statement",
            ...
          }
        ]
      }
    ]
...          
```
- **locations_access_contact_in_repository** Not implemented.
- **locations_location_call_number** Look for the ```identified_by``` array. Loop through each item in the array and look for the ```classified_as``` array. Loop through the ```classified_as``` array and check if the ```_label``` key is "Call Number". If it matches, go up one level and grab the ```content``` key.
```json
"identified_by:" [
  {
      "type": "Identifier",
      "content": "LA566 G66",
      "classified_as": [
        {
          "id": "http://vocab.getty.edu/aat/300311706",
          "type": "Type",
          "_label": "Call Number"
        }
      ],
  }
]
```
- **rights_original_rights_status_display** Not implemented.
- **rights_original_rights_copyright_credit_display** Not implemented.
- **rights_original_rights_notes**  TBD.
- **rights_original_rights_type** Not implemented.
- **rights_original_rights_type_label** Not implemented.
- **digital_assets_asset_rights_status_display**
- **digital_assets_asset_rights_notes** Not implemented.
- **digital_assets_asset_rights_type** Not implemented.
- **digital_assets_asset_rights_type_label** Not implemented.
- **digital_assets_asset_type** Not implemented.
- **digital_assets_asset_caption_display** Look for the root ```representation``` key array. Traverse the array and look for the ```digitally_shown_by``` array. Traverse the array and find the ```referred_to_by``` array.  Traverse that array and find ```classified_as``` key. Traverse that array and check if the ```id``` equals "http://vocab.getty.edu/aat/300418049" or the ```_label``` equals "Description". If it matches, go up one level and grab the ```content``` key.
```json
 "representation": [
    {
      "type": "VisualItem",
      "digitally_shown_by": [
        {
          "type": "DigitalObject",
          ...
          "referred_to_by": [
            {
              "type": "LinguisticObject",
              "content": "cropped to image",
              "classified_as": [
                {
                  "id": "http://vocab.getty.edu/aat/300411780",
                  "type": "Type",
                  "_label": "Description",
                  "classified_as": [
                    {
                      "id": "http://vocab.getty.edu/aat/300418049",
                      "type": "Type",
                      "_label": "Brief Text"
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    },
...
``` 
- **hierarchies_hierarchy_type** TBD.
- **hierarchies_root_internal_identifier** Not implemented.
- **hierarchies_descendant_count** Not implemented.
- **hierarchies_maximum_depth** Not implemented.
- **hierarchies_sibling_count** Not implemented.
- **hierarchies_ancestor_internal_identifiers** Not implemented.
- **hierarchies_ancestor_display_names** Not implemented.
- **citations_citation_string_display**  Look for the ```referred_to_by``` key. Traverse the array and look in each object's ```classified_as``` key. Check that the ```id``` key is equal to "http://vocab.getty.edu/aat/300311705" or the ```_label``` key is equal to the "Citation" value. If it matches, go up one level and grab the ```content``` key.
```json
    "referred_to_by": [ 
      {
        "type": "LinguisticObject",
        "content": "Michael H. Crawford, Roman Republican Coinage (Cambridge: Cambridge University Press, 1974), no. 474/1a.",
        "classified_as": [{
            "id": "http://vocab.getty.edu/aat/300311705",
            "type": "Type",
            "_label": "Citation",
...
```
- **citations_citation_identifier_value** Not implemented.
- **citations_citation_identifier_type** Not implemented.
- **citations_citation_type** Not implemented.