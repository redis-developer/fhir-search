# FHIR Search Examples  

## Blog
https://joeywhelan.blogspot.com/2022/12/redis-search-with-fhir-data.html


## Contents
1.  [Summary](#summary)
2.  [Features](#features)
3.  [Prerequisites](#prerequisites)
4.  [Installation](#installation)
5.  [Usage](#usage)
6.  [Search Scenarios](#search_scenarios)
    1.  [FHIR Location Resource](#location_resource)
        1.  [Index](#location_index)
        2.  [Scenario 1](#location_scenario1)
        3.  [Scenario 2](#location_scenario2)
    2.  [FHIR PractitionerRole Resource](#practitionerRole_resource)
        1.  [Index](#practitionerRole_index)
        2.  [Scenario 1](#practitionerRole_scenario1)
        3.  [Scenario 2](#practitionerRole_scenario2)
    3.  [FHIR MedicationRequest Resource](#medicationRequest_resource)
        1.  [Index](#medicationRequest_index)
        2.  [Scenario 1](#medicationRequest_scenario1)
        3.  [Scenario 2](#medicationRequest_scenario2)
    4.  [FHIR Immunization Resource](#immunization_resource)
        1.  [Index](#immunization_index)
        2.  [Scenario 1](#immunization_scenario1)
        3.  [Scenario 2](#immunization_scenario2)
    5.  [FHIR Condition Resource](#condition_resource)
        1.  [Index](#condition_index)
        2.  [Scenario 1](#condition_scenario1)
        3.  [Scenario 2](#condition_scenario2)
    6.  [FHIR Claim Resource](#claim_resource)
        1.  [Index](#claim_index)
        2.  [Scenario 1](#claim_scenario1)
        3.  [Scenario 2](#claim_scenario2)

## Summary <a name="summary"></a>
This is collection of shell scripts and a python app that will build synthetic FHIR data, a Redis Enterprise deployment in Docker, and load that data into Redis.  Subsequently there are multiple advanced Redis search query examples in CLI, JavaScript and Python formats.


## Features <a name="features"></a>
- Builds bundles of synthetic FHIR data for each state in the US
- Builds a 3-node Redis Enterprise cluster
- Utilizes RedisJSON to load that FHIR data into the Redis cluster.
- Implements multiple search and aggregation operations against Redis.  

## Prerequisites <a name="prerequisites"></a>
- Docker Compose
- Python
- Java
- Nodejs

## Installation <a name="installation"></a>
1. Clone this repo.

2. Go to synthea folder and execute the load script.  This will create a directory (output) with FHIR patient bundles
for every state in the US.
```bash
cd synthea
./run.sh
```
3. Go to the redis folder and execute the build script.  This will create a 3-node Redis Enterprise cluster and build a db to house the FHIR data.
```bash
cd redis
./run.sh
```

4.  Install Python requirements
```bash
pip install -r requirements.txt
```

5.  Install JavaScript requirements
```bash
npm install
```

## Usage <a name="usage"></a>
### Options
- --url. Redis connection string.  Default = redis://localhost:12000
### Execution
#### JavaScript
```bash
node fhir-search.js
```
#### Python
```bash
python3 fhir-search.py
```

## Search Scenarios <a name="search_scenarios"></a>

### [FHIR Location Resource](https://www.hl7.org/fhir/location.html) <a name="location_resource"></a>

#### **Index** <a name="location_index"></a>
#### CLI
```bash
ft.create location_idx on json prefix 1 Location: schema $.status as status TAG $.name as name TEXT $.address.city as city TAG $.address.state as state TAG $.position.longitude as longitude NUMERIC $.position.latitude as latitude NUMERIC
```
#### JavaScript
```javascript
        await this.client.ft.create('location_idx', {
            '$.status': {
                type: SchemaFieldTypes.TAG,
                AS: 'status'  
            },
            '$.name': {
                type: SchemaFieldTypes.TEXT,
                AS: 'name'
            },
            '$.address.city': {
                type: SchemaFieldTypes.TAG,
                AS: 'city'
            },
            '$.address.state': {
                type: SchemaFieldTypes.TAG,
                AS: 'state'
            },
            '$.position.longitude': {
                type: SchemaFieldTypes.NUMERIC,
                AS: 'longitude'
            },
            '$.position.latitude': {
                type: SchemaFieldTypes.NUMERIC,
                AS: 'latitude'
            }
        }, { ON: 'JSON', PREFIX: 'Location:'});
```
#### Python
```python
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Location:'])
        schema = [  TagField('$.status', as_name='status'),
            TextField('$.name', as_name='name'),
            TagField('$.address.city', as_name='city'),
            TagField('$.address.state', as_name='state'),
            NumericField('$.position.longitude', as_name='longitude'),
            NumericField('$.position.latitude', as_name='latitude')
        ]
        connection.ft('location_idx').create_index(schema, definition=idx_def)
```
#### **Location Scenario 1** <a name="location_scenario1"></a>
#### Business Problem
Find 3 medical facilities in Alaska
#### CLI
```bash
ft.search location_idx '(@status:{active} @state:{AK})' return 2 $.name $.address.city limit 0 3
```
#### JavaScript
```javascript
        result = await this.client.ft.search('location_idx', '(@status:{active} @state:{AK})', {
            RETURN: ['$.name', '$.address.city'],
            LIMIT: {from: 0, size: 3}
        });
```

#### Python
```python
        query = Query('(@status:{active} @state:{AK})').return_fields('$.name', '$.address.city').paging(0,3)
        result = connection.ft('location_idx').search(query)
```
#### Results
```bash
[Document {'id': 'Location:05fa672f-7eca-3f66-af60-1d5c640c5c26', 'payload': None, '$.name': 'PROVIDENCE TRANSITIONAL CARE CENTER', '$.address.city': 'ANCHORAGE'}, Document {'id': 'Location:12bbaf94-87f8-35ce-b715-b3d649aeb0d1', 'payload': None, '$.name': 'SOUTHEAST ALASKA REGIONAL HEALTH CONSORTIUM', '$.address.city': 'YAKUTAT'}, Document {'id': 'Location:568be154-f4b7-370a-bfab-024bcf10467f', 'payload': None, '$.name': 'YAKUTAT TLINGIT TRIBE', '$.address.city': 'YAKUTAT'}]
```  

#### **Location Scenario 2** <a name="location_scenario2"></a>
#### Business Problem
Find the closest, active medical facility (in the database) to Woodland Park CO
#### CLI
```bash
ft.aggregate location_idx '@status:{active}' LOAD 5 @name @city @state @longitude @latitude APPLY 'geodistance(@longitude, @latitude, -105.0569, 38.9939)' AS meters APPLY 'ceil(@meters*0.000621371)' as miles sortby 2 @miles ASC limit 0 1
```
#### JavaScript
```javascript
        result = await this.client.ft.aggregate('location_idx','@status:{active}', {
            LOAD: ['@name', '@city', '@state', '@longitude', '@latitude'],
            STEPS: [
                    {   type: AggregateSteps.APPLY,
                        expression: 'geodistance(@longitude, @latitude, -105.0569, 38.9939)', 
                        AS: 'meters' 
                    },
                    {   type: AggregateSteps.APPLY ,
                        expression: 'ceil(@meters*0.000621371)', 
                        AS: 'miles' 
                    },
                    {
                        type: AggregateSteps.SORTBY,
                        BY: {
                            BY: '@miles', 
                            DIRECTION: 'ASC' 
                        }
                    },
                    {
                        type: AggregateSteps.LIMIT,
                        from: 0, 
                        size: 1
                    }
            ]
        });
```
#### Python
```python
        request = AggregateRequest('@status:{active}')\
        .load('@name', '@city', '@state', '@longitude', '@latitude')\
        .apply(meters='geodistance(@longitude, @latitude, -105.0569, 38.9939)')\
        .apply(miles='ceil(@meters*0.000621371)')\
        .sort_by(Asc('@miles'))\
        .limit(0,1)
        result = connection.ft('location_idx').aggregate(request)
```
#### Results
```bash
[[b'name', b'ARETI COMPREHENSIVE PRIMARY CARE', b'city', b'COLORADO SPRINGS', b'state', b'CO', b'longitude', b'-104.768591624', b'latitude', b'38.9006726282', b'meters', b'27009.43', b'miles', b'17']]
```  

### [FHIR PractitionerRole Resource](https://www.hl7.org/fhir/practitionerrole.html) <a name="practitionerRole_resource"></a>
#### **Index** <a name="practitionerRole_index"></a>
#### CLI
```bash
ft.create practitionerRole_idx on json prefix 1 PractitionerRole: schema $.practitioner.display as physician TEXT $.specialty[*].text as specialty TEXT SORTABLE $.location[*].display as location TEXT
```
#### Javascript
```javascript
        await this.client.ft.create('practitionerRole_idx', {
            '$.practitioner.display': {
                type:  SchemaFieldTypes.TEXT,
                AS: 'physician'
            },
            '$.specialty[*].text': {
                type: SchemaFieldTypes.TEXT,
                AS: 'specialty',
                SORTABLE: true
            },
            '$.location[*].display': {
                type: SchemaFieldTypes.TEXT,
                AS: 'location'
            }
        }, {ON: 'JSON', PREFIX: 'PractitionerRole:'});
```
#### Python
```python
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['PractitionerRole:'])
        schema = [  TextField('$.practitioner.display', as_name='physician'),
            TextField('$.specialty[*].text', as_name='specialty', sortable=True),
            TextField('$.location[*].display', as_name='location')
        ]
        connection.ft('practitionerRole_idx').create_index(schema, definition=idx_def)
```
#### **PractitionerRole Scenario 1** <a name="practitionerRole_scenario1"></a>
#### Business Problem
Find 3 General Practice physicians that work from a hospital
#### CLI
```bash
ft.search practitionerRole_idx '(@specialty:"General Practice" @location:hospital)' return 1 $.practitioner.display limit 0 3
```
#### JavaScript
```javascript
        result = await this.client.ft.search('practitionerRole_idx', '(@specialty:"General Practice" @location:hospital)', {
            RETURN: ['$.practitioner.display'],
            LIMIT: {from: 0, size: 3}
        });
```
#### Python
```python
        query = Query('(@specialty:"General Practice" @location:hospital)').return_fields('$.practitioner.display').paging(0,3)
        result = connection.ft('practitionerRole_idx').search(query)
```
#### Results
```bash
[Document {'id': 'PractitionerRole:15de72f0-ac59-8686-5f1f-fcad194b41cf', 'payload': None, '$.practitioner.display': 'Dr. Blythe746 Heller342'}, Document {'id': 'PractitionerRole:b934db3f-23d1-94c0-9f3e-4acfca3939be', 'payload': None, '$.practitioner.display': 'Dr. Leoma634 Jaskolski867'}, Document {'id': 'PractitionerRole:479ed146-1b2a-e61c-e095-88c1c3142a70', 'payload': None, '$.practitioner.display': 'Dr. Fernando603 Zamudio115'}]
```  

#### **PractitionerRole Scenario 2** <a name="practitionerRole_scenario2"></a>
#### Business Problem
Find the count of physicians per medical specialty
#### CLI
```bash
ft.aggregate practitionerRole_idx * groupby 1 @specialty reduce count 0 as count
```
#### JavaScript
```javascript
        result = await this.client.ft.aggregate('practitionerRole_idx', '*', {
            STEPS: [
                {   type: AggregateSteps.GROUPBY,
                    properties: ['@specialty'],
                    REDUCE: [
                        {   type: AggregateGroupByReducers.COUNT,
                            property: '@speciality',
                            AS: 'count'
                        }
                    ]   
                }
            ]
        })
```
#### Python
```python
        request = AggregateRequest('*').group_by('@specialty', reducers.count().alias('count'))     
        result = connection.ft('practitionerRole_idx').aggregate(request)
```
#### Results
```bash
[[b'specialty', b'General Practice', b'count', b'1489']]
```  

### [FHIR MedicationRequest Resource](https://www.hl7.org/fhir/medicationrequest.html) <a name="medicationRequest_resource"></a>
#### **Index** <a name="medicationRequest_index"></a>
#### CLI
```bash
ft.create medicationRequest_idx on json prefix 1 MedicationRequest: schema $.status as status TAG $.medicationCodeableConcept.text as drug TEXT $.requester.display as prescriber TEXT SORTABLE $.reasonReference[*].display as reason TEXT
```
#### JavaScript
```javascript
        await this.client.ft.create('medicationRequest_idx', {
            '$.status': {
                type: SchemaFieldTypes.TAG,
                AS: 'status'
            },
            '$.medicationCodeableConcept.text': {
                type: SchemaFieldTypes.TEXT,
                AS: 'drug'
            },
            '$.requester.display': {
                type: SchemaFieldTypes.TEXT,
                AS: 'prescriber',
                SORTABLE: true
            },
            '$.reasonReference[*].display': {
                type: SchemaFieldTypes.TEXT,
                AS: 'reason'
            }
        }, {ON: 'JSON', PREFIX: 'MedicationRequest:'});
```
#### Python
```python
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['MedicationRequest:'])
        schema = [  TagField('$.status', as_name='status'),
            TextField('$.medicationCodeableConcept.text', as_name='drug'),
            TextField('$.requester.display', as_name='prescriber', sortable=True),
            TextField('$.reasonReference[*].display', as_name='reason')
        ]
        connection.ft('medicationRequest_idx').create_index(schema, definition=idx_def)
```
#### **MedicationRequest Scenario 1** <a name="medicationRequest_scenario1"></a>
#### Business Problem
Find the names of 3 medications that have been currently prescribed for bronchitis
#### CLI
```bash
ft.search medicationRequest_idx '@status:{active} @reason:%bronchitis%' return 1 $.medicationCodeableConcept.text limit 0 3
```
#### JavaScript
```javascript
        result = await this.client.ft.search('medicationRequest_idx', '(@status:{active} @reason:%bronchitis%)', {
            RETURN: ['$.medicationCodeableConcept.text'],
            LIMIT: {from: 0, size: 3}
        });
```
#### Python
```python
        query = Query('(@status:{active} @reason:%bronchitis%)').return_fields('$.medicationCodeableConcept.text').paging(0,3)
        result = connection.ft('medicationRequest_idx').search(query)
```
#### Results
```bash
[Document {'id': 'MedicationRequest:2059a952-4e36-8741-8196-e6c6ff82f94b', 'payload': None, '$.medicationCodeableConcept.text': 'albuterol 5 MG/ML Inhalation Solution'}, Document {'id': 'MedicationRequest:4b31b9c0-4491-236a-164b-60c1de6026c3', 'payload': None, '$.medicationCodeableConcept.text': 'albuterol 5 MG/ML Inhalation Solution'}, Document {'id': 'MedicationRequest:e58ca28f-9d1f-403a-43a8-b06e267a8eba', 'payload': None, '$.medicationCodeableConcept.text': 'Acetaminophen 325 MG Oral Tablet'}]
```
#### **MedicationRequest Scenario 2** <a name="medicationRequest_scenario2"></a>
#### Business Problem
Find the top 3 physicians by prescription count who are prescribing opioids
#### CLI
```bash
ft.aggregate medicationRequest_idx '@drug:(Hydrocodone|Oxycodone|Oxymorphone|Morphine|Codeine|Fentanyl|Hydromorphone|Tapentadol|Methadone)' groupby 1 @prescriber reduce count 0 as opioids_prescribed sortby 2 @opioids_prescribed DESC limit 0 3
```
#### Javascript
```javascript
        const opioids = 'Hydrocodone|Oxycodone|Oxymorphone|Morphine|Codeine|Fentanyl|Hydromorphone|Tapentadol|Methadone';
        result = await this.client.ft.aggregate('medicationRequest_idx', `@drug:${opioids}`, {
            STEPS: [
                {   type: AggregateSteps.GROUPBY,
                    properties: ['@prescriber'],
                    REDUCE: [
                        {   type: AggregateGroupByReducers.COUNT,
                            property: '@prescriber',
                            AS: 'opioids_prescribed'
                        }
                    ]   
                },
                {
                    type: AggregateSteps.SORTBY,
                    BY: { 
                        BY: '@opioids_prescribed', 
                        DIRECTION: 'DESC' 
                    }
                },
                {
                    type: AggregateSteps.LIMIT,
                    from: 0, 
                    size: 3
                }
            ]
        });
```
#### Python
```python
        opioids = 'Hydrocodone|Oxycodone|Oxymorphone|Morphine|Codeine|Fentanyl|Hydromorphone|Tapentadol|Methadone'
        request = AggregateRequest(f'@drug:{opioids}')\
        .group_by('@prescriber', reducers.count().alias('opioids_prescribed'))\
        .sort_by(Desc('@opioids_prescribed'))\
        .limit(0,3)
        result = connection.ft('medicationRequest_idx').aggregate(request)
```
#### Results
```bash
[[b'prescriber', b'Dr. Aja848 McKenzie376', b'opiods_prescribed', b'53'], [b'prescriber', b'Dr. Jaquelyn689 Bernier607', b'opiods_prescribed', b'52'], [b'prescriber', b'Dr. Aurora248 Kessler503', b'opiods_prescribed', b'49']]
```    

### [FHIR Immunization Resource](https://www.hl7.org/fhir/immunization.html) <a name="immunization_resource"></a>
#### **Index** <a name="immunization_index"></a>
#### CLI
```bash
ft.create immunization_idx on json prefix 1 Immunization: schema $.vaccineCode.text as vax TEXT SORTABLE $.location.display as location TEXT $.occurrenceDateTime as date TEXT
```
#### JavaScript
```javascript
        await this.client.ft.create('immunization_idx', {
            '$.vaccineCode.text': {
                type: SchemaFieldTypes.TEXT,
                AS: 'vax',
                SORTABLE: true
            },
            '$.location.display': {
                type: SchemaFieldTypes.TEXT,
                AS: 'location'
            },
            '$.occurrenceDateTime': {
                type: SchemaFieldTypes.TEXT,
                AS: 'date'
            }
        }, {ON: 'JSON', PREFIX: 'Immunization:'});
```
#### Python
```python
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Immunization:'])
        schema = [  TextField('$.vaccineCode.text', as_name='vax', sortable=True),
            TextField('$.location.display', as_name='location'),
            TextField('$.occurrenceDateTime', as_name='date')
        ]
        connection.ft('immunization_idx').create_index(schema, definition=idx_def)
```
#### **Immunization Scenario 1** <a name="immunization_scenario1"></a>
#### Business Problem
Find 5 patients that received an immunizations at an urgent care clinic in 2015
#### CLI
```bash
ft.search immunization_idx '@location:urgent @date:2015*' return 1 $.patient.reference limit 0 5
```
#### JavaScript
```javascript
        result = await this.client.ft.search('immunization_idx', '@location:urgent @date:2015*', {
            RETURN: ['$.patient.reference'],
            LIMIT: {from: 0, size: 5}
        });
```
#### Python
```python
        query = Query('@location:urgent @date:2015*').return_fields('$.patient.reference').paging(0,5)
        result = connection.ft('immunization_idx').search(query)
```
#### Results
```bash
[Document {'id': 'Immunization:ec1d62eb-d63a-5849-41a3-543169d78485', 'payload': None, '$.patient.reference': 'urn:uuid:4302aff5-e1c1-6f8b-dc16-ffbda9d42543'}, Document {'id': 'Immunization:3fe54e36-8738-ca25-fa56-2ed44db03c9e', 'payload': None, '$.patient.reference': 'urn:uuid:4bb9f61c-1a53-ece9-699f-96641917cd27'}, Document {'id': 'Immunization:e670d5a7-f8e3-020d-8e25-66390ced7b17', 'payload': None, '$.patient.reference': 'urn:uuid:d75a70a5-8111-4425-433e-fe8c5ea7f7f4'}, Document {'id': 'Immunization:add61ed6-9b55-c337-e865-f76f352d999d', 'payload': None, '$.patient.reference': 'urn:uuid:d75a70a5-8111-4425-433e-fe8c5ea7f7f4'}, Document {'id': 'Immunization:17da6d63-1a91-4c00-6699-08cb009f47a8', 'payload': None, '$.patient.reference': 'urn:uuid:0d3b9fe9-1ae8-2b4c-4282-f1070bd3d978'}]
```
#### **Immunization Scenario 2** <a name="immunization_scenario2"></a>
#### Business Problem
Find the top 5 vaccines administered in 2020
#### CLI
```bash
ft.aggregate immunization_idx '@date:2020*' groupby 1 @vax reduce count 0 as num_vax sortby 2 @num_vax DESC limit 0 5
```
#### Javascript
```javascript
        result = await this.client.ft.aggregate('immunization_idx', '@date:2020*', {
            STEPS: [{   
                type: AggregateSteps.GROUPBY,
                properties: ['@vax'],
                REDUCE: [{   
                    type: AggregateGroupByReducers.COUNT,
                    property: '@vax',
                    AS: 'num_vax'
                }]},
                {   type: AggregateSteps.SORTBY,
                    BY: { 
                    BY: '@num_vax', 
                    DIRECTION: 'DESC' 
                }},
                {   type: AggregateSteps.LIMIT,
                    from: 0, 
                    size: 5
                }
            ]
        });
```
#### Python
```python
        request = AggregateRequest('@date:2020*')\
        .group_by('@vax', reducers.count().alias('num_vax'))\
        .sort_by(Desc('@num_vax'))\
        .limit(0,5)
        result = connection.ft('immunization_idx').aggregate(request)
```
#### Results
```bash
[[b'vax', b'Influenza, seasonal, injectable, preservative free', b'num_vax', b'264'], [b'vax', b'DTaP', b'num_vax', b'27'], [b'vax', b'Hep B, adolescent or pediatric', b'num_vax', b'25'], [b'vax', b'Pneumococcal conjugate PCV 13', b'num_vax', b'25'], [b'vax', b'IPV', b'num_vax', b'24']]
```  

### [FHIR Condition Resource](https://www.hl7.org/fhir/condition.html) <a name="condition_resource"></a>
#### **Index** <a name="condition_index"></a>
#### CLI
```bash
ft.create condition_idx on json prefix 1 Condition: schema $.clinicalStatus.coding[*].code as code TAG $.code.text as problem TEXT $.recordedDate as date TAG
```
#### JavaScript
```javascript
        await this.client.ft.create('condition_idx', {
            '$.clinicalStatus.coding[*].code': {
                type: SchemaFieldTypes.TAG,
                AS: 'code'
            },
            '$.code.text': {
                type: SchemaFieldTypes.TEXT,
                AS: 'problem'
            },
            '$.recordedDate': {
                type: SchemaFieldTypes.TAG,
                AS: 'date'
            }
        }, {ON: 'JSON', PREFIX: 'Condition:'});
```
#### Python
```python
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Condition:'])
        schema = [  TagField('$.clinicalStatus.coding[*].code', as_name='code'),
            TextField('$.code.text', as_name='problem'),
            TagField('$.recordedDate', as_name='date')
        ]
        connection.ft('condition_idx').create_index(schema, definition=idx_def)
```
#### **Condition Scenario 1** <a name="condition_scenario1"></a>
#### Business Problem
Find 3 patients with active cases of rhinitis or asthma
#### CLI
```bash
ft.search condition_idx '@code:{active} @problem:(rhinitis|asthma)' return 1 $.subject.reference limit 0 3
```
#### JavaScript
```javascript
        result = await this.client.ft.search('condition_idx', '@code:{active} @problem:(rhinitis|asthma)', {
            RETURN: ['$.subject.reference'],
            LIMIT: {from: 0, size: 3}
        });
```
#### Python
```python
        query = Query('@code:{active} @problem:(rhinitis|asthma)').return_fields('$.subject.reference').paging(0, 3)
        result = connection.ft('condition_idx').search(query)
```
#### Results
```bash
[Document {'id': 'Condition:4f526fb1-9bf8-3be9-f523-979dd39933c4', 'payload': None, '$.subject.reference': 'urn:uuid:7bb4db4d-03e8-a1fc-1dea-fee694bf0251'}, Document {'id': 'Condition:6f7892fe-130b-2fe7-f0ce-98cf7aaf045b', 'payload': None, '$.subject.reference': 'urn:uuid:4efc9596-68af-f0c8-e9c3-bd9bfcbcef39'}, Document {'id': 'Condition:0f5f7bf7-799b-0b41-a2c3-f9caf0403e5c', 'payload': None, '$.subject.reference': 'urn:uuid:2e924baf-09ab-240a-9fd5-1bd4790fdc8b'}]
```
#### **Condition Scenario 2** <a name="condition_scenario2"></a>
#### Business Problem
Find the count of reported medical conditions categorized by year
#### CLI
```bash
ft.aggregate condition_idx * load 1 @date apply 'substr(@date,0,4)' as year groupby 1 @year reduce count 0 as num_conditions SORTBY 2 @year DESC limit 0 5
```
#### JavaScript
```javascript
        result = await this.client.ft.aggregate('condition_idx', '*', {
            LOAD: ['@date'],
            STEPS: [
                {   type: AggregateSteps.APPLY,
                    expression: 'substr(@date,0,4)', 
                    AS: 'year' 
                },   
                {   type: AggregateSteps.GROUPBY,
                    properties: ['@year'],
                    REDUCE: [{   
                        type: AggregateGroupByReducers.COUNT,
                        property: '@year',
                        AS: 'num_conditions'
                }]},
                {   type: AggregateSteps.SORTBY,
                    BY: { 
                    BY: '@year', 
                    DIRECTION: 'DESC' 
                }},
                {   type: AggregateSteps.LIMIT,
                    from: 0, 
                    size: 5
                }
            ]
        });
```
#### Python
```python
        request = AggregateRequest('*')\
        .load('@date')\
        .apply(year='substr(@date,0,4)')\
        .group_by('@year', reducers.count().alias('num_conditions'))\
        .sort_by(Desc('@year'))\
        .limit(0,5)
        result = connection.ft('condition_idx').aggregate(request)
```
#### Results
```bash
[[b'year', b'2022', b'num_conditions', b'557'], [b'year', b'2021', b'num_conditions', b'690'], [b'year', b'2020', b'num_conditions', b'719'], [b'year', b'2019', b'num_conditions', b'600'], [b'year', b'2018', b'num_conditions', b'571']]
```  

### [FHIR Claim Resource](https://www.hl7.org/fhir/claim.html) <a name="claim_resource"></a>
#### **Index** <a name="claim_index"></a>
#### CLI
```bash
ft.create claims_idx on json prefix 1 Claim: schema $.status as status tag $.insurance[*].coverage.display as insurer text SORTABLE $.total.value as value numeric
```
#### JavaScript
```javascript
        await this.client.ft.create('claims_idx', {
            '$.status': {
                type: SchemaFieldTypes.TAG,
                AS: 'status'
            },
            '$.insurance[*].coverage.display': {
                type: SchemaFieldTypes.TEXT,
                AS: 'insurer',
                SORTABLE: true    
            },
            '$.total.value': {
                type: SchemaFieldTypes.NUMERIC,
                AS: 'value'
            }
        }, {ON: 'JSON', PREFIX: 'Claim:'});
```
#### Python
```python
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Claim:'])
        schema = [  TagField('$.status', as_name='status'),
            TextField('$.insurance[*].coverage.display', as_name='insurer', sortable=True),
            NumericField('$.total.value', as_name='value')
        ]
        connection.ft('claims_idx').create_index(schema, definition=idx_def)
```
#### **Claim Scenario 1** <a name="claim_scenario1"></a>
#### Business Problem
Find the service description of 3 active claims where Aetna is the insurer and the claim value is greater than $1000
#### CLI
```bash
ft.search claims_idx '@insurer:Aetna @value:[1000,+inf] @status:{active}' return 1 $.item[0].productOrService.text limit 0 3
```
#### JavaScript
```javascript
        result = await this.client.ft.search('claims_idx', '@insurer:Aetna @value:[1000,+inf] @status:{active}', {
            RETURN: ['$.item[0].productOrService.text'],
            LIMIT: {from: 0, size: 3}
        });
```
#### Python
```python
        query = Query('@insurer:Aetna @value:[1000,+inf] @status:{active}').return_fields('$.item[0].productOrService.text').paging(0, 3)
        result = connection.ft('claims_idx').search(query)
```
#### Results
```bash
[Document {'id': 'Claim:182c810d-501b-ad59-0690-192b57cfa144', 'payload': None, '$.item[0].productOrService.text': 'Well child visit (procedure)'}, Document {'id': 'Claim:69caae8a-5d96-ae67-7b8d-364606823115', 'payload': None, '$.item[0].productOrService.text': 'Jolivette 28 Day Pack'}, Document {'id': 'Claim:726b4d60-1223-3931-efc3-f10d4f6186a4', 'payload': None, '$.item[0].productOrService.text': 'Errin 28 Day Pack'}]
```
#### **Claim Scenario 2** <a name="claim_scenario2"></a>
#### Business Problem
Find the Top 3 insurers by claim value
#### CLI
```bash
ft.aggregate claims_idx '@status:{active}' groupby 1 @insurer reduce sum 1 @value as total_value filter '@total_value > 0' sortby 2 @total_value DESC limit 0 3
```
#### JavaScript
```javascript
        result = await this.client.ft.aggregate('claims_idx', '@status:{active}', {
            STEPS: [
                {   type: AggregateSteps.GROUPBY,
                    properties: ['@insurer'],
                    REDUCE: [{   
                        type: AggregateGroupByReducers.SUM,
                        property: '@value',
                        AS: 'total_value'
                }]},
                {
                    type: AggregateSteps.FILTER,
                    expression: '@total_value > 0'
                },
                {   type: AggregateSteps.SORTBY,
                    BY: { 
                    BY: '@total_value', 
                    DIRECTION: 'DESC' 
                }},
                {   type: AggregateSteps.LIMIT,
                    from: 0, 
                    size: 5
                }
            ]
        });
```
#### Python
```python
        request = AggregateRequest('@status:{active}')\
        .group_by('@insurer', reducers.sum('@value').alias('total_value'))\
        .filter('@total_value > 0')\
        .sort_by(Desc('@total_value'))\
        .limit(0,3)
        result = connection.ft('claims_idx').aggregate(request)
```
#### Results
```bash
[[b'insurer', b'Medicare', b'total_value', b'29841923.54'], [b'insurer', b'NO_INSURANCE', b'total_value', b'9749265.48'], [b'insurer', b'UnitedHealthcare', b'total_value', b'8859141.59']]
```