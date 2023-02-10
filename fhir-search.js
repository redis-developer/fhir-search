/**
 * @maker Joey Whelan
 * @fileoverview Series of Redis Search and Aggregate scenarios
 */

import { AggregateGroupByReducers, AggregateSteps, createClient, SchemaFieldTypes } from 'redis';
import { ArgumentParser } from 'argparse';
import fsPromises from 'node:fs/promises';
const FHIR_DIR = './synthea/output/fhir'
const REDIS_URL = 'redis://localhost:12000'

class FHIR {
    constructor(args) {
        this.url = args.url;
    }

    async connect() {
        this.client = createClient({url: this.url});
        this.client.on('error', (err) => {
            console.error(err.message);
        });  
        await this.client.connect();
    }

    async disconnect() {
        await this.client.disconnect();
    }

    async buildIndices() {
        console.log('*** Build indices ***');
        let count = 0;

        // location index
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
        count++;

        // practitionerRole index
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
        count++;

        // medicationRequest index
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
        count++;

        // immunization index
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
        count++;

        // condition index
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
        count++;

        // claims index
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
        count++;
        console.log(`${count} indices built.`);
    }

    async loadDocs() {
        console.log('\n*** Load documents ***')
        let count = 0;
        let files = await fsPromises.readdir(FHIR_DIR);
        files = files.filter(file => file.endsWith(('.json')));
        for (const file of files) {
            const data = await fsPromises.readFile(`${FHIR_DIR}/${file}`);
            const bundle = JSON.parse(data);
            for (const entry of bundle.entry) {
                const key = `${entry.resource.resourceType}:${entry.resource.id}`  
                await this.client.json.set(key, '.', entry.resource);  
                count++;  
            }
        }   
        console.log(`${count} documents loaded.`);
    }

    async runScenarios() {
        console.log('\n*** Search Scenarios ***');
        
        console.log('\n**** Location Scenario 1 ****');
        // Business Problem - Find 3 medical facilities in Alaska
        let result = await this.client.ft.search('location_idx', '(@status:{active} @state:{AK})', {
            RETURN: ['$.name', '$.address.city'],
            LIMIT: {from: 0, size: 3}
        });
        console.log(JSON.stringify(result.documents,null,4));

        console.log('\n**** Location Scenario 2 ****');
        // Business Problem - Find the closest medical facility (in the database) to Woodland Park CO
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
        console.log(JSON.stringify(result.results,null,4));

        console.log('\n**** PractionerRole Scenario 1 ****')
        // Business Problem - Find 3 General Practice physicians that work from a hospital
        result = await this.client.ft.search('practitionerRole_idx', '(@specialty:"General Practice" @location:hospital)', {
            RETURN: ['$.practitioner.display'],
            LIMIT: {from: 0, size: 3}
        });
        console.log(JSON.stringify(result.documents,null,4));

        console.log('\n**** PractitionerRole Scenario 2 ****');
        // Business Problem - Find the count of physicians per medical specialty
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
        console.log(JSON.stringify(result.results,null,4));

        console.log('\n**** MedicationRequest Scenario 1 ****')
        // Business Problem - Find the names of 3 medications that have been currently prescribed for bronchitis
        result = await this.client.ft.search('medicationRequest_idx', '(@status:{active} @reason:%bronchitis%)', {
            RETURN: ['$.medicationCodeableConcept.text'],
            LIMIT: {from: 0, size: 3}
        });
        console.log(JSON.stringify(result.documents,null,4));

        console.log('\n**** MedicationRequest Scenario 2 ****')
        // Business Problem - Find the top 3 physicians by prescription count who are prescribing opioids
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
        console.log(JSON.stringify(result.results,null,4));

        console.log('\n**** Immunization Scenario 1 ****')
        // Business Problem - Find 5 patients that received an immunizations at an urgent care clinic in 2015
        result = await this.client.ft.search('immunization_idx', '@location:urgent @date:2015*', {
            RETURN: ['$.patient.reference'],
            LIMIT: {from: 0, size: 5}
        });
        console.log(JSON.stringify(result.documents,null,4));


        console.log('\n**** Immunization Scenario 2 ****')
        // Business Problem - Find the top 5 vaccines administered in 2020.
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
        console.log(JSON.stringify(result.results,null,4));

        console.log('\n**** Condition Scenario 1 ****')
        // Business Problem - Find 3 patients with active cases of rhinitis or asthma
        result = await this.client.ft.search('condition_idx', '@code:{active} @problem:(rhinitis|asthma)', {
            RETURN: ['$.subject.reference'],
            LIMIT: {from: 0, size: 3}
        });
        console.log(JSON.stringify(result.documents,null,4));

        console.log('\n**** Condition Scenario 2 ****')
        // Business Problem - Find the count of reported medical conditions categorized by year
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
        console.log(JSON.stringify(result.results,null,4));

        console.log('\n**** Claims Scenario 1 ****')
        // Business Problem - Find the service description of 3 active claims where Aetna is the insurer and the claim value is greater than $1000
        result = await this.client.ft.search('claims_idx', '@insurer:Aetna @value:[1000,+inf] @status:{active}', {
            RETURN: ['$.item[0].productOrService.text'],
            LIMIT: {from: 0, size: 3}
        });
        console.log(JSON.stringify(result.documents,null,4));

        console.log('\n**** Claims Scenario 2 ****')
        // Business Problem - Find the Top 3 insurers by claim value
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
        console.log(JSON.stringify(result.results,null,4));
    }
}

(async () => {
    
    const parser = ArgumentParser({ description: 'FHIR Search Examples' });
    parser.add_argument('--url', {
            required: false,
            type: 'str',
            help: 'Redis URL connect string',
            default: REDIS_URL
    });
    const args = parser.parse_args();
    const fhir = new FHIR(args);
    await fhir.connect();
    await fhir.buildIndices();
    await fhir.loadDocs();
    await fhir.runScenarios();
    await fhir.disconnect();
})();