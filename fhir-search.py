# Author Joey Whelan
from argparse import ArgumentParser
from redis import Connection, from_url
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.commands.search.aggregation import AggregateRequest, Desc, Asc
from redis.commands.search import reducers
import os
import json

FHIR_DIR = './synthea/output/fhir'
REDIS_URL = 'redis://localhost:12000'

class FHIR(object):
    def __init__(self, args):
        self.connection: Connection = from_url(args.url)
    
    def build_indices(self) -> None:
        print('*** Build indices ***')
        count = 0
        
        # location index
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Location:'])
        schema = [  TagField('$.status', as_name='status'),
            TextField('$.name', as_name='name'),
            TagField('$.address.city', as_name='city'),
            TagField('$.address.state', as_name='state'),
            NumericField('$.position.longitude', as_name='longitude'),
            NumericField('$.position.latitude', as_name='latitude')
        ]
        self.connection.ft('location_idx').create_index(schema, definition=idx_def)
        count +=1

        # practionerRole index
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['PractitionerRole:'])
        schema = [  TextField('$.practitioner.display', as_name='physician'),
            TextField('$.specialty[*].text', as_name='specialty', sortable=True),
            TextField('$.location[*].display', as_name='location')
        ]
        self.connection.ft('practitionerRole_idx').create_index(schema, definition=idx_def)
        count += 1

        # medicationRequest index
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['MedicationRequest:'])
        schema = [  TagField('$.status', as_name='status'),
            TextField('$.medicationCodeableConcept.text', as_name='drug'),
            TextField('$.requester.display', as_name='prescriber', sortable=True),
            TextField('$.reasonReference[*].display', as_name='reason')
        ]
        self.connection.ft('medicationRequest_idx').create_index(schema, definition=idx_def)
        count += 1

        # immunization index
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Immunization:'])
        schema = [  TextField('$.vaccineCode.text', as_name='vax', sortable=True),
            TextField('$.location.display', as_name='location'),
            TextField('$.occurrenceDateTime', as_name='date')
        ]
        self.connection.ft('immunization_idx').create_index(schema, definition=idx_def)
        count += 1

        # condition index
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Condition:'])
        schema = [  TagField('$.clinicalStatus.coding[*].code', as_name='code'),
            TextField('$.code.text', as_name='problem'),
            TagField('$.recordedDate', as_name='date')
        ]
        self.connection.ft('condition_idx').create_index(schema, definition=idx_def)
        count += 1

        # claims index
        idx_def = IndexDefinition(index_type=IndexType.JSON, prefix=['Claim:'])
        schema = [  TagField('$.status', as_name='status'),
            TextField('$.insurance[*].coverage.display', as_name='insurer', sortable=True),
            NumericField('$.total.value', as_name='value')
        ]
        self.connection.ft('claims_idx').create_index(schema, definition=idx_def)
        count += 1

        print(f'{count} indices built.')

    def load_docs(self) -> None:
        print('\n*** Load documents ***')
        count = 0
        for file in os.listdir(FHIR_DIR):
            with open(os.path.join(FHIR_DIR, file), 'r') as infile:
                bundle = json.load(infile)
                for entry in bundle['entry']:
                    key = f'{entry["resource"]["resourceType"]}:{entry["resource"]["id"]}'
                    self.connection.json().set(key, '$', entry['resource'])
                    count += 1
        print(f'{count} documents loaded.')
    
    def run_scenarios(self) -> None:
        print('\n*** Search Scenarios ***')
        
        print('\n**** Location Scenario 1 ****')
        ''' Business Problem - Find 3 medical facilities in Alaska
        '''
        query = Query('(@status:{active} @state:{AK})').return_fields('$.name', '$.address.city').paging(0,3)
        result = self.connection.ft('location_idx').search(query)
        print(result.docs) 

        print('\n**** Location Scenario 2 ****')
        ''' Business Problem - Find the closest medical facility (in the database) to Woodland Park CO
        '''
        request = AggregateRequest('@status:{active}')\
        .load('@name', '@city', '@state', '@longitude', '@latitude')\
        .apply(meters='geodistance(@longitude, @latitude, -105.0569, 38.9939)')\
        .apply(miles='ceil(@meters*0.000621371)')\
        .sort_by(Asc('@miles'))\
        .limit(0,1)
        result = self.connection.ft('location_idx').aggregate(request)
        print(result.rows)

        print('\n**** PractionerRole Scenario 1 ****')
        ''' Business Problem - Find 3 General Practice physicians that work from a hospital
        '''
        query = Query('(@specialty:"General Practice" @location:hospital)').return_fields('$.practitioner.display').paging(0,3)
        result = self.connection.ft('practitionerRole_idx').search(query)
        print(result.docs) 

        print('\n**** PractitionerRole Scenario 2 ****')
        ''' Business Problem - Find the count of physicians per medical specialty
        '''
        request = AggregateRequest('*').group_by('@specialty', reducers.count().alias('count'))     
        result = self.connection.ft('practitionerRole_idx').aggregate(request)
        print(result.rows)

        print('\n**** MedicationRequest Scenario 1 ****')
        ''' Business Problem - Find the names of 3 medications that have been currently prescribed for bronchitis
        '''
        query = Query('(@status:{active} @reason:%bronchitis%)').return_fields('$.medicationCodeableConcept.text').paging(0,3)
        result = self.connection.ft('medicationRequest_idx').search(query)
        print(result.docs)

        print('\n**** MedicationRequest Scenario 2 ****')
        ''' Business Problem - Find the top 3 physicians by prescription count who are prescribing opioids
        '''
        opioids = 'Hydrocodone|Oxycodone|Oxymorphone|Morphine|Codeine|Fentanyl|Hydromorphone|Tapentadol|Methadone'
        request = AggregateRequest(f'@drug:{opioids}')\
        .group_by('@prescriber', reducers.count().alias('opioids_prescribed'))\
        .sort_by(Desc('@opioids_prescribed'))\
        .limit(0,3)
        result = self.connection.ft('medicationRequest_idx').aggregate(request)
        print(result.rows)

        print('\n**** Immunization Scenario 1 ****')
        '''  Business Problem - Find 5 patients that received an immunizations at an urgent care clinic in 2015
        '''
        query = Query('@location:urgent @date:2015*').return_fields('$.patient.reference').paging(0,5)
        result = self.connection.ft('immunization_idx').search(query)
        print(result.docs)

        print('\n**** Immunization Scenario 2 ****')
        '''  Business Problem - Find the top 5 vaccines administered in 2020.
        '''
        request = AggregateRequest('@date:2020*')\
        .group_by('@vax', reducers.count().alias('num_vax'))\
        .sort_by(Desc('@num_vax'))\
        .limit(0,5)
        result = self.connection.ft('immunization_idx').aggregate(request)
        print(result.rows)

        print('\n**** Condition Scenario 1 ****')
        '''  Business Problem - Find 3 patients with active cases of rhinitis or asthma
        '''
        query = Query('@code:{active} @problem:(rhinitis|asthma)').return_fields('$.subject.reference').paging(0, 3)
        result = self.connection.ft('condition_idx').search(query)
        print(result.docs)

        print('\n**** Condition Scenario 2 ****')
        ''' Business Problem - Find the count of reported medical conditions categorized by year
        '''
        request = AggregateRequest('*')\
        .load('@date')\
        .apply(year='substr(@date,0,4)')\
        .group_by('@year', reducers.count().alias('num_conditions'))\
        .sort_by(Desc('@year'))\
        .limit(0,5)
        result = self.connection.ft('condition_idx').aggregate(request)
        print(result.rows)

        print('\n**** Claims Scenario 1 ****')
        ''' Business Problem - Find the service description of 3 active claims where Aetna is the insurer and the claim value is greater than $1000
        '''
        query = Query('@insurer:Aetna @value:[1000,+inf] @status:{active}').return_fields('$.item[0].productOrService.text').paging(0, 3)
        result = self.connection.ft('claims_idx').search(query)
        print(result.docs)

        print('\n**** Claims Scenario 2 ****')
        ''' Business Problem - Find the Top 3 insurers by claim value
        '''
        request = AggregateRequest('@status:{active}')\
        .group_by('@insurer', reducers.sum('@value').alias('total_value'))\
        .filter('@total_value > 0')\
        .sort_by(Desc('@total_value'))\
        .limit(0,3)
        result = self.connection.ft('claims_idx').aggregate(request)
        print(result.rows)



if __name__ == '__main__':
    parser = ArgumentParser(description='FHIR Search Examples')
    parser.add_argument('--url', required=False, type=str, default=REDIS_URL,
        help='Redis URL connect string')
    args = parser.parse_args()
    fhir = FHIR(args)
    fhir.build_indices()
    fhir.load_docs()
    fhir.run_scenarios()
    
