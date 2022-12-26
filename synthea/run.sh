#!/bin/bash
## Author: Joey Whelan
## Usage: run.sh
## Description:  Builds synthetic FHIR patient bundles.  For each US state, a random number (1-10) of patient bundles are created.

if [ ! -f synthea-with-dependencies.jar ]
then
    wget -q https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
fi

STATES=("Alabama" "Alaska" "Arizona" "Arkansas" "California" "Colorado" "Connecticut" 
"Delaware" "District of Columbia" "Florida" "Georgia" "Hawaii" "Idaho" "Illinois"
"Indiana" "Iowa" "Kansas" "Kentucky" "Louisiana"  "Maine" "Montana" "Nebraska" 
"Nevada" "New Hampshire" "New Jersey" "New Mexico" "New York" "North Carolina"
"North Dakota" "Ohio" "Oklahoma" "Oregon" "Maryland" "Massachusetts" "Michigan" 
"Minnesota" "Mississippi" "Missouri" "Pennsylvania" "Rhode Island" "South Carolina"
"South Dakota" "Tennessee" "Texas" "Utah" "Vermont" "Virginia" "Washington" 
"West Virginia" "Wisconsin" "Wyoming")

MAX_POP=10

for state in "${STATES[@]}"; do   
  pop=$(($RANDOM%$MAX_POP + 1))
  java -jar synthea-with-dependencies.jar -c ./syntheaconfig.txt -p $pop "$state"
done