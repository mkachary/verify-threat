## Anonymisation of the production SSO-event data for threat analysis

- Code for anonymising .ndjson files and produce an anonymises .ndjson file.
- Can be used for very large datasets.


## Installation of dependencies

To install the required libraries follow the instructions
- Go to your anaconda prompt and navigate to this project folder.
- Type `conda env create --file environment.yml` . This will create a python environment for running the code along with all the depencies.
- To activate the new environment type `conda activate enc`. By default the name of the new environment will be `enc`.
    - To change the new environment name, open the environment.yml file in any editor and replace the `name` field value with your desired name. 
- Yey, you are ready to run the code.



## Running the scripts

Before running the test.py script
- Go to configuration folder and open config.json file.
- Give the `input_path` and `output_path` of the ndjson file.
- Set the encryption key to be used for anonymisation.
- [optional] Change the `Max_instances_to_process_at_once` which will be used for encryption of sensitive fields.

Then simply run the `test.py` file. The output ndjson file will be created/populated at the output_path location. 