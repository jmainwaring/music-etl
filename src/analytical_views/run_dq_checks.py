import configparser
import psycopg2
import sys
sys.path.append('/home/workspace/src/')
from helpers.data_quality_checks import DataQualityChecks


# Specifying all of the data quality checks to run     
all_checks = [
    { 
        'table_name': 'pop_attr_comparison',
        'desired_checks': 
        {
            'custom_check': {'query': 'SELECT COUNT(*) FROM pop_attr_comparison;', 'expected_result': 2},
            'no_nulls': {'column_name': 'avg_duration'},
            'all_distinct': {'column_name': 'avg_duration'},
        }
    },

    { 
        'table_name': 'pop_attr_countries',
        'desired_checks': 
        {
            'has_rows': {},
            'no_nulls': {'column_name': 'id_country'},
            'all_distinct': {'column_name': 'id_country'},            
            'row_count_between': {'lower_bound': 50, 'upper_bound': 150},
        }
    },

    { 
        'table_name': 'first_chart_appearance',
        'desired_checks': 
        {
            'has_rows': {},
            'no_nulls': {'column_name': 'id_song'},
        }
    },

    { 
        'table_name': 'ten_leading_countries',
        'desired_checks': 
        {
            'has_rows': {},
            'no_nulls': {'column_name': 'song_leadership_count'},
            'all_distinct': {'column_name': 'leader_country_id'},            
            'custom_check': {'query': 'SELECT COUNT(*) FROM ten_leading_countries;', 'expected_result': 10},
        }
    }
]



def run_checks(desired_checks):
    """
    Carries out all of the user-specified data quality checks and raises an assertion error if any of the checks do not pass.

    Parameters
    ----------
        desired_checks (array) - an array of dictionaries that specifies the tests to run. Each dictionary contains two keys: the first one
          being the table_name on which testing will be done, and the second being a desired_checks dictionary that lists all of the tests 
          to run, along with any necessary parameters
    """

    config = configparser.ConfigParser()
    config.read('../redshift.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    for testing_dict in desired_checks:
        initial_args_dict = {'cur': cur, 'conn': conn, 'table_name': testing_dict['table_name']}

        for check in testing_dict['desired_checks'].keys():

            # Adds the additional arguments passed in to the args_dict, like column name
            args_dict = {**initial_args_dict, **testing_dict['desired_checks'][check]}

            # Creates reference to the correct data quality function
            function_name = DataQualityChecks.mapping_dict[check]
    
            # Runs the data quality function
            function_name(**args_dict)
            print('{} data quality check passed for table {}'.format(check, testing_dict['table_name']))

    conn.close()



def main():
    run_checks(desired_checks = all_checks)


if __name__ == "__main__":
    main()