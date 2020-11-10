import PrateekFunctions as pf

def main():
    '''Main function makes a secure connection and then implements the logic.
    Check if the file exists in a path. If no, download the file. If yes, compare the new data with the old data.
    Is the new data and the old data same? If yes, do nothing. If yes, download the new data.'''

    # Installing all the required libraries
    pf.fn_InstallLibraries(['pandas', 'pyodbc', 'cryptography', 'getpass4'])
    import pyodbc
    import pandas as pd

    # Get all the necessary parameters in order to make a db connection
    connect = pf.fn_ConnectionDetails()
    # Make a connection to database   LAPTOP-2M1VBV0L
    # Get the data from the Survey Structure table for comparison
    df_survey_struc = pf.fn_GetSurveyStruc(connect)
    # Check if the data has been downloaded previously.
    # If not, generate a pickle file for future comparison and then get the data from the database and save it in a csv file.
    if pf.fn_CheckFileInPath('SurveySampleS20.pkl') == False:
        df_survey_struc.to_pickle('SurveySampleS20.pkl')
        final_query = pf.fn_GetSurveyData(connect)
        get_final_query_data = pd.read_sql(final_query, connect)
        csv_output = get_final_query_data.to_csv('SurveyOutput.csv', index=False, header=True)
        connect.close
        print('The file SurveyOutput.csv is generated and is saved here : ' + pf.os.getcwd())
    else:
        # If the data has been downloaded previously in a file and the new data has not changed, provide the path of the file.
        if pf.fn_CompareSurveyStrucFiles(df_survey_struc, 'SurveySampleS20.pkl') == True:
            csv_output = None
            connect.close
            print('File already exists in the path :' + pf.os.getcwd())
        else:
            # If the data has been downloaded previousy but the new data has changed,
            # generate a pickle file for future comparison and get the data from the database and save it in a csv file.
            df_survey_struc.to_pickle('SurveySampleS20.pkl')
            final_query = pf.fn_GetSurveyData(connect)
            get_final_query_data = pd.read_sql(final_query, connect)
            # Deleting the old csv file to save a new one.
            pf.os.remove('SurveyOutput.csv')
            csv_output = get_final_query_data.to_csv('SurveyOutput.csv', index=False, header=True)
            connect.close
            print('The updated file SurveyOutput.csv is saved here : ' + pf.os.getcwd())

    return None


if __name__ == '__main__':
    main()