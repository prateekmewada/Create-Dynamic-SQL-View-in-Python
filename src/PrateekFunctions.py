'''Importing all the python standard libraries here.'''
import subprocess
import sys
import os
import csv


def fn_InstallLibraries(libraries):
    '''This function checks whether any external python libraries are installed.
    If the library is not installed already, it will install it via pip install'''
    for library_name in libraries:
        verify_lib = library_name in sys.modules
        if verify_lib == False:
            subprocess.check_call([sys.executable, "-m", "pip", "install", library_name])


def fn_ConnectionDetails():
    '''This function contains all the connection parameters.'''
    import pyodbc
    global dbConnection

    username = input('Enter Username: ')
    password = fn_Password()
    server = input('Enter Server Name: ')
    database = 'Survey_Sample_A19'
    try:
    # connection_details = {'server' : server, 'database': database, 'username': username, 'password': password}
        dbConnection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=" + server + "; DATABASE=" \
                  + database + "; UID=" + username + "; PWD=" + password)
        print("Connection established!")
        return dbConnection
    except pyodbc.Error as e:
        error_message = e.args[0]
        sys.exit(f"Connection could not be established. Please make sure you have entered correct login details. \n"
                  f"Exiting with code: {error_message}")



def fn_Key():
    '''This function creates a file in the local system to save the key required for encrypting and decrypting the password.'''
    from cryptography.fernet import Fernet
    # Creates a text file if not already created.
    if fn_CheckFileInPath("Keys.txt") == False:
        txtfile = open("Keys.txt", "w+")

    with open("Keys.txt","r") as txtfile:
        reader=csv.reader(txtfile)
        KeyFound=0
        #for loop reads the key from the text file.
        for row in reader:
            try:
                row[0]
            except IndexError:
                continue
            if len(row[0])>4:
                KeyFound=1
                Key=row[0]
            else:
                pass
    # if the key is not found, generates the key.
    if KeyFound==0:
        Key= Fernet.generate_key()
        txtfile.close()
        # Saves the generated key in the text file.
        with open("Keys.txt","w") as txtfile:
            headers =['key']
            writer = csv.DictWriter(txtfile,fieldnames=headers)
            writer.writeheader()
            writer.writerow({'key': Key.decode('utf-8')})
            txtfile.close()
    Ecy = Fernet(Key)
    txtfile.close()
    return Ecy


def fn_Password():
    '''This function asks user for the password and decrypts is using the key generated in the function fn_Key()'''
    from cryptography.fernet import Fernet
    import getpass4
    #Prompts user for password.
    myPassword =  getpass4.getpass(prompt="Password:", char="*")
    b = bytes(myPassword, 'utf-8')
    # Password encryption using the key
    encryptedPassword = fn_Key().encrypt(b)
    # Password decryption
    decryptedPassword=str(fn_Key().decrypt(encryptedPassword))
    decryptedPassword = decryptedPassword.strip("b'")
    return decryptedPassword


def fn_GetSurveyStruc(connection):
    '''This function checks the valid connection and returns the data from the table SurveyStructure.'''
    import pandas as pd
    survey_structure_query = """SELECT SurveyId, QuestionId
                                    FROM SurveyStructure
                                    ORDER BY SurveyId, QuestionId"""
    survey_structure_res = pd.read_sql(survey_structure_query, connection)
    return survey_structure_res


def fn_CheckFileInPath(file_name):
    '''This function checks if a file exists in the given path.'''
    if os.path.isfile(file_name):
        file_in_path = True
    else:
        file_in_path = False
    return file_in_path


def fn_CompareSurveyStrucFiles(survey_structure_res, old_file):
    '''This function is for comparing the new data from the query with the old data that is stored in the local system.'''
    import pandas as pd
    # Takes the old file, converts it into readable format and compares it to the new data.
    if(survey_structure_res.equals(pd.read_pickle(old_file)) == True):
        file_verified = True
    else:
        file_verified = False
    return file_verified


def fn_GetSurveyData(connection):
    '''This function replicates the functionality of the stored procedure almost exactly and returns the final query.'''
    import pandas as pd
    #Initialise main query to be built
    main_query = ''
    #Initialisation for union
    current_union_query_blk = ''
    #This variable with the survey ids will be used in the for loop for iterating survey ids.
    survey_query = """SELECT SurveyId FROM Survey ORDER BY SurveyId"""
    # Build the base query for the Answer Column just like in the stored procedure.
    ans_col_query_tmp = """COALESCE(( SELECT a.Answer_Value	FROM Answer as a
					WHERE
						a.UserId = u.UserId	AND a.SurveyId = <SURVEY_ID> AND a.QuestionId = <QUESTION_ID>
				), -1) AS ANS_Q<QUESTION_ID> """
    # Build the base query for the columns where NULL needs to be inserted.
    null_col_query_tmp = """ NULL AS ANS_Q<QUESTION_ID> """
    # Build the base query which will be used for union.
    outer_union_query_tmp = """SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> FROM [User] as u
			WHERE EXISTS
			(SELECT * FROM Answer as a
					WHERE 
					u.UserId = a.UserId	AND a.SurveyId = <SURVEY_ID> )"""
    # Get all the survey ids from survey_query by verifying the connection.
    df_survey_query = pd.read_sql(survey_query, connection)

    #Initialise the variable
    curr_survey_id = None
    # Get the survey id and for each survey id loop the below query of the questions
    for rownum_1, survey_curr_row in df_survey_query.iterrows():
        curr_survey_id = survey_curr_row['SurveyId']

        question_query_tmp = """SELECT * FROM
                    ( SELECT SurveyId, QuestionId, 1 as InSurvey
                        FROM SurveyStructure
                        WHERE SurveyId = """ +str(curr_survey_id) + """
                        UNION
                        SELECT	""" +str(curr_survey_id) + """ as SurveyId,	Q.QuestionId, 0 as InSurvey
                        FROM Question as Q
                        WHERE NOT EXISTS
                        ( SELECT *
                            FROM SurveyStructure as S
                            WHERE S.SurveyId = """ +str(curr_survey_id) + """ AND S.QuestionId = Q.QuestionId
                        )) as t ORDER BY QuestionId """
        # Get all the data of the query that was built above after verifying the connection
        df_question_query = pd.read_sql(question_query_tmp, connection)
        #Initialisations
        column_query = ''
        curr_ques_id = None
        curr_in_survey = None
        curr_ques_survey_id = None
        # Get the survey id, question id and in survey from the above question query and loop it to build the query.
        for rownum_2, question_curr_row in df_question_query.iterrows():
            curr_ques_survey_id = question_curr_row['SurveyId']
            curr_ques_id = question_curr_row['QuestionId']
            curr_in_survey = question_curr_row['InSurvey']
            # Put the Null column template by replacing the question id in it.
            if curr_in_survey == 0:
                column_query = column_query + null_col_query_tmp.replace('<QUESTION_ID>', str(curr_ques_id))
            # Put the answer column template by replacing the question id in it.
            else:
                column_query = column_query + ans_col_query_tmp.replace('<QUESTION_ID>', str(curr_ques_id))
            # If the column is not the last column, put comma in between
            if  (len(df_question_query.index) -1) > (rownum_2):
                column_query = column_query + ' , '

        # Replacing the column query in the union template
        current_union_query_blk = outer_union_query_tmp.replace('<DYNAMIC_QUESTION_ANSWERS>', column_query)
        # Replacing the survey id in the union template
        current_union_query_blk = current_union_query_blk.replace('<SURVEY_ID>', str(curr_survey_id))
        # Building the final query
        main_query = main_query + current_union_query_blk
        # If the query is not the last query, add UNION in between just like putting a comma in the above ef condition.
        if (len(df_survey_query.index) - 1) > (rownum_1):
            main_query = main_query + ' UNION '

    return main_query
