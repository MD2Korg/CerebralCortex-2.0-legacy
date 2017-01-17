import mysql.connector


class ReadDataFromMySql:
    def __init__(self, databaseName="cortex", dbUserName="root", dbPassword="pass"):
        self.dbConnection = mysql.connector.connect(user=dbUserName,password=dbPassword, database=databaseName)
        self.cursor = self.dbConnection.cursor()

    '''
    :param datastreamType (optional) type of a datastream, for example, ECG
    :return a list of all available datastream IDs
    '''
    def getAllDatastreamIDs(self, datastreamType=""):
        qry = "SELECT id FROM datastream"
        if not datastreamType=="":
            qry = qry+" where metadata->'$.'"

        return self.executeQuery(qry)

    '''
    :param datastreamType (optional) type of a datastream, for example, ECG
    :return a list of all available datastream IDs of a user
    '''
    def getAllDatastreamIDsByUserID(self, participantID, datastreamType=""):
        qry = "SELECT id FROM datastream"
        if not datastreamType=="":
            qry = qry+" where metadata->'$.'"

        return self.executeQuery(qry)

    '''
    :param datastreamID
    :return metadata of a datastream
    '''
    def getDatastreamMetadata(self, datastreamID):
        qry = "SELECT metadata FROM datastream where id="+datastreamID
        return self.executeQuery(qry)

    '''
    :param studyID (optional)
    :return a list of all available configuration IDs
    '''
    def getAllConfigurationIDs(self, studyID=""):
        qry = "SELECT id FROM datastream"
        if not studyID=="":
            qry = qry+" where metadata->'$.'"

        return self.executeQuery(qry)

    '''
    :param configurationID
    :return metadata of a configuration
    '''
    def getConfigurationMetadata(self, configurationID):
        qry = ("SELECT id FROM country_list group by id")
        return self.executeQuery(qry)

    '''
    :param processingModuleType (optional) type of a processing module, for example, stress
    :return a list of all available processing module IDs
    '''
    def getAllProcessingModuleIDs(self, processingModuleType=""):
        qry = "SELECT id FROM configuration"
        if not processingModuleType=="":
            qry = qry+" where metadata->'$.'"

        return self.executeQuery(qry)

    '''
    :param processingModuleID processing module ID
    :return metadata of a processing module
    '''
    def getProcessingModuleMetadata(self, processingModuleID):
        qry = "SELECT id FROM configuration where id"+processingModuleID
        return self.executeQuery(qry)

    '''
    :param userID
    :param datastreamID (optional)
    :param startTime (optional)
    :param endTime (optional)
    :return SQL query
    '''
    def getUserDatastreams(userID, datastreamID="", startTime="", endTime=""):
        qry = "Select * from data_points where participant_id="+participantID
        if not datastreamID=="":
            qry = qry+" and datastream_id="+datastreamID
        if not startTime=="":
            qry = qry+" and timestamp<"+startTime
        if not endTime=="":
            qry = qry+" and timestamp<="+endTime

        return qry

    def getUserMetadata(self, userID):
        qry = ("SELECT id FROM country_list group by id")
        return self.executeQuery(qry)

    '''
    :param SQL query
    :return
    '''
    def executeQuery(self,qry):
        self.cursor.execute(qry)
        results = self.cursor.fetchall()
        self.cursor.close()
        self.dbConnection.close()
        return results

#TO-DO

    def getAllStudyIDs(self, datastreamType=""):
        pass
