import psycopg2
import thread
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'


##########################################################################################################
finallist=[]
import time
class myThreads (threading.Thread):
   def __init__(self, i, SortingColumnName, openconnection, InputTable, m, p):
      threading.Thread.__init__(self)
      self.i = i
      self.SortingColumnName = SortingColumnName
      self.openconnection = openconnection
      self.InputTable =InputTable
      self.m = m
      self.p = p
   def run(self):
      #print "Starting "
      ThreadParallelSort(self.i, self.SortingColumnName, self.openconnection, self.InputTable, self.m, self.p)

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    try:
        cursor = openconnection.cursor()
        # Finding the minimum & maximum values
        cursor.execute("SELECT MIN(" + str(SortingColumnName) + ") FROM " + str(InputTable))
        MinValue = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(" + str(SortingColumnName) + ") FROM " + str(InputTable))
        MaxValue = cursor.fetchone()[0]
        interval = abs(MaxValue - MinValue) / float(5)


        # Create the Output Table similar to InputTable Schema
        cursor.execute("DROP TABLE IF EXISTS " + str(OutputTable))
        cursor.execute("CREATE TABLE " + str(OutputTable) + " AS SELECT * FROM " + InputTable + " WHERE 1=2")


        # creating the partition tables and sorting them using threads in a different function
        totallist = []
        thread = range(5)
        lowerbound = MinValue
        for i in range(5):
            upperbound = lowerbound + interval

            table_name = InputTable + "_range_part" + str(i)
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
            cursor.execute("CREATE TABLE " + table_name + " AS SELECT * FROM " + InputTable + " WHERE 1=2")
            thread[i] = threading.Thread(target=Insert_Sort, args=(
                i, InputTable, SortingColumnName, lowerbound, upperbound, openconnection))
            thread[i].start()
            lowerbound = upperbound

        # Ensure all of the threads have finished
        for i in thread:
            i.join()

        # extend all tables into a list
        for i in range(5):
            cursor.execute(" SELECT * FROM " + InputTable + "_range_part" + str(i))
            totallist.extend(cursor.fetchall())

        # Insert the values into the output table
        for i in totallist:
            cursor.execute("INSERT INTO " + OutputTable + " VALUES " + str(i) + ";")


    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        # Clean up Code
        for i in range(5):
            cursor.execute("DROP TABLE IF EXISTS " + InputTable + "_range_part" + str(i))
        openconnection.commit()

        if cursor:
            cursor.close()


def Insert_Sort(i, InputTable, SortingColumnName, lowerbound, upperbound, openconnection):
    cursor = openconnection.cursor()
    table_name = InputTable + "_range_part" + str(i)

    if i == 0:
        cursor.execute(
            "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(
                lowerbound) + " AND " + SortingColumnName + " <= " + str(
                upperbound) + " ORDER BY " + SortingColumnName + " ASC")
    else:
        cursor.execute(
            "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(
                lowerbound) + " AND " + SortingColumnName + " <= " + str(
                upperbound) + " ORDER BY " + SortingColumnName + " ASC")

    openconnection.commit()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        cursor = openconnection.cursor()

        # Finding the minimum & maximum values
        cursor.execute("SELECT MIN(" + str(Table1JoinColumn) + ") FROM " + str(InputTable1))
        MinValue_1 = cursor.fetchone()[0]
        cursor.execute("SELECT MIN(" + str(Table2JoinColumn) + ") FROM " + str(InputTable2))
        MinValue_2 = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(" + str(Table1JoinColumn) + ") FROM " + str(InputTable1))
        MaxValue_1 = cursor.fetchone()[0]
        cursor.execute("SELECT MAX(" + str(Table2JoinColumn) + ") FROM " + str(InputTable2))
        MaxValue_2 = cursor.fetchone()[0]
        MinValue = min(MinValue_1, MinValue_2)
        MaxValue = max(MaxValue_1, MaxValue_2)
        interval = abs(MaxValue - MinValue) / float(5)

        # Create the Output Table similar to InputTable Schema
        cursor.execute("DROP TABLE IF EXISTS " + OutputTable)
        cursor.execute("CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable1 + "," + InputTable2 + " WHERE 1=2")

        # creating 5 outputtables
        for i in range(5):
            outtable_name = OutputTable + str(i)
            cursor.execute("DROP TABLE IF EXISTS " + outtable_name)
            cursor.execute("CREATE TABLE " + outtable_name + " AS SELECT * FROM " + InputTable1 + "," + InputTable2 + " WHERE 1=2")


        # Assign and start threads
        thread = range(5)
        lowerbound = MinValue

        for i in range(5):
            upperbound = lowerbound + interval
            outtable_name = OutputTable + str(i)
            thread[i] = threading.Thread(target=PartitionJoin, args=(
            outtable_name, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, lowerbound, upperbound,
            openconnection))
            thread[i].start()
            lowerbound = upperbound

        # Ensure all of the threads have finished
        for i in thread:
            i.join()

        # Insert the values into the output table
        for i in range(5):
            outtable_name = OutputTable + str(i)
            cursor.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + outtable_name)


    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        # cleanup
        for i in range(5):
            outtable_name = OutputTable + str(i)
            cursor.execute("DROP TABLE IF EXISTS "+outtable_name+"")
        openconnection.commit()
        if cursor:
            cursor.close()


def PartitionJoin(outtable, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, lowerbound, upperbound,openconnection):
    cursor = openconnection.cursor()

    cursor.execute("INSERT INTO " + outtable +
                   " select * from " + str(InputTable1) + " inner join " + str(InputTable2) + " on " + str(
        InputTable1) + "." + str(
        Table1JoinColumn) + "=" + str(InputTable2) + "." + str(Table2JoinColumn) + " where " + str(
        InputTable2) + "." + str(Table2JoinColumn) + " >= " + str(lowerbound) + " and " + str(
        InputTable2) + "." + str(Table2JoinColumn) + " <= " + str(upperbound))


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':
    try:
        # Creating Database ddsassignment3
        print "Creating Database named as ddsassignment3"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE,
                     'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail